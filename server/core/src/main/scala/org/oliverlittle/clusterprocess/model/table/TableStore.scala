package org.oliverlittle.clusterprocess.model.table

import org.oliverlittle.clusterprocess.model.table.sources.{DataSource, PartialDataSource, DependentDataSource}

import akka.pattern.StatusReply
import akka.Done
import akka.actor.typed.scaladsl.{Behaviors, LoggerOps}
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}

import scala.collection.immutable.HashMap
import scala.collection.MapView

object TableStore {
    sealed trait TableStoreEvent
    // Requests for TableResults
    final case class AddResult(table : PartialTable, result : TableResult, replyTo : ActorRef[StatusReply[Done]]) extends TableStoreEvent
    final case class DeleteResult(table : Table) extends TableStoreEvent
    final case class GetResult(table : PartialTable, replyTo : ActorRef[Option[TableResult]]) extends TableStoreEvent
    final case class GetAllResults(table : Table, replyTo : ActorRef[Seq[TableResult]]) extends TableStoreEvent
    // Requests for computed partitions (PartialDataSource)
    final case class AddPartition(partition : PartialDataSource, result : TableResult, replyTo : ActorRef[StatusReply[Done]]) extends TableStoreEvent
    final case class DeletePartition(dataSource : DataSource) extends TableStoreEvent
    final case class GetPartition(partition : PartialDataSource, replyTo : ActorRef[Option[TableResult]]) extends TableStoreEvent
    // Requests for *preparing* computed partitions
    final case class HashPartition(dataSource : DependentDataSource, numPartitions : Int, replyTo : ActorRef[StatusReply[Done]]) extends TableStoreEvent
    final case class GetHash(table : Table, totalPartitions : Int, partitionNum : Int, replyTo : ActorRef[Option[TableResult]]) extends TableStoreEvent
    final case class DeleteHash(dataSource : DataSource, numPartitions : Int) extends TableStoreEvent

    def apply() : Behavior[TableStoreEvent] = processResults(HashMap(), HashMap(), HashMap())
        
    def processResults(
        tables : Map[Table, Map[PartialTable, TableResult]], 
        partitions : Map[DataSource, Map[PartialDataSource, TableResult]], 
        hashes : Map[(Table, Int), MapView[Int, TableResult]]
    ) : Behavior[TableStoreEvent] = Behaviors.receive{ (context, message) =>
        message match {
            case AddResult(table, result, replyTo) =>
                // Update the mapping in place
                val newMap = tables + (table.parent -> (tables.getOrElse(table.parent, HashMap()) + (table -> result)))
                replyTo ! StatusReply.ack()
                processResults(newMap, partitions, hashes)

            case DeleteResult(table) => 
                processResults(tables - table, partitions, hashes)

            case GetResult(table, replyTo) =>
                replyTo ! tables.get(table.parent).flatMap(_.get(table))
                Behaviors.same

            case GetAllResults(table, replyTo) =>
                replyTo ! tables.get(table).map(_.values).getOrElse(Seq()).toSeq
                Behaviors.same

            case AddPartition(partition, result, replyTo) =>
                val newMap = partitions + (partition.parent -> (partitions.getOrElse(partition.parent, HashMap()) + (partition -> result)))
                replyTo ! StatusReply.ack()
                processResults(tables, newMap, hashes)

            case DeletePartition(partition) =>
                processResults(tables, partitions - partition, hashes)

            case GetPartition(partition, replyTo) => 
                replyTo ! partitions.get(partition.parent).flatMap(_.get(partition))
                Behaviors.same

            case HashPartition(dataSource, numPartitions, replyTo) =>
                val dependencies = dataSource.getDependencies
                dependencies.forall(dependency => tables contains dependency)
                val newMap = hashes ++ dependencies.map(dependency => (dependency, numPartitions) -> dataSource.hashPartitionedData(tables(dependency).values.reduce(_ ++ _), numPartitions))
                replyTo ! StatusReply.ack()
                processResults(tables, partitions, newMap)

            case GetHash(table, totalPartitions, partitionNum, replyTo) =>
                // Attempt to get the partition
                replyTo ! hashes.get((table, totalPartitions)).flatMap(_.get(partitionNum))
                Behaviors.same

            case DeleteHash(dataSource, numPartitions) =>
                val hashKeys = dataSource.getDependencies.map(table => (table, numPartitions))
                val newMap = hashes -- hashKeys
                processResults(tables, partitions, newMap)
        }
    }
}