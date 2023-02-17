package org.oliverlittle.clusterprocess.model.table

import org.oliverlittle.clusterprocess.model.table.sources.{DataSource, PartialDataSource, DependentDataSource}

import akka.actor.Status
import akka.Done
import akka.actor.typed.scaladsl.{Behaviors, LoggerOps}
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}

import scala.collection.immutable.HashMap
import scala.collection.MapView

object TableStore {
    sealed trait TableStoreEvent
    final case class AddResult(table : PartialTable, result : TableResult, replyTo : ActorRef[Status[Done]]) extends TableStoreEvent
    final case class DeleteResult(table : Table) extends TableStoreEvent
    final case class GetResult(table : PartialTable, replyTo : ActorRef[Option[TableResult]]) extends TableStoreEvent
    final case class AddPartition(partition : PartialDataSource, result : TableResult, replyTo : ActorRef[Status[Done]]) extends TableStoreEvent
    final case class DeletePartition(dataSource : DataSource) extends TableStoreEvent
    final case class GetPartition(partition : PartialDataSource, replyTo : ActorRef[Option[TableResult]]) extends TableStoreEvent
    final case class HashPartition(dataSource : DependentDataSource, numPartitions : Int, replyTo : ActorRef[Status[Done]]) extends TableStoreEvent
    final case class GetHash(table : Table, totalPartitions : Int, partitionNum : Int, replyTo : ActorRef[Option[TableResult]]) extends TableStoreEvent
    final case class DeleteHash(dataSource : DataSource, numPartitions : Int) extends TableStoreEvent

    def apply() : Behavior[TableStoreEvent] = processResults(HashMap(), HashMap(), HashMap())
        
    def processResults(
        tables : Map[Table, Map[PartialTable, TableResult]], 
        partitions : Map[DataSource, Map[PartialDataSource, TableResult]], 
        hashes : Map[(Table, Int), MapView[Int, TableResult]]
    ) : Behavior[TableStoreEvent] = Behaviors.receive{ (context, message) =>
        message match {
            case AddResult(table, result) =>
                // Update the mapping in place
                val newMap = tables + (table.parent -> tables.getOrElse(table.parent, HashMap()) + (table -> result))
                replyTo ! Status.ack()
                processResults(newMap, partitions, hashes)
            case DeleteResult(table) => 
                processResults(tables - table, partitions, hashes)
            case GetResult(table, replyTo) =>
                replyTo ! tables.get(table)
                Behaviors.same
            case AddPartition(partition, result) =>
                val newMap = partitions + (partition.parent -> partitions.getOrElse(partition.parent, HashMap()) + (partition -> result))
                replyTo ! Status.ack()
                processResults(tables, newMap, hashes)
            case DeletePartition(partition) =>
                processResults(tables, partitions - partition, hashes)
            case GetPartition(partition, replyTo) => 
                replyTo ! partitions.get(partition)
                Behaviors.same
            case HashPartition(dataSource, numPartitions) =>
                val dependencies = dataSource.dependencies
                dependencies.forall(dependency => tables contains dependency)
                val newMap = hashes ++ dataSource.dependencies.map(dependency => (dependency, numPartitions) -> dataSource.getPartitionedData(tables(dependency).values.reduce(_ ++ _), numPartitions))
                replyTo ! Status.ack()
                processResults(tables, partitions, newMap)
            case GetHash(table, totalPartitions, partitionNum) =>
                // Attempt to get the partition
                replyTo ! hashes.get((table, totalPartitions)).flatMap(_.get(partitionNum))
                Behaviors.same
            case DeleteHash(dataSource, numPartitions) =>
                val hashKeys = dataSource.dependencies.map(table => (table, numPartitions))
                val newMap = hashes -- hashKeys
                processResults(tables, partitions, newMap)
        }
    }
}