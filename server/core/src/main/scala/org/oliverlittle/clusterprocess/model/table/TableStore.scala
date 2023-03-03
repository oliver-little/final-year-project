package org.oliverlittle.clusterprocess.model.table

import org.oliverlittle.clusterprocess.worker_query
import org.oliverlittle.clusterprocess.model.table.sources.{DataSource, PartialDataSource, DependentDataSource}

import akka.pattern.StatusReply
import akka.Done
import akka.actor.typed.scaladsl.{Behaviors, LoggerOps}
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}

import scala.collection.immutable.{HashMap}
import scala.collection.{LinearSeq, MapView}

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
    // Push/Pop cache stack
    final case class PushCache() extends TableStoreEvent
    final case class PopCache(replyTo : ActorRef[TableStoreData]) extends TableStoreEvent
    final case class GetData(replyTo : ActorRef[TableStoreData]) extends TableStoreEvent
    // Reset state
    final case class Reset() extends TableStoreEvent

    def apply() : Behavior[TableStoreEvent] = empty
        
    def processResults(tableStoreData : TableStoreData, cache : Seq[TableStoreData]) : Behavior[TableStoreEvent] = Behaviors.receive{ (context, message) =>
        message match {
            case AddResult(table, result, replyTo) =>
                // Update the mapping in place
                val newMap = tableStoreData.tables + (table.parent -> (tableStoreData.tables.getOrElse(table.parent, HashMap()) + (table -> result)))
                replyTo ! StatusReply.ack()
                processResults(tableStoreData.copy(tables=newMap), cache)

            case DeleteResult(table) => 
                processResults(tableStoreData.copy(tableStoreData.tables - table), cache)

            case GetResult(table, replyTo) =>
                replyTo ! tableStoreData.tables.get(table.parent).flatMap(_.get(table))
                Behaviors.same

            case GetAllResults(table, replyTo) =>
                replyTo ! tableStoreData.tables.get(table).map(_.values).getOrElse(Seq()).toSeq
                Behaviors.same

            case AddPartition(partition, result, replyTo) =>
                val newMap = tableStoreData.partitions + (partition.parent -> (tableStoreData.partitions.getOrElse(partition.parent, HashMap()) + (partition -> result)))
                replyTo ! StatusReply.ack()
                processResults(tableStoreData.copy(partitions=newMap), cache)

            case DeletePartition(partition) =>
                processResults(tableStoreData.copy(partitions=tableStoreData.partitions - partition), cache)

            case GetPartition(partition, replyTo) => 
                replyTo ! tableStoreData.partitions.get(partition.parent).flatMap(_.get(partition))
                Behaviors.same

            case HashPartition(dataSource, numPartitions, replyTo) =>
                // It's possible that we don't have a hashed object for every dependency
                // Therefore, we filter the dependency for ones we actually have data for
                val dependencies = dataSource.getDependencies.filter(dependency => tableStoreData.tables contains dependency)
                val newMap = tableStoreData.hashes ++ dependencies.map(dependency => (dependency, numPartitions) -> dataSource.hashPartitionedData(tableStoreData.tables(dependency).values.reduce(_ ++ _), numPartitions))
                replyTo ! StatusReply.ack()
                processResults(tableStoreData.copy(hashes=newMap), cache)

            case GetHash(table, totalPartitions, partitionNum, replyTo) =>
                // Attempt to get the partition
                replyTo ! tableStoreData.hashes.get((table, totalPartitions)).flatMap(_.get(partitionNum))
                Behaviors.same

            case DeleteHash(dataSource, numPartitions) =>
                val hashKeys = dataSource.getDependencies.map(table => (table, numPartitions))
                val newMap = tableStoreData.hashes -- hashKeys
                processResults(tableStoreData.copy(hashes=newMap), cache)

            // Cache operations
            case PushCache() =>
                processResults(tableStoreData, tableStoreData +: cache)

            case PopCache(replyTo) =>
                val head = cache.headOption.getOrElse(TableStoreData.empty)
                replyTo ! head
                processResults(head, cache.drop(1))

            case GetData(replyTo) =>
                replyTo ! tableStoreData
                Behaviors.same

            case Reset() => empty
        }
    }

    def empty : Behavior[TableStoreEvent] = processResults(TableStoreData.empty, LinearSeq().toSeq)
}

object TableStoreData:
    def empty = TableStoreData(HashMap(), HashMap(), HashMap())

case class TableStoreData(tables : Map[Table, Map[PartialTable, TableResult]], partitions : Map[DataSource, Map[PartialDataSource, TableResult]], hashes : Map[(Table, Int), MapView[Int, TableResult]]):
    lazy val protobuf = worker_query.TableStoreData(
        tables.values.map(partialTables =>
            worker_query.TableStoreData.PartialTableList(partialTables.keys.map(_.protobuf).toSeq)
        ).toSeq,
        partitions.values.map(partialDataSources =>
            worker_query.TableStoreData.PartialDataSourceList(
                partialDataSources.keys.map(_.protobuf).toSeq
            )
        ).toSeq
    )