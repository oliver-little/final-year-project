package org.oliverlittle.clusterprocess.model.table

import org.oliverlittle.clusterprocess.worker_query
import org.oliverlittle.clusterprocess.model.table.sources.{DataSource, PartialDataSource, DependentDataSource}
import org.oliverlittle.clusterprocess.util.{MemoryUsage, LRUCache}

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
                val newData = tableStoreData.updateResult(table, result)
                replyTo ! StatusReply.ack()
                processResults(newData, cache)

            case DeleteResult(table) => 
                processResults(tableStoreData.deleteResult(table), cache)

            case GetResult(table, replyTo) =>
                replyTo ! tableStoreData.tables.get(table.parent).flatMap(_.get(table).map(_.get))
                // Update LRU ordering
                Behaviors.same

            case GetAllResults(table, replyTo) =>
                replyTo ! tableStoreData.tables.get(table).map(_.values).getOrElse(Seq()).map(_.get).toSeq
                // Update LRU ordering
                Behaviors.same

            case AddPartition(partition, result, replyTo) =>
                val newData = tableStoreData.updatePartition(partition, result)
                replyTo ! StatusReply.ack()
                processResults(newData, cache)

            case DeletePartition(partition) =>
                processResults(tableStoreData.deletePartition(partition), cache)

            case GetPartition(partition, replyTo) => 
                replyTo ! tableStoreData.partitions.get(partition.parent).flatMap(_.get(partition).map(_.get))
                Behaviors.same

            case HashPartition(dataSource, numPartitions, replyTo) =>
                val newData = tableStoreData.updateHashes(dataSource, numPartitions)
                replyTo ! StatusReply.ack()
                processResults(newData, cache)

            case GetHash(table, totalPartitions, partitionNum, replyTo) =>
                // Attempt to get the partition
                replyTo ! tableStoreData.hashes.get((table, totalPartitions)).flatMap(_.get(partitionNum).map(_.get))
                Behaviors.same

            case DeleteHash(dataSource, numPartitions) =>
                processResults(tableStoreData.deleteHashes(dataSource, numPartitions), cache)

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

            case Reset() =>    
                tableStoreData.cleanup
                empty
        }
    }

    def empty : Behavior[TableStoreEvent] = processResults(TableStoreData.empty, LinearSeq().toSeq)
}

object TableStoreData:
    def empty = TableStoreData(HashMap(), HashMap(), HashMap(), LRUCache())

case class TableStoreData(
    tables : Map[Table, Map[PartialTable, StoredTableResult[PartialTable]]], 
    partitions : Map[DataSource, Map[PartialDataSource, StoredTableResult[PartialDataSource]]], 
    hashes : Map[(Table, Int), Map[Int, StoredTableResult[((Table, Int), Int)]]],
    leastRecentlyUsed : LRUCache[StoredTableResult[_]]):
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

    def updateResult(table : PartialTable, result : TableResult) : TableStoreData = {
        val memoryUsagePercentage = MemoryUsage.getMemoryUsagePercentage(Runtime.getRuntime)
        val storedResult = StoredTableResult.getStoredTable(table, result, memoryUsagePercentage)
        val newMap : Map[Table, Map[PartialTable, StoredTableResult[PartialTable]]] = tables + (table.parent -> (tables.getOrElse(table.parent, HashMap()) + (table -> storedResult)))
        return copy(tables=newMap)
    }

    def updatePartition(partition : PartialDataSource, result : TableResult) : TableStoreData = {
        val memoryUsagePercentage = MemoryUsage.getMemoryUsagePercentage(Runtime.getRuntime)
        val storedResult = StoredTableResult.getStoredDataSource(partition, result, memoryUsagePercentage)
        val newMap : Map[DataSource, Map[PartialDataSource, StoredTableResult[PartialDataSource]]] = partitions + (partition.parent -> (partitions.getOrElse(partition.parent, HashMap()) + (partition -> storedResult)))
        return copy(partitions=newMap)
    }

    def updateHashes(dataSource : DependentDataSource, numPartitions : Int) : TableStoreData = {
        // It's possible that we don't have a hashed object for every dependency
        // Therefore, we filter the dependency for ones we actually have data for
        val dependencies = dataSource.getDependencies.filter(dependency => tables contains dependency)
        val memoryUsagePercentage = MemoryUsage.getMemoryUsagePercentage(Runtime.getRuntime)
        // Update the hashes in place
        val newMap = hashes ++ 
            // For each dependency
            dependencies.map(dependency => 
                // Get the first mapping
                (dependency, numPartitions) -> 
                // Calculate the partitions for that mapping
                dataSource.hashPartitionedData(tables(dependency).values.map(_.get).reduce(_ ++ _), numPartitions)
                    // Convert each partition into a stored table result
                    .map((partitionNum, tableResult) =>
                        // Maintain the mapping 
                        partitionNum ->
                        StoredTableResult.getStoredDependency(
                            ((dependency, numPartitions), partitionNum), 
                            tableResult, 
                            memoryUsagePercentage
                        )
                    )
            )
        return copy(hashes=newMap)
    }

    def deleteResult(table : Table) : TableStoreData = {
        tables(table).values.foreach(_.cleanup)
        return copy(
            tables = tables - table, 
            leastRecentlyUsed = leastRecentlyUsed.deleteAll(tables(table).values.toSet)
        )
    }

    def deletePartition(partition : DataSource) : TableStoreData = {
        partitions(partition).values.foreach(_.cleanup)
        return copy(
            partitions = partitions - partition,
            leastRecentlyUsed = leastRecentlyUsed.deleteAll(partitions(partition).values.toSet)
        )
    }

    def deleteHashes(dataSource : DataSource, numPartitions : Int) : TableStoreData = {
        val hashKeys = dataSource.getDependencies.map(table => (table, numPartitions))
        // Cleanup all dependencies
        hashKeys.foreach(hashes(_).values.foreach(_.cleanup))
        return copy(
            hashes = hashes -- hashKeys,
            leastRecentlyUsed = leastRecentlyUsed.deleteAll(hashKeys.map(hashes(_).values.toSet).reduce(_ union _))
        )
    }

    def cleanup : Unit = {
        // Everything that's in least recently used will be in memory, so call cleanup on those
        leastRecentlyUsed.order.foreach(_.cleanup)
    }