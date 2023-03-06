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
import com.typesafe.config.ConfigFactory

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
    final case class GetData(replyTo : ActorRef[TableStoreData]) extends TableStoreEvent
    // Reset state
    final case class Reset() extends TableStoreEvent

    

    def apply() : Behavior[TableStoreEvent] = empty
        
    def processResults(tableStoreData : TableStoreData) : Behavior[TableStoreEvent] = Behaviors.receive{ (context, message) =>
        message match {
            case AddResult(table, result, replyTo) =>
                val newData = tableStoreData.updateResult(table, result)
                replyTo ! StatusReply.ack()
                processResults(newData)

            case DeleteResult(table) => 
                processResults(tableStoreData.deleteResult(table))

            case GetResult(table, replyTo) =>
                val (resultOption, newTableStoreData) = tableStoreData.getResult(table)
                replyTo ! resultOption
                processResults(newTableStoreData)

            case GetAllResults(table, replyTo) =>
                val (resultSeq, newTableStoreData) = tableStoreData.getAllResults(table)
                replyTo ! resultSeq
                processResults(newTableStoreData)

            case AddPartition(partition, result, replyTo) =>
                val newData = tableStoreData.updatePartition(partition, result)
                replyTo ! StatusReply.ack()
                processResults(newData)

            case DeletePartition(partition) =>
                processResults(tableStoreData.deletePartition(partition))

            case GetPartition(partition, replyTo) => 
                val (resultOption, newTableStoreData) = tableStoreData.getPartition(partition)
                replyTo ! resultOption
                processResults(newTableStoreData)

            case HashPartition(dataSource, numPartitions, replyTo) =>
                val newData = tableStoreData.updateHashes(dataSource, numPartitions)
                replyTo ! StatusReply.ack()
                processResults(newData)

            case GetHash(table, totalPartitions, partitionNum, replyTo) =>
                val (resultOption, newTableStoreData) = tableStoreData.getHash(table, totalPartitions, partitionNum)
                replyTo ! resultOption
                processResults(newTableStoreData)

            case DeleteHash(dataSource, numPartitions) =>
                processResults(tableStoreData.deleteHashes(dataSource, numPartitions))

            case GetData(replyTo) =>
                replyTo ! tableStoreData
                Behaviors.same

            case Reset() =>    
                tableStoreData.cleanup
                empty
        }
    }

    def empty : Behavior[TableStoreEvent] = processResults(TableStoreData.empty)
}

object TableStoreData:
    val memoryUsageThreshold : Double = ConfigFactory.load.getString("clusterprocess.chunk.memory_usage_threshold_percent").toDouble

    def empty = TableStoreData(HashMap(), HashMap(), HashMap(), LRUCache())

/**
  * Immutable implementation of a TableStore
  * NOTE: operations are not always entirely side-effect free due to storage on disk.
  * If an operation is called on an instance of this object, do NOT use the previous object again
  * as it is possible that the data it references no longer exists on disk
  *
  * @param tables A mapping from Table -> PartialTable -> StoredTableResult
  * @param partitions A mapping from DataSource -> PartialDataSource -> StoredTableResult
  * @param hashes A mapping from (Table Dependency, Number of Partitions) -> Partition Number -> StoredTableResult
  * @param leastRecentlyUsed An instance of a least-recently-used list
  */
case class TableStoreData(
    tables : Map[Table, Map[PartialTable, StoredTableResult[PartialTable]]], 
    partitions : Map[DataSource, Map[PartialDataSource, StoredTableResult[PartialDataSource]]], 
    hashes : Map[(Table, Int), Map[Int, StoredTableResult[((Table, Int), Int)]]],
    leastRecentlyUsed : LRUCache[InMemoryTableResult[_]]):

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

    def access(e : InMemoryTableResult[_]) : TableStoreData = copy(leastRecentlyUsed = leastRecentlyUsed.access(e))

    def getResult(table : PartialTable) : (Option[TableResult], TableStoreData) =
        // Get the StoredTableResult, then match on its type
        tables.get(table.parent).flatMap(_.get(table)) match {
            case Some(t) => t match {
                case i : InMemoryTableResult[_] => (Some(i.get), access(i).checkSpill)
                case p : ProtobufTableResult[_] =>
                    // Replace the stored result with an in memory version again
                    val result = p.get
                    p.cleanup
                    (Some(result), updateResult(table, result))
            }
            case None => (None, checkSpill)
        }

    def getPartition(partition : PartialDataSource) : (Option[TableResult], TableStoreData) =
        // Get the StoredTableResult, then match on its type
        partitions.get(partition.parent).flatMap(_.get(partition)) match {
            case Some(t) => t match {
                case i : InMemoryTableResult[_] => (Some(i.get), access(i).checkSpill)
                case p : ProtobufTableResult[_] =>
                    // Replace the stored result with an in memory version again
                    val result = p.get
                    p.cleanup
                    (Some(result), updatePartition(partition, result))
            }
            case None => (None, checkSpill)
        }

    def getHash(source : Table, numPartitions : Int, partitionNum : Int) : (Option[TableResult], TableStoreData) =
        // Get the StoredTableResult, then match on its type
        hashes.get((source, numPartitions)).flatMap(_.get(partitionNum)) match {
            case Some(t) => t match {
                case i : InMemoryTableResult[_] => (Some(i.get), access(i).checkSpill)
                case p : ProtobufTableResult[_] =>
                    // Replace the stored result with an in memory version again
                    val result = p.get
                    p.cleanup
                    (Some(result), updateDependencyHash(((source, numPartitions), partitionNum), result))
            }
            case None => (None, checkSpill)
        }

    def getAllResults(table : Table) : (Seq[TableResult], TableStoreData) =
        // Get all the partialTables for this table, then fold starting from an empty set of results
        tables(table).foldLeft((Seq() : Seq[TableResult], checkSpill)){(inputData, storedDataPair) =>
            // Unpack the intermediate fold value
            val (items, tableStoreData) = inputData
            // Unpack the PartialTable -> StoredTableResult mapping
            val (partialTable, storedResult) = storedDataPair
            // Match on the stored table result, and read it into memory if required
            storedResult match {
                case i : InMemoryTableResult[_] => (items :+ i.get, tableStoreData.access(i).checkSpill)
                case p : ProtobufTableResult[_] =>
                    // Replace the stored result with an in memory version again
                    val result = p.get
                    p.cleanup
                    (items :+ result, tableStoreData.updateResult(partialTable, result))
                }
        }

    def updateResult(table : PartialTable, result : TableResult) : TableStoreData = {
        val currentStore = checkSpill
        val storedResult = InMemoryPartialTable(table, result)
        val newMap = tables + (table.parent -> (tables.getOrElse(table.parent, HashMap()) + (table -> storedResult)))
        return currentStore.copy(tables=newMap, leastRecentlyUsed = leastRecentlyUsed.add(storedResult))
    }

    def updatePartition(partition : PartialDataSource, result : TableResult) : TableStoreData = {
        val currentStore = checkSpill
        val storedResult = InMemoryPartialDataSource(partition, result)
        val newMap = partitions + (partition.parent -> (partitions.getOrElse(partition.parent, HashMap()) + (partition -> storedResult)))
        return currentStore.copy(partitions=newMap, leastRecentlyUsed = leastRecentlyUsed.add(storedResult))
    }

    def updateHashes(dataSource : DependentDataSource, numPartitions : Int) : TableStoreData = {
        val currentStore = checkSpill
        // It's possible that we don't have a hashed object for every dependency
        // Therefore, we filter the dependency for ones we actually have data for
        val dependencies = dataSource.getDependencies.filter(dependency => tables contains dependency)

        val newDependencies = 
            // For each dependency
            dependencies.map(dependency => 
                // Get the first mapping
                (dependency, numPartitions) -> 
                // Calculate the partitions for that mapping
                dataSource.hashPartitionedData(tables(dependency).values.map(_.get).reduce(_ ++ _), numPartitions)
                    // Convert each partition into a stored table result
                    .map((partitionNum, tableResult) => partitionNum -> InMemoryDependency(((dependency, numPartitions), partitionNum), tableResult))
            ).toMap

        // Update the hashes in place
        val newMap = hashes ++ newDependencies

        val newLRU = leastRecentlyUsed.addAll(newDependencies.values.flatMap(_.values).toSeq)

        return currentStore.copy(hashes = newMap, leastRecentlyUsed = newLRU)
    }

    def updateDependencyHash(dependencyData : ((Table, Int), Int), tableResult : TableResult) : TableStoreData = {
        val currentStore = checkSpill
        val storedResult = InMemoryDependency(dependencyData, tableResult)
        val newMap = hashes + (dependencyData._1 -> (hashes(dependencyData._1) + (dependencyData._2 -> storedResult)))
        return copy(hashes = newMap, leastRecentlyUsed = leastRecentlyUsed.add(storedResult))
    }

    def deleteResult(table : Table) : TableStoreData = {
        tables(table).values.foreach(_.cleanup)
        return copy(
            tables = tables - table, 
            leastRecentlyUsed = leastRecentlyUsed.deleteAll(tables(table).values.collect{case e : InMemoryTableResult[_] => e}.toSet)
        )
    }

    def deletePartition(partition : DataSource) : TableStoreData = {
        partitions(partition).values.foreach(_.cleanup)
        return copy(
            partitions = partitions - partition,
            leastRecentlyUsed = leastRecentlyUsed.deleteAll(partitions(partition).values.collect{case e : InMemoryTableResult[_] => e}.toSet)
        )
    }

    def deleteHashes(dataSource : DataSource, numPartitions : Int) : TableStoreData = {
        val hashKeys = dataSource.getDependencies.map(table => (table, numPartitions))
        // Cleanup all dependencies
        hashKeys.foreach(hashes(_).values.foreach(_.cleanup))
        return copy(
            hashes = hashes -- hashKeys,
            leastRecentlyUsed = leastRecentlyUsed.deleteAll(hashKeys.map(hashes(_).values.collect{case e : InMemoryTableResult[_] => e}.toSet).reduce(_ union _))
        )
    }

    /**
      * Cleanup entire TableStoreData - this deletes *everything* including data stored on disk
      */
    def cleanup : Unit = {
        // Everything that's in least recently used will be in memory, so call cleanup on those
        leastRecentlyUsed.order.foreach(_.cleanup)
    }

    /**
     * Spill only if required
     */
    def checkSpill : TableStoreData = if MemoryUsage.getMemoryUsagePercentage(Runtime.getRuntime) > TableStoreData.memoryUsageThreshold then spill else this

    /**
      * Perform a spill to disk on the most recently used item
      *
      * @return
      */
    def spill : TableStoreData = 
        // Get the least recently used item
        leastRecentlyUsed.getLeastRecentlyUsed
        // Spill it to disk
        .map(_.spillToDisk(this))
        // Fallback: no item is present (shouldn't happen), do nothing
        .getOrElse(this)