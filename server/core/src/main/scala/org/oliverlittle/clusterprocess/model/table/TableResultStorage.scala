package org.oliverlittle.clusterprocess.model.table

import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.model.table.field.TableValue
import org.oliverlittle.clusterprocess.model.table.sources.PartialDataSource

import scala.util.Properties.envOrElse
import java.nio.file.Path
import java.nio.file.Files

object StoredTableResult:
    def getStoredTable(table : PartialTable, result : TableResult, memoryUsagePercentage : Double) : StoredTableResult[PartialTable] = 
        if memoryUsagePercentage < memoryUsageThreshold
        then InMemoryPartialTable(table, result)
        else 
            val storedPB = ProtobufTableResult(table)
            storedPB.store(result)
            storedPB
    
    def getStoredDataSource(dataSource : PartialDataSource, result : TableResult, memoryUsagePercentage : Double) : StoredTableResult[PartialDataSource] = 
        if memoryUsagePercentage < memoryUsageThreshold
        then InMemoryPartialDataSource(dataSource, result)
        else
            val storedPB = ProtobufTableResult(dataSource)
            storedPB.store(result)
            storedPB

    def getStoredDependency(dependencyData : ((Table, Int), Int), result : TableResult, memoryUsagePercentage : Double) : StoredTableResult[((Table, Int), Int)] =
        if memoryUsagePercentage < memoryUsageThreshold
        then InMemoryDependency(dependencyData, result)
        else
            val storedPB = ProtobufTableResult(dependencyData)
            storedPB.store(result)
            storedPB

sealed trait StoredTableResult[T]:
    val source : T
    def get : TableResult
    def cleanup : Unit

trait InMemoryTableResult[T] extends StoredTableResult[T]:
    val tableResult : TableResult
    val get = tableResult
    /**
      * Spills this in-memory TableResult to disk
      *
      * @param tableStoreData
      * @return
      */
    def spillToDisk(tableStoreData : TableStoreData) : TableStoreData
    // No cleanup required for in memory data - JVM will handle it
    def cleanup: Unit = ()

case class InMemoryPartialTable(table : PartialTable, tableResult : TableResult) extends InMemoryTableResult[PartialTable]:
    val source = table
    def spillToDisk(tableStoreData: TableStoreData) : TableStoreData = {
        val storedPB = ProtobufTableResult(table)
        storedPB.store(tableResult)
        val newMap = tableStoreData.tables + (table.parent -> (tableStoreData.tables(table.parent) + (table -> storedPB)))
        return tableStoreData.copy(tables = newMap, leastRecentlyUsed = tableStoreData.leastRecentlyUsed.delete(this))
    }

case class InMemoryPartialDataSource(partition : PartialDataSource, tableResult : TableResult) extends InMemoryTableResult[PartialDataSource]:
    val source = partition
    def spillToDisk(tableStoreData: TableStoreData) : TableStoreData = {
        val storedPB = ProtobufTableResult(partition)
        storedPB.store(tableResult)
        val newMap = tableStoreData.partitions + (partition.parent -> (tableStoreData.partitions(partition.parent) + (partition -> storedPB)))
        return tableStoreData.copy(partitions = newMap, leastRecentlyUsed = tableStoreData.leastRecentlyUsed.delete(this))
    }


// Source is the input (Table, Number of Partitions, Partition Number)
case class InMemoryDependency(dependencyData : ((Table, Int), Int), tableResult : TableResult) extends InMemoryTableResult[((Table, Int), Int)]:
    val source = dependencyData
    def spillToDisk(tableStoreData: TableStoreData) : TableStoreData = {
        val storedPB = ProtobufTableResult(dependencyData)
        storedPB.store(tableResult)
        // Update this hash in the TableStore
        val newMap = tableStoreData.hashes + (dependencyData._1 -> (tableStoreData.hashes(dependencyData._1) + (dependencyData._2 -> storedPB)))
        return tableStoreData.copy(hashes = newMap, leastRecentlyUsed = tableStoreData.leastRecentlyUsed.delete(this))
    }

object ProtobufTableResult:
    val storagePath = Path.of(envOrElse("SPILL_STORAGE_PATH", "temp/"))

case class ProtobufTableResult[T](source : T) extends StoredTableResult[T]:
    val path = ProtobufTableResult.storagePath.resolve(source.getClass.getName + "/" + source.hashCode + ".table")

    def store(tableResult : TableResult) : Unit ={
        val outputStream = Files.newOutputStream(path)
        try {
            tableResult.protobuf.writeTo(outputStream)
        }
        finally {
            outputStream.close
        }
    } 

    def get : TableResult = {
        val inputStream = Files.newInputStream(path)
        try {
            TableResult.fromProtobuf(table_model.TableResult.parseFrom(inputStream))
        }
        finally {
            inputStream.close
        }
    }

    def cleanup : Unit = Files.delete(path)