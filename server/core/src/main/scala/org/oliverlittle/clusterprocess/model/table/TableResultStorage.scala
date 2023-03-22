package org.oliverlittle.clusterprocess.model.table

import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.model.table.field.TableValue
import org.oliverlittle.clusterprocess.model.table.sources.PartialDataSource
import org.oliverlittle.clusterprocess.dependency.SizeEstimator
import org.oliverlittle.clusterprocess.util.MemoryUsage

import scala.util.Properties.envOrElse
import math.BigDecimal.long2bigDecimal
import java.nio.file.{Path, Paths}
import java.nio.file.Files
import java.util.UUID

object StoredTableResult:
    def getDependency(dependencyData : ((Table, Int), Int), result : TableResult, threshold : Double)(using runtime : Runtime = Runtime.getRuntime) : StoredTableResult[((Table, Int), Int)] = 
        if MemoryUsage.getMemoryUsagePercentage(runtime) > threshold
        then 
            val storedPB = ProtobufTableResult(dependencyData)
            storedPB.store(result)
            storedPB
        else InMemoryDependency(dependencyData = dependencyData , result)
    
    def combineDependencies(data : Iterable[StoredTableResult[((Table, Int), Int)]], threshold : Double)(using runtime : Runtime = Runtime.getRuntime) : StoredTableResult[((Table, Int), Int)] = {
        val inMemoryResults = data.map {
                case i: InMemoryTableResult[_] => i
                case p : ProtobufTableResult[((Table, Int), Int)] => 
                    val res = p.get
                    p.cleanup
                    InMemoryDependency(p.source, res)
            }.toSeq
        
        return getDependency(inMemoryResults.head.source, inMemoryResults.map(_.get).reduce(_ ++ _), threshold)
    }

sealed trait StoredTableResult[T]:
    val source : T
    def get : TableResult
    def estimatedSizeMb : Long
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

    def estimatedSizeMb : Long = (SizeEstimator.estimate(tableResult) / 1000000).toLong

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
    // Generate a semi-random path
    val storagePath = Paths.get(envOrElse("SPILL_STORAGE_PATH", "temp/"), UUID.randomUUID.toString)

case class ProtobufTableResult[T](source : T) extends StoredTableResult[T]:
    // Add randomness, because PrepareHashes will use the same hashCode multiple times
    val uuid = UUID.randomUUID.toString
    val path = ProtobufTableResult.storagePath.resolve(source.getClass.getName + "/" + source.hashCode + uuid + ".table").toAbsolutePath()
    

    def store(tableResult : TableResult) : Unit = {
        // Make the directory first if required
        Files.createDirectories(path.getParent())
        Files.createFile(path)
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

    def estimatedSizeMb : Long = (Files.size(path) / 1000000).toLong

    def cleanup : Unit = Files.delete(path)