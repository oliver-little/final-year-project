package org.oliverlittle.clusterprocess.model.table

import org.oliverlittle.clusterprocess.UnitSpec
import org.oliverlittle.clusterprocess.model.table.sources.{PartialDataSource, MockDataSource, MockPartialDataSource}
import org.oliverlittle.clusterprocess.util.LRUCache
import org.oliverlittle.clusterprocess.model.table.ProtobufTableResult.storagePath

import java.nio.file.Files

class InMemoryPartialTableTest extends UnitSpec {
    "An InMemoryPartialTable" should "spill the result to disk, and update the TableStoreData" in {
        val table = PartialTable(MockPartialDataSource())
        val inMemory = InMemoryPartialTable(table, table.parent.empty)
        val originalTableStoreData = TableStoreData(Map(table.parent -> Map(table -> inMemory)), Map(), Map(), LRUCache[InMemoryTableResult[_]](Seq(inMemory)))
        val newTableStoreData = inMemory.spillToDisk(originalTableStoreData)
        val res = newTableStoreData.tables(table.parent)(table) 
        res should be (ProtobufTableResult(table))
        res.get should be (table.parent.empty)
        res.cleanup
    }
}

class InMemoryPartialDataSourceTest extends UnitSpec {
    "An InMemoryPartialDataSource" should "spill the result to disk, and update the TableStoreData" in {
        val ds = MockPartialDataSource()
        val inMemory = InMemoryPartialDataSource(ds, ds.parent.empty)
        val originalTableStoreData = TableStoreData(Map(), Map(ds.parent -> Map(ds -> inMemory)), Map(), LRUCache[InMemoryTableResult[_]](Seq(inMemory)))
        val newTableStoreData = inMemory.spillToDisk(originalTableStoreData)
        val res = newTableStoreData.partitions(ds.parent)(ds) 
        res should be (ProtobufTableResult(ds))
        res.get should be (ds.parent.empty)
        res.cleanup
    }
}

class InMemoryDependencyTest extends UnitSpec {
    "An InMemoryDependency" should "spill the result to disk, and update the TableStoreData" in {
        val table = Table(MockDataSource())
        val inMemory = InMemoryDependency(((table, 2), 0), table.empty)
        val originalTableStoreData = TableStoreData(Map(), Map(), Map((table, 2) -> Map(0 -> inMemory)),  LRUCache[InMemoryTableResult[_]](Seq(inMemory)))
        val newTableStoreData = inMemory.spillToDisk(originalTableStoreData)
        val res = newTableStoreData.hashes((table, 2))(0) 
        res should be (ProtobufTableResult(((table, 2), 0)))
        res.get should be (table.empty)
        res.cleanup
    }
}

class ProtobufTableResultTest extends UnitSpec {
    "A ProtobufTableResult" should "resolve the correct path from the environment variable, and the source" in {
        // This is slightly randomised, so get the storagePath
        val storagePath = ProtobufTableResult.storagePath
        ProtobufTableResult(1).path should be (storagePath.resolve("java.lang.Integer/" + 1.hashCode + ".table").toAbsolutePath())
    }

    it should "store a TableResult at the path location" in {
        val table = Table(MockDataSource())
        val storagePath = ProtobufTableResult.storagePath
        val path = storagePath.resolve("java.lang.Integer/" + 1.hashCode + ".table").toAbsolutePath()
        val storedPB = ProtobufTableResult(1)
        storedPB.store(table.empty)
        Files.exists(path) should be (true)
        storedPB.cleanup
    }

    it should "retrieve a TableResult from the path location" in {
        val table = Table(MockDataSource())
        val storagePath = ProtobufTableResult.storagePath
        val path = storagePath.resolve("java.lang.Integer/" + 1.hashCode + ".table").toAbsolutePath()
        val storedPB = ProtobufTableResult(1)
        storedPB.store(table.empty)
        Files.exists(path) should be (true)
        storedPB.get should be (table.empty)
        storedPB.cleanup
    }

    it should "delete a stored file on cleanup" in {
        val table = Table(MockDataSource())
        val storagePath = ProtobufTableResult.storagePath
        val path = storagePath.resolve("java.lang.Integer/" + 1.hashCode + ".table").toAbsolutePath()
        val storedPB = ProtobufTableResult(1)
        storedPB.store(table.empty)
        Files.exists(path) should be (true)
        storedPB.cleanup
        Files.exists(path) should be (false)
    }
}