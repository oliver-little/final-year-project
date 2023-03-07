package org.oliverlittle.clusterprocess.model.table

import org.oliverlittle.clusterprocess.UnitSpec
import org.oliverlittle.clusterprocess.model.table.sources.{PartialDataSource, MockDataSource, MockPartialDataSource}
import org.oliverlittle.clusterprocess.util.LRUCache

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar
import akka.actor.testkit.typed.scaladsl.BehaviorTestKit
import akka.actor.testkit.typed.scaladsl.TestInbox
import akka.actor.typed._
import akka.actor.typed.scaladsl._
import akka.pattern.StatusReply
import akka.Done

class TableStoreTest extends UnitSpec {
    "A TableStore" should "add a PartialTable" in {
        val testKit = BehaviorTestKit(TableStore())
        val resultInbox = TestInbox[StatusReply[Done]]()
        val table = PartialTable(MockPartialDataSource(), Seq())
        testKit.run(TableStore.AddResult(table, table.parent.empty, resultInbox.ref))
        resultInbox.expectMessage(StatusReply.ack())
        
        val dataInbox = TestInbox[TableStoreData]()
        testKit.run(TableStore.GetData(dataInbox.ref))
        val data = dataInbox.receiveMessage()
        val res = data.tables(table.parent)(table) 
        res shouldBe a [StoredTableResult[PartialTable]]
        res.get should be (table.parent.empty)
    }

    it should "remove all instances of a Table" in {
        // Setup and verify exists
        val testKit = BehaviorTestKit(TableStore())
        val resultInbox = TestInbox[StatusReply[Done]]()
        val table = PartialTable(MockPartialDataSource(), Seq())
        testKit.run(TableStore.AddResult(table, table.parent.empty, resultInbox.ref))
        resultInbox.expectMessage(StatusReply.ack())
        
        val dataInbox = TestInbox[TableStoreData]()
        testKit.run(TableStore.GetData(dataInbox.ref))
        val data = dataInbox.receiveMessage()
        val res = data.tables(table.parent)(table) 
        res shouldBe a [StoredTableResult[PartialTable]]
        res.get should be (table.parent.empty)

        testKit.run(TableStore.DeleteResult(table.parent))

        testKit.run(TableStore.GetData(dataInbox.ref))
        dataInbox.receiveMessage().tables.contains(table.parent) should be (false)
    }

    it should "get a result for a PartialTable" in {
        // Setup
        val testKit = BehaviorTestKit(TableStore())
        val resultInbox = TestInbox[StatusReply[Done]]()
        val table = PartialTable(MockPartialDataSource(), Seq())
        testKit.run(TableStore.AddResult(table, table.parent.empty, resultInbox.ref))
        resultInbox.expectMessage(StatusReply.ack())
        
        val dataInbox = TestInbox[Option[TableResult]]()
        testKit.run(TableStore.GetResult(table, dataInbox.ref))
        dataInbox.receiveMessage() should be (Some(table.parent.empty))

        testKit.run(TableStore.GetResult(PartialTable(MockPartialDataSource(), Seq()), dataInbox.ref))
        dataInbox.receiveMessage() should be (None)
    }

    it should "get all results for a Table" in {
        // Setup
        val testKit = BehaviorTestKit(TableStore())
        val resultInbox = TestInbox[StatusReply[Done]]()
        val dataSource = MockDataSource()
        val table = PartialTable(MockPartialDataSource(dataSource), Seq())
        testKit.run(TableStore.AddResult(table, table.parent.empty, resultInbox.ref))
        resultInbox.expectMessage(StatusReply.ack())
        val tableTwo = PartialTable(MockPartialDataSource(dataSource), Seq())
        testKit.run(TableStore.AddResult(tableTwo, tableTwo.parent.empty, resultInbox.ref))
        resultInbox.expectMessage(StatusReply.ack())
        
        val dataInbox = TestInbox[Seq[TableResult]]()
        testKit.run(TableStore.GetAllResults(table.parent, dataInbox.ref))
        dataInbox.receiveMessage().toSet should be (Set(table.parent.empty, tableTwo.parent.empty))

        testKit.run(TableStore.GetAllResults(Table(MockDataSource(), Seq()), dataInbox.ref))
        dataInbox.receiveMessage() should be (Seq())
    }

    it should "add a result for a PartialDataSource" in {
        val testKit = BehaviorTestKit(TableStore())
        val resultInbox = TestInbox[StatusReply[Done]]()
        val dataSource = MockPartialDataSource()
        testKit.run(TableStore.AddPartition(dataSource, dataSource.parent.empty, resultInbox.ref))
        resultInbox.expectMessage(StatusReply.ack())

        val dataInbox = TestInbox[TableStoreData]()
        testKit.run(TableStore.GetData(dataInbox.ref))
        val res = dataInbox.receiveMessage().partitions(dataSource.parent)(dataSource) 
        res shouldBe a [StoredTableResult[PartialDataSource]]
        res.get should be (dataSource.parent.empty)
    }

    it should "remove all partitions for a DataSource" in {
        val testKit = BehaviorTestKit(TableStore())
        val resultInbox = TestInbox[StatusReply[Done]]()
        val dataSource = MockPartialDataSource()
        testKit.run(TableStore.AddPartition(dataSource, dataSource.parent.empty, resultInbox.ref))
        resultInbox.expectMessage(StatusReply.ack())

        val dataInbox = TestInbox[TableStoreData]()
        testKit.run(TableStore.GetData(dataInbox.ref))
        val res = dataInbox.receiveMessage().partitions(dataSource.parent) 
        res.size should be (1)
        res(dataSource).get should be (dataSource.parent.empty)

        testKit.run(TableStore.DeletePartition(dataSource.parent))

        testKit.run(TableStore.GetData(dataInbox.ref))
        dataInbox.receiveMessage().partitions.contains(dataSource.parent) should be (false)

    }

    it should "get the result for a PartialDataSource" in {
        val testKit = BehaviorTestKit(TableStore())
        val resultInbox = TestInbox[StatusReply[Done]]()
        val dataSource = MockPartialDataSource()
        testKit.run(TableStore.AddPartition(dataSource, dataSource.parent.empty, resultInbox.ref))
        resultInbox.expectMessage(StatusReply.ack())

        val dataInbox = TestInbox[Option[TableResult]]()
        testKit.run(TableStore.GetPartition(dataSource, dataInbox.ref))
        dataInbox.receiveMessage() should be (Some(dataSource.parent.empty))

        testKit.run(TableStore.GetPartition(MockPartialDataSource(), dataInbox.ref))
        dataInbox.receiveMessage() should be (None)
    }

    it should "hash a PartialDataSource's dependencies" in {
        // Setup
        val testKit = BehaviorTestKit(TableStore())
        val resultInbox = TestInbox[StatusReply[Done]]()
        val dataSource = MockPartialDataSource()
        val table = dataSource.parent.getDependencies(0)
        val partialTable = PartialTable(table.dataSource.asInstanceOf[MockDataSource].partial, Seq())
        testKit.run(TableStore.AddResult(partialTable, partialTable.parent.empty, resultInbox.ref))
        resultInbox.expectMessage(StatusReply.ack())

        testKit.run(TableStore.HashPartition(dataSource.parent, 2, resultInbox.ref))
        resultInbox.expectMessage(StatusReply.ack())

        val dataInbox = TestInbox[TableStoreData]()
        testKit.run(TableStore.GetData(dataInbox.ref))
        dataInbox.receiveMessage().hashes((partialTable.parent, 2)).view.mapValues(_.get).toMap should be (dataSource.parent.partitionHash.toMap)
    }

    it should "get the hashed dependency" in {
        // Setup
        val testKit = BehaviorTestKit(TableStore())
        val resultInbox = TestInbox[StatusReply[Done]]()
        val dataSource = MockPartialDataSource()
        val table = dataSource.parent.getDependencies(0)
        val partialTable = PartialTable(table.dataSource.asInstanceOf[MockDataSource].partial, Seq())
        testKit.run(TableStore.AddResult(partialTable, partialTable.parent.empty, resultInbox.ref))
        resultInbox.expectMessage(StatusReply.ack())

        testKit.run(TableStore.HashPartition(dataSource.parent, 2, resultInbox.ref))
        resultInbox.expectMessage(StatusReply.ack())

        val dataInbox = TestInbox[Option[TableResult]]()
        testKit.run(TableStore.GetHash(table, 2, 0, dataInbox.ref))
        dataInbox.receiveMessage() should be (Some(dataSource.parent.partitionHash(0)))

        testKit.run(TableStore.GetHash(table, 3, 1, dataInbox.ref))
        dataInbox.receiveMessage() should be (None)
    }

    it should "delete a hashed dependency" in {
        // Setup
        val testKit = BehaviorTestKit(TableStore())
        val resultInbox = TestInbox[StatusReply[Done]]()
        val dataSource = MockPartialDataSource()
        val table = dataSource.parent.getDependencies(0)
        val partialTable = PartialTable(table.dataSource.asInstanceOf[MockDataSource].partial, Seq())
        testKit.run(TableStore.AddResult(partialTable, partialTable.parent.empty, resultInbox.ref))
        resultInbox.expectMessage(StatusReply.ack())

        testKit.run(TableStore.HashPartition(dataSource.parent, 2, resultInbox.ref))
        resultInbox.expectMessage(StatusReply.ack())

        val dataInbox = TestInbox[TableStoreData]()
        testKit.run(TableStore.GetData(dataInbox.ref))
        dataInbox.receiveMessage().hashes((partialTable.parent, 2)).view.mapValues(_.get).toMap should be (dataSource.parent.partitionHash.toMap)

        testKit.run(TableStore.DeleteHash(dataSource.parent, 2))

        testKit.run(TableStore.GetData(dataInbox.ref))
        dataInbox.receiveMessage().hashes.contains((partialTable.parent, 2)) should be (false)
    }

    it should "get the TableStoreData" in {
        val testKit = BehaviorTestKit(TableStore()) 
        val dataInbox = TestInbox[TableStoreData]()
        testKit.run(TableStore.GetData(dataInbox.ref))
        val data = dataInbox.receiveMessage()
        data should be (TableStoreData.empty)
    }

    it should "reset all data" in {
        // Setup
        val testKit = BehaviorTestKit(TableStore())
        val resultInbox = TestInbox[StatusReply[Done]]()
        val table = PartialTable(MockPartialDataSource(), Seq())
        testKit.run(TableStore.AddResult(table, table.parent.empty, resultInbox.ref))
        resultInbox.expectMessage(StatusReply.ack())
        
        val dataInbox = TestInbox[TableStoreData]()
        testKit.run(TableStore.GetData(dataInbox.ref))
        val res = dataInbox.receiveMessage().tables(table.parent)(table) 
        res shouldBe a [StoredTableResult[PartialTable]]
        res.get should be (table.parent.empty)

        testKit.run(TableStore.Reset())

        testKit.run(TableStore.GetData(dataInbox.ref))
        dataInbox.receiveMessage() should be (TableStoreData.empty)
    }
}

class TableStoreDataTest extends UnitSpec with MockitoSugar {
    "A TableStoreData" should "call the cleanup operation on all stored data" in {
        val table = PartialTable(MockPartialDataSource())
        val stored = mock[InMemoryPartialTable]
        val originalTableStoreData = TableStoreData(Map(table.parent -> Map(table -> stored)), Map(), Map(), LRUCache[InMemoryTableResult[_]](Seq(stored)))
        originalTableStoreData.cleanup
        verify(stored, times(1)).cleanup
    }

    it should "spill to disk if the memory usage is over the threshold" in {
        val runtimeMock = mock[Runtime]
        when(runtimeMock.maxMemory).thenReturn(200L)
        when(runtimeMock.totalMemory).thenReturn(180L)
        when(runtimeMock.freeMemory).thenReturn(30L)

        val table = PartialTable(MockPartialDataSource())
        val inMemory = InMemoryPartialTable(table, table.parent.empty)
        val originalTableStoreData = TableStoreData(Map(table.parent -> Map(table -> inMemory)), Map(), Map(), LRUCache[InMemoryTableResult[_]](Seq(inMemory)))
        val newTableStoreData = originalTableStoreData.checkSpill(using runtimeMock)
        val res = newTableStoreData.tables(table.parent)(table)
        try {
            res shouldBe a [ProtobufTableResult[PartialTable]]
            res.get should be (table.parent.empty)
        }
        finally {
            res.cleanup
        }
    }

    it should "return the same instance of TableStoreData if no spilling is required" in {
        val runtimeMock = mock[Runtime]
        when(runtimeMock.maxMemory).thenReturn(200L)
        when(runtimeMock.totalMemory).thenReturn(180L)
        when(runtimeMock.freeMemory).thenReturn(100L)

        val table = PartialTable(MockPartialDataSource())
        val inMemory = InMemoryPartialTable(table, table.parent.empty)
        val originalTableStoreData = TableStoreData(Map(table.parent -> Map(table -> inMemory)), Map(), Map(), LRUCache[InMemoryTableResult[_]](Seq(inMemory)))
        val newTableStoreData = originalTableStoreData.checkSpill(using runtimeMock)
        val res = newTableStoreData.tables(table.parent)(table)
        try {
            res shouldBe a [InMemoryTableResult[PartialTable]]
            res.get should be (table.parent.empty)
        }
        finally {
            res.cleanup
        }
    }
}