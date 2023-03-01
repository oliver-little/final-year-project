package org.oliverlittle.clusterprocess.query

import org.oliverlittle.clusterprocess.worker_query
import org.oliverlittle.clusterprocess.{AsyncUnitSpec, UnitSpec}
import org.oliverlittle.clusterprocess.connector.grpc.{WorkerHandler, ChannelManager, MockitoChannelManager}
import org.oliverlittle.clusterprocess.model.table._
import org.oliverlittle.clusterprocess.model.table.sources._
import org.oliverlittle.clusterprocess.model.field.expressions._

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar
import org.mockito.AdditionalAnswers

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed._
import akka.actor.typed.scaladsl._
import akka.util.Timeout
import akka.actor.typed.scaladsl.AskPattern._
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.Future
import io.grpc.StatusRuntimeException
import org.scalatest.BeforeAndAfterEach

class QueryPlanItemSchedulerTest extends UnitSpec with MockitoSugar with BeforeAndAfterAll {
    val testKit : ActorTestKit = ActorTestKit()

    override def beforeAll() : Unit = {
        super.beforeAll()
    }    

    override def afterAll() : Unit = {
        super.afterAll()
        testKit.shutdownTestKit()
    }

    "A QueryPlanItemScheduler" should "call the execute method of its QueryPlanItem, intercept the return message and stop" in {
        val probe = testKit.createTestProbe[QueryInstruction]()
        val scheduler = testKit.spawn(QueryPlanItemScheduler(IdentityQueryPlanItem(), mock[WorkerHandler], probe.ref))
        probe.expectMessage(InstructionComplete())
        probe.expectTerminated(scheduler)
    }
}
class PrepareResultTest extends UnitSpec with MockitoSugar with BeforeAndAfterAll {
    // TestKit setup and teardown
    val testKit : ActorTestKit = ActorTestKit()

    override def beforeAll() : Unit = {
        super.beforeAll()
    }    

    override def afterAll() : Unit = {
        super.afterAll()
        testKit.shutdownTestKit()
    }
    
    "A PrepareResult" should "throw an IllegalStateException if executed" in {
        val ds = MockDataSource()
        val table = Table(ds, Seq(SelectTransformation(F("a") as "b")))

        val probe = testKit.createTestProbe[QueryInstruction]()
        val scheduler = testKit.spawn(QueryPlanItemScheduler(PrepareResult(table), mock[WorkerHandler], probe.ref))
        val msg = probe.receiveMessage() 
        msg shouldBe a [InstructionError]
        msg.asInstanceOf[InstructionError].exception shouldBe a [IllegalStateException]
        probe.expectTerminated(scheduler)
    }

    it should "return PrepareResultWithPartitions when provided with PartialDataSource partitions" in {
        val ds = MockDataSource()
        val partial = MockPartialDataSource(ds)
        val table = Table(ds, Seq(SelectTransformation(F("a") as "b")))
        val map : Map[ChannelManager, Seq[PartialDataSource]] = Map(MockitoChannelManager() -> Seq(partial))
        PrepareResult(table).usePartitions(map) should be (PrepareResultWithPartitions(table, map))
    }
}

class PrepareResultWithPartitionsTest extends worker_query.WorkerComputeServiceTestServer(2) with MockitoSugar with BeforeAndAfterAll {
    // TestKit setup and teardown
    val testKit : ActorTestKit = ActorTestKit()

    override def beforeAll() : Unit = {
        super.beforeAll()
    }    

    override def afterAll() : Unit = {
        super.afterAll()
        testKit.shutdownTestKit()
    }

    // Define server implementation
    val serviceImpl = mock[worker_query.WorkerComputeServiceGrpc.WorkerComputeService]

    "A PrepareResultWithPartitions" should "calculate queries correctly" in {
        val ds = MockDataSource()
        val partial = MockPartialDataSource(ds)
        val table = Table(ds, Seq(SelectTransformation(F("a") as "b")))
        val partialTable = table.withPartialDataSource(partial)
        val channel = MockitoChannelManager()
        val map : Map[ChannelManager, Seq[PartialDataSource]] = Map(channel -> Seq(partial))
        PrepareResultWithPartitions(table, map).queries should be (Map(channel -> Seq((partialTable, worker_query.QueryPlanItem().withPrepareResult(worker_query.PrepareResult(Some(partialTable.protobuf)))))))
    }

    it should "calculate the partitions count correctly" in {
        val ds = MockDataSource()
        val partial = MockPartialDataSource(ds)
        val table = Table(ds, Seq(SelectTransformation(F("a") as "b")))
        val partialTable = table.withPartialDataSource(partial)
        val map : Map[ChannelManager, Seq[PartialDataSource]] = Map(MockitoChannelManager() -> Seq(partial, partial), MockitoChannelManager() -> Seq(partial))
        PrepareResultWithPartitions(table, map).partitionsCount should be (3)
    }

    it should "send a message when completed" in {
        when(serviceImpl.processQueryPlanItem(any[worker_query.QueryPlanItem]())).thenReturn(Future.successful(worker_query.ProcessQueryPlanItemResult(true)))

        val ds = MockDataSource()
        val partial = MockPartialDataSource(ds)
        val table = Table(ds, Seq(SelectTransformation(F("a") as "b")))
        val partialTable = table.withPartialDataSource(partial)
        // Use the TestServer channels here
        val partitions : Map[ChannelManager, Seq[PartialDataSource]] = Map(channels(0) -> Seq(partial, partial), channels(1) -> Seq(partial))
        val outputPartitions = partitions.view.mapValues(s => s.map(partialDS => table.withPartialDataSource(partialDS))).toMap
        val probe = testKit.createTestProbe[QueryInstruction]()
        val scheduler = testKit.spawn(QueryPlanItemScheduler(PrepareResultWithPartitions(table, partitions), mock[WorkerHandler], probe.ref))
        probe.receiveMessage() should be (InstructionCompleteWithTableOutput(outputPartitions))
    }

    it should "send an error message if an error occurred" in {
        when(serviceImpl.processQueryPlanItem(any[worker_query.QueryPlanItem]())).thenReturn(Future.failed(new IllegalStateException("Test exception")))

        val ds = MockDataSource()
        val partial = MockPartialDataSource(ds)
        val table = Table(ds, Seq(SelectTransformation(F("a") as "b")))
        val partialTable = table.withPartialDataSource(partial)
        // Use the TestServer channels here
        val partitions : Map[ChannelManager, Seq[PartialDataSource]] = Map(channels(0) -> Seq(partial, partial), channels(1) -> Seq(partial))
        val outputPartitions = partitions.view.mapValues(s => s.map(partialDS => table.withPartialDataSource(partialDS))).toMap
        val probe = testKit.createTestProbe[QueryInstruction]()
        val scheduler = testKit.spawn(QueryPlanItemScheduler(PrepareResultWithPartitions(table, partitions), mock[WorkerHandler], probe.ref))
        val msg = probe.receiveMessage() 
        msg shouldBe a [InstructionError]
        msg.asInstanceOf[InstructionError].exception shouldBe a [StatusRuntimeException]
    }
}

class DeleteResultTest extends worker_query.WorkerComputeServiceTestServer(5) with MockitoSugar with BeforeAndAfterAll {
    // TestKit setup and teardown
    val testKit : ActorTestKit = ActorTestKit()

    override def beforeAll() : Unit = {
        super.beforeAll()
    }    

    override def afterAll() : Unit = {
        super.afterAll()
        testKit.shutdownTestKit()
    }

    // Define server implementation - note, verifying does not work on this
    val serviceImpl = mock[worker_query.WorkerComputeServiceGrpc.WorkerComputeService]

    "A DeleteResult" should "send DeleteResult messages to workers" in {
        when(serviceImpl.processQueryPlanItem(any[worker_query.QueryPlanItem]())).thenReturn(Future.successful(worker_query.ProcessQueryPlanItemResult(true)))
        val ds = MockDataSource()
        val table = Table(ds, Seq(SelectTransformation(F("a") as "b")))

        val probe = testKit.createTestProbe[QueryInstruction]()
        val scheduler = testKit.spawn(QueryPlanItemScheduler(DeleteResult(table), workerHandler, probe.ref))

        probe.receiveMessage() should be (InstructionComplete())
    }

    it should "forward partitions if it has any" in {
        when(serviceImpl.processQueryPlanItem(any[worker_query.QueryPlanItem]())).thenReturn(Future.successful(worker_query.ProcessQueryPlanItemResult(true)))
        val ds = MockDataSource()
        val table = Table(ds, Seq(SelectTransformation(F("a") as "b")))

        val probe = testKit.createTestProbe[QueryInstruction]()
        val scheduler = testKit.spawn(QueryPlanItemScheduler(DeleteResult(table, Some(Map())), workerHandler, probe.ref))

        probe.receiveMessage() should be (InstructionCompleteWithDataSourceOutput(Map()))
    }

    it should "send an error message if an error occurs" in {
        when(serviceImpl.processQueryPlanItem(any[worker_query.QueryPlanItem]())).thenReturn(Future.failed(new IllegalStateException("Test exception")))
        
        val ds = MockDataSource()
        val table = Table(ds, Seq(SelectTransformation(F("a") as "b")))

        val probe = testKit.createTestProbe[QueryInstruction]()
        val scheduler = testKit.spawn(QueryPlanItemScheduler(DeleteResult(table), workerHandler, probe.ref))

        val msg = probe.receiveMessage() 
        msg shouldBe a [InstructionError]
        msg.asInstanceOf[InstructionError].exception shouldBe a [StatusRuntimeException]
    }
}
class GetPartitionTest extends worker_query.WorkerComputeServiceTestServer(5) with MockitoSugar with BeforeAndAfterAll with BeforeAndAfterEach {
    // TestKit setup and teardown
    val testKit : ActorTestKit = ActorTestKit()

    override def beforeAll() : Unit = {
        super.beforeAll()
    }    

    override def afterAll() : Unit = {
        super.afterAll()
        testKit.shutdownTestKit()
    }

    override protected def beforeEach(): Unit = {
        super.beforeEach()
        delegateClass.reset()
    }

    class WorkerComputeServiceImpl() {
        var numPrepareHashes : Int = 0
        var numDeletePreparedHashes : Int = 0

        val response : Future[worker_query.ProcessQueryPlanItemResult] = Future.successful(worker_query.ProcessQueryPlanItemResult(true))

        def processQueryPlanItem(request : worker_query.QueryPlanItem) : Future[worker_query.ProcessQueryPlanItemResult] = request.item match {
            case worker_query.QueryPlanItem.Item.PrepareHashes(value) => 
                numPrepareHashes += 1
                response
            case worker_query.QueryPlanItem.Item.DeletePreparedHashes(value) => 
                numDeletePreparedHashes += 1
                response
            case _ => 
                response
        }

        def reset() : Unit = {
            numPrepareHashes = 0
            numDeletePreparedHashes = 0
        }
    }

    val delegateClass = new WorkerComputeServiceImpl()
    // Define server implementation - note, verifying does not work on this
    val serviceImpl = mock[worker_query.WorkerComputeServiceGrpc.WorkerComputeService](AdditionalAnswers.delegatesTo(delegateClass))

    "A GetPartition" should "send prepare hashes and delete hashes messages if the data source has dependencies" in {
        val ds = MockDataSource()

        val probe = testKit.createTestProbe[QueryInstruction]()
        val scheduler = testKit.spawn(QueryPlanItemScheduler(GetPartition(ds), workerHandler, probe.ref))
        val msg = probe.receiveMessage()
        msg shouldBe a [InstructionCompleteWithDataSourceOutput]
        val castedMsg = msg.asInstanceOf[InstructionCompleteWithDataSourceOutput]
        castedMsg.partitions.values.map(_.size).sum should be (1)

        // Number of mock channels
        delegateClass.numPrepareHashes should be (5)
        delegateClass.numDeletePreparedHashes should be (5)
    }

    it should "send a message with Data Source partitions when it completes" in {
        // Specifically use a nondependent data source so we can check that prepare/delete was not called
        val ds = MockRootDataSource()

        val probe = testKit.createTestProbe[QueryInstruction]()
        val scheduler = testKit.spawn(QueryPlanItemScheduler(GetPartition(ds), workerHandler, probe.ref))
        val msg = probe.receiveMessage()
        msg shouldBe a [InstructionCompleteWithDataSourceOutput]
        val castedMsg = msg.asInstanceOf[InstructionCompleteWithDataSourceOutput]
        castedMsg.partitions.values.map(_.size).sum should be (1)

        // Should be 0 as there are no dependencies
        delegateClass.numPrepareHashes should be (0)
        delegateClass.numDeletePreparedHashes should be (0)
    }
}

class DeletePartitionTest extends worker_query.WorkerComputeServiceTestServer(5) with MockitoSugar with BeforeAndAfterAll {
    // TestKit setup and teardown
    val testKit : ActorTestKit = ActorTestKit()

    override def beforeAll() : Unit = {
        super.beforeAll()
    }    

    override def afterAll() : Unit = {
        super.afterAll()
        testKit.shutdownTestKit()
    }

    // Define server implementation - note, verifying does not work on this
    val serviceImpl = mock[worker_query.WorkerComputeServiceGrpc.WorkerComputeService]

    "A DeletePartition" should "send DeletePartition messages to all workers" in {
        when(serviceImpl.processQueryPlanItem(any[worker_query.QueryPlanItem]())).thenReturn(Future.successful(worker_query.ProcessQueryPlanItemResult(true)))
        val ds = MockDataSource()

        val probe = testKit.createTestProbe[QueryInstruction]()
        val scheduler = testKit.spawn(QueryPlanItemScheduler(DeletePartition(ds), workerHandler, probe.ref))

        probe.receiveMessage() should be (InstructionComplete())
    }

    it should "send an error message if an error occurs" in {
        when(serviceImpl.processQueryPlanItem(any[worker_query.QueryPlanItem]())).thenReturn(Future.failed(new IllegalStateException("Test exception")))
        
        val ds = MockDataSource()

        val probe = testKit.createTestProbe[QueryInstruction]()
        val scheduler = testKit.spawn(QueryPlanItemScheduler(DeletePartition(ds), workerHandler, probe.ref))

        val msg = probe.receiveMessage() 
        msg shouldBe a [InstructionError]
        msg.asInstanceOf[InstructionError].exception shouldBe a [StatusRuntimeException]
    }
}