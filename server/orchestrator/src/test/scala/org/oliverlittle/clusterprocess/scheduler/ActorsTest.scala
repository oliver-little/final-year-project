package org.oliverlittle.clusterprocess.scheduler

import org.oliverlittle.clusterprocess.UnitSpec
import org.oliverlittle.clusterprocess.model.table._
import org.oliverlittle.clusterprocess.model.table.field._
import org.oliverlittle.clusterprocess.worker_query
import org.oliverlittle.clusterprocess.data_source
import org.oliverlittle.clusterprocess.table_model

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar
import akka.actor.testkit.typed.CapturedLogEvent
import akka.actor.testkit.typed.Effect._
import akka.actor.testkit.typed.scaladsl.BehaviorTestKit
import akka.actor.testkit.typed.scaladsl.TestInbox
import akka.actor.typed._
import akka.actor.typed.scaladsl._


class WorkProducerTest extends UnitSpec {
    "A WorkProducer" should "provide results while it has data" in {
        val one = worker_query.ComputePartialResultCassandraRequest(dataSource=Some(data_source.CassandraDataSource("test", "one")))
        val two = worker_query.ComputePartialResultCassandraRequest(dataSource=Some(data_source.CassandraDataSource("test", "two")))
        val data = Seq(one, two)
        val testKit = BehaviorTestKit(WorkProducer(data))
        val inbox = TestInbox[WorkConsumer.ConsumerEvent]()
        testKit.run(WorkProducer.RequestWork(inbox.ref))
        inbox.expectMessage(WorkConsumer.HasWork(one))
        testKit.run(WorkProducer.RequestWork(inbox.ref))
        inbox.expectMessage(WorkConsumer.HasWork(two))
    }

    it should "provide no results when it has no data" in {
        val testKit = BehaviorTestKit(WorkProducer(Seq()))
        val inbox = TestInbox[WorkConsumer.ConsumerEvent]()
        testKit.run(WorkProducer.RequestWork(inbox.ref))
        inbox.expectMessage(WorkConsumer.NoWork())
    }

    it should "stop providing results when it runs out" in {
        val one = worker_query.ComputePartialResultCassandraRequest(dataSource=Some(data_source.CassandraDataSource("test", "one")))
        val data = Seq(one)
        val testKit = BehaviorTestKit(WorkProducer(data))
        val inbox = TestInbox[WorkConsumer.ConsumerEvent]()
        testKit.run(WorkProducer.RequestWork(inbox.ref))
        inbox.expectMessage(WorkConsumer.HasWork(one))
        testKit.run(WorkProducer.RequestWork(inbox.ref))
        inbox.expectMessage(WorkConsumer.NoWork())
    }
}

class WorkConsumerTest extends UnitSpec with MockitoSugar {
    "A WorkConsumer" should "compute work when it receives it" in {
        val one = worker_query.ComputePartialResultCassandraRequest(dataSource=Some(data_source.CassandraDataSource("test", "one")))
        val mockStub = mock[worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceBlockingStub]
        val header = TableResultHeader(Seq(BaseStringField("a")))
        when(mockStub.computePartialResultCassandra(one)).thenReturn(Seq(table_model.StreamedTableResult().withHeader(header.protobuf)).iterator)

        val inbox = TestInbox[WorkProducer.ProducerEvent]()
        val assemblerInbox = TestInbox[WorkExecutionScheduler.AssemblerEvent]()
        val testKit = BehaviorTestKit(WorkConsumer(mockStub, Seq(inbox.ref), assemblerInbox.ref))
        
        inbox.expectMessage(WorkProducer.RequestWork(testKit.ref))
        testKit.run(WorkConsumer.HasWork(one))
        assemblerInbox.expectMessage(WorkExecutionScheduler.SendResult(EvaluatedTableResult(header, Seq())))
        inbox.expectMessage(WorkProducer.RequestWork(testKit.ref))
    }

    it should "move to the next producer when the current one is empty" in {
        val one = worker_query.ComputePartialResultCassandraRequest(dataSource=Some(data_source.CassandraDataSource("test", "one")))
        val mockStub = mock[worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceBlockingStub]
        val header = TableResultHeader(Seq(BaseStringField("a")))
        when(mockStub.computePartialResultCassandra(one)).thenReturn(Seq(table_model.StreamedTableResult().withHeader(header.protobuf)).iterator)

        val inbox = TestInbox[WorkProducer.ProducerEvent]()
        val inboxTwo = TestInbox[WorkProducer.ProducerEvent]()
        val assemblerInbox = TestInbox[WorkExecutionScheduler.AssemblerEvent]()
        val testKit = BehaviorTestKit(WorkConsumer(mockStub, Seq(inbox.ref, inboxTwo.ref), assemblerInbox.ref))
        
        inbox.expectMessage(WorkProducer.RequestWork(testKit.ref))
        testKit.run(WorkConsumer.NoWork())
        inboxTwo.expectMessage(WorkProducer.RequestWork(testKit.ref))
        testKit.run(WorkConsumer.HasWork(one))
        assemblerInbox.expectMessage(WorkExecutionScheduler.SendResult(EvaluatedTableResult(header, Seq())))
        inboxTwo.expectMessage(WorkProducer.RequestWork(testKit.ref))
    }

    it should "stop when all producers are empty" in {
        val one = worker_query.ComputePartialResultCassandraRequest(dataSource=Some(data_source.CassandraDataSource("test", "one")))
        val mockStub = mock[worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceBlockingStub]
        val header = TableResultHeader(Seq(BaseStringField("a")))
        when(mockStub.computePartialResultCassandra(one)).thenReturn(Seq(table_model.StreamedTableResult().withHeader(header.protobuf)).iterator)

        val inbox = TestInbox[WorkProducer.ProducerEvent]()
        val inboxTwo = TestInbox[WorkProducer.ProducerEvent]()
        val inboxThree = TestInbox[WorkProducer.ProducerEvent]()
        val assemblerInbox = TestInbox[WorkExecutionScheduler.AssemblerEvent]()
        val testKit = BehaviorTestKit(WorkConsumer(mockStub, Seq(inbox.ref, inboxTwo.ref, inboxThree.ref), assemblerInbox.ref))
        
        inbox.expectMessage(WorkProducer.RequestWork(testKit.ref))
        testKit.run(WorkConsumer.NoWork())
        inboxTwo.expectMessage(WorkProducer.RequestWork(testKit.ref))
        testKit.run(WorkConsumer.NoWork())
        inboxThree.expectMessage(WorkProducer.RequestWork(testKit.ref))
        testKit.run(WorkConsumer.NoWork())
        testKit.isAlive should be (false)
    }
}