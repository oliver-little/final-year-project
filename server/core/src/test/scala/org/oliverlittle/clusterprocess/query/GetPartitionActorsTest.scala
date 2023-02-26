package org.oliverlittle.clusterprocess.query

import org.oliverlittle.clusterprocess.worker_query
import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.{AsyncUnitSpec, UnitSpec}
import org.oliverlittle.clusterprocess.model.table._
import org.oliverlittle.clusterprocess.model.table.field._
import org.oliverlittle.clusterprocess.model.table.sources._
import org.oliverlittle.clusterprocess.connector.grpc.{ChannelManager, MockChannelManager}

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar
import akka.actor.testkit.typed.scaladsl.BehaviorTestKit
import akka.actor.testkit.typed.scaladsl.TestInbox
import akka.actor.typed._
import akka.actor.typed.scaladsl._

import scala.concurrent.Promise


class GetPartitionProducerTest extends UnitSpec {
    "A GetPartitionProducer" should "provide results while it has data" in {
        val partialDataSource = MockPartialDataSource()
        val one = (partialDataSource, worker_query.QueryPlanItem().withGetPartition(worker_query.GetPartition(Some(partialDataSource.protobuf), Seq())))
        val two = (partialDataSource, worker_query.QueryPlanItem().withGetPartition(worker_query.GetPartition(Some(partialDataSource.protobuf), Seq())))
        val data = Seq(one, two)
        val testKit = BehaviorTestKit(GetPartitionProducer(data))
        val inbox = TestInbox[GetPartitionConsumer.ConsumerEvent]()
        testKit.run(GetPartitionProducer.RequestWork(inbox.ref))
        inbox.expectMessage(GetPartitionConsumer.HasWork(one._1, one._2))
        testKit.run(GetPartitionProducer.RequestWork(inbox.ref))
        inbox.expectMessage(GetPartitionConsumer.HasWork(two._1, two._2))
    }

    it should "provide no results when it has no data" in {
        val testKit = BehaviorTestKit(GetPartitionProducer(Seq()))
        val inbox = TestInbox[GetPartitionConsumer.ConsumerEvent]()
        testKit.run(GetPartitionProducer.RequestWork(inbox.ref))
        inbox.expectMessage(GetPartitionConsumer.NoWork())
    }

    it should "stop providing results when it runs out" in {
        val partialDataSource = MockPartialDataSource()
        val one = (partialDataSource, worker_query.QueryPlanItem().withGetPartition(worker_query.GetPartition(Some(partialDataSource.protobuf), Seq())))
        val data = Seq(one)
        val testKit = BehaviorTestKit(GetPartitionProducer(data))
        val inbox = TestInbox[GetPartitionConsumer.ConsumerEvent]()
        testKit.run(GetPartitionProducer.RequestWork(inbox.ref))
        inbox.expectMessage(GetPartitionConsumer.HasWork(one._1, one._2))
        testKit.run(GetPartitionProducer.RequestWork(inbox.ref))
        inbox.expectMessage(GetPartitionConsumer.NoWork())
    }
}

class GetPartitionConsumerTest extends UnitSpec with MockitoSugar {
    "A GetPartitionConsumer" should "compute work when it receives it" in {
        val partialDataSource = MockPartialDataSource()
        val mockChannelManager = MockChannelManager()
        val one = (partialDataSource, worker_query.QueryPlanItem().withGetPartition(worker_query.GetPartition(Some(partialDataSource.protobuf), Seq())))
        
        val header = TableResultHeader(Seq(BaseStringField("a")))
        when(mockChannelManager.workerComputeServiceBlockingStub.processQueryPlanItem(one._2)).thenReturn(Seq(table_model.StreamedTableResult().withHeader(header.protobuf)).iterator)

        val inbox = TestInbox[GetPartitionProducer.ProducerEvent]()
        val counterInbox = TestInbox[GetPartitionCounter.CounterEvent]()
        val testKit = BehaviorTestKit(GetPartitionConsumer(mockChannelManager, Seq(inbox.ref), counterInbox.ref))
        
        inbox.expectMessage(GetPartitionProducer.RequestWork(testKit.ref))
        testKit.run(GetPartitionConsumer.HasWork(one._1, one._2))
        counterInbox.expectMessage(GetPartitionCounter.Increment(mockChannelManager, Seq(one._1)))
        inbox.expectMessage(GetPartitionProducer.RequestWork(testKit.ref))
    }

    it should "move to the next producer when the current one is empty" in {
        val partialDataSource = MockPartialDataSource()
        val mockChannelManager = MockChannelManager()
        val one = (partialDataSource, worker_query.QueryPlanItem().withGetPartition(worker_query.GetPartition(Some(partialDataSource.protobuf), Seq())))
        val header = TableResultHeader(Seq(BaseStringField("a")))
        when(mockChannelManager.workerComputeServiceBlockingStub.processQueryPlanItem(one._2)).thenReturn(Seq(table_model.StreamedTableResult().withHeader(header.protobuf)).iterator)

        val inbox = TestInbox[GetPartitionProducer.ProducerEvent]()
        val inboxTwo = TestInbox[GetPartitionProducer.ProducerEvent]()
        val counterInbox = TestInbox[GetPartitionCounter.CounterEvent]()
        val testKit = BehaviorTestKit(GetPartitionConsumer(mockChannelManager, Seq(inbox.ref, inboxTwo.ref), counterInbox.ref))
        
        inbox.expectMessage(GetPartitionProducer.RequestWork(testKit.ref))
        testKit.run(GetPartitionConsumer.NoWork())
        inboxTwo.expectMessage(GetPartitionProducer.RequestWork(testKit.ref))
        testKit.run(GetPartitionConsumer.HasWork(one._1, one._2))
        counterInbox.expectMessage(GetPartitionCounter.Increment(mockChannelManager, Seq(one._1)))
        inboxTwo.expectMessage(GetPartitionProducer.RequestWork(testKit.ref))
    }

    it should "stop when all producers are empty" in {
        val partialDataSource = MockPartialDataSource()
        val mockChannelManager = MockChannelManager()
        val one = (partialDataSource, worker_query.QueryPlanItem().withGetPartition(worker_query.GetPartition(Some(partialDataSource.protobuf), Seq())))
        val header = TableResultHeader(Seq(BaseStringField("a")))
        when(mockChannelManager.workerComputeServiceBlockingStub.processQueryPlanItem(one._2)).thenReturn(Seq(table_model.StreamedTableResult().withHeader(header.protobuf)).iterator)

        val inbox = TestInbox[GetPartitionProducer.ProducerEvent]()
        val inboxTwo = TestInbox[GetPartitionProducer.ProducerEvent]()
        val inboxThree = TestInbox[GetPartitionProducer.ProducerEvent]()
        val counterInbox = TestInbox[GetPartitionCounter.CounterEvent]()
        val testKit = BehaviorTestKit(GetPartitionConsumer(mockChannelManager, Seq(inbox.ref, inboxTwo.ref, inboxThree.ref), counterInbox.ref))
        
        inbox.expectMessage(GetPartitionProducer.RequestWork(testKit.ref))
        testKit.run(GetPartitionConsumer.NoWork())
        inboxTwo.expectMessage(GetPartitionProducer.RequestWork(testKit.ref))
        testKit.run(GetPartitionConsumer.NoWork())
        inboxThree.expectMessage(GetPartitionProducer.RequestWork(testKit.ref))
        testKit.run(GetPartitionConsumer.NoWork())
        testKit.isAlive should be (false)
    }

    it should "provide the partitions it is holding as a message when it completes" in {
        val partialDataSource = MockPartialDataSource()
        val mockChannelManager = MockChannelManager()
        val one = (partialDataSource, worker_query.QueryPlanItem().withGetPartition(worker_query.GetPartition(Some(partialDataSource.protobuf), Seq())))
        
        val header = TableResultHeader(Seq(BaseStringField("a")))
        when(mockChannelManager.workerComputeServiceBlockingStub.processQueryPlanItem(one._2)).thenReturn(Seq(table_model.StreamedTableResult().withHeader(header.protobuf)).iterator)

        val inbox = TestInbox[GetPartitionProducer.ProducerEvent]()
        val counterInbox = TestInbox[GetPartitionCounter.CounterEvent]()
        val testKit = BehaviorTestKit(GetPartitionConsumer(mockChannelManager, Seq(inbox.ref), counterInbox.ref))
        
        inbox.expectMessage(GetPartitionProducer.RequestWork(testKit.ref))
        testKit.run(GetPartitionConsumer.HasWork(one._1, one._2))
        counterInbox.expectMessage(GetPartitionCounter.Increment(mockChannelManager, Seq(one._1)))
        inbox.expectMessage(GetPartitionProducer.RequestWork(testKit.ref))
        testKit.run(GetPartitionConsumer.NoWork())
        counterInbox.expectMessage(GetPartitionCounter.Increment(mockChannelManager, Seq(partialDataSource)))
        testKit.isAlive should be (false)
    }
}

class GetPartitionCounterTest extends AsyncUnitSpec with MockitoSugar {
    "A GetPartitionCounter" should "complete the promise when the expected number of responses is reached" in {
        val promise = Promise[Map[ChannelManager, Seq[PartialDataSource]]]()
        val testKit = BehaviorTestKit(GetPartitionCounter(2, promise))

        val mockChannelManager = MockChannelManager()
        val mockPartitions = Seq(MockPartialDataSource(), MockPartialDataSource())
        testKit.run(GetPartitionCounter.Increment(mockChannelManager, mockPartitions))
        testKit.isAlive should be (false)

        promise.future map { channelMap =>
            channelMap(mockChannelManager) should be (mockPartitions)
        }
    }

    it should "complete the promise when the expected number of responses is reached (and this number is 1)" in {
        val promise = Promise[Map[ChannelManager, Seq[PartialDataSource]]]()
        val testKit = BehaviorTestKit(GetPartitionCounter(1, promise))

        val mockChannelManager = MockChannelManager()
        val mockPartitions = Seq(MockPartialDataSource())
        testKit.run(GetPartitionCounter.Increment(mockChannelManager, mockPartitions))
        testKit.isAlive should be (false)

        promise.future map { channelMap =>
            channelMap(mockChannelManager) should be (mockPartitions)
        }
    }

    it should "wait for more responses if not all responses are received" in {
        val promise = Promise[Map[ChannelManager, Seq[PartialDataSource]]]()
        val testKit = BehaviorTestKit(GetPartitionCounter(2, promise))

        val mockChannelManager = MockChannelManager()
        val mockPartitions = Seq(MockPartialDataSource())
        testKit.run(GetPartitionCounter.Increment(mockChannelManager, mockPartitions))
        testKit.isAlive should be (true)
        val mockChannelManagerTwo = MockChannelManager()
        val mockPartitionsTwo = Seq(MockPartialDataSource())
        testKit.run(GetPartitionCounter.Increment(mockChannelManagerTwo, mockPartitionsTwo))
        testKit.isAlive should be (false)

        promise.future map { channelMap =>
            channelMap(mockChannelManager) should be (mockPartitions)
            channelMap(mockChannelManagerTwo) should be (mockPartitionsTwo)
        }
    }
}