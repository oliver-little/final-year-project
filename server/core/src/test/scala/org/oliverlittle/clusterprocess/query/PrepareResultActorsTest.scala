package org.oliverlittle.clusterprocess.query

import org.oliverlittle.clusterprocess.worker_query
import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.{AsyncUnitSpec, UnitSpec}
import org.oliverlittle.clusterprocess.model.table._
import org.oliverlittle.clusterprocess.model.table.field._
import org.oliverlittle.clusterprocess.model.table.sources._
import org.oliverlittle.clusterprocess.connector.grpc.{ChannelManager, MockitoChannelManager}

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar
import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.testkit.typed.scaladsl.TestInbox
import akka.actor.typed._
import akka.actor.typed.scaladsl._
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.{Promise, Future}

class PrepareResultConsumerTest extends AsyncUnitSpec with MockitoSugar with BeforeAndAfterAll {
    // TestKit setup and teardown
    val testKit : ActorTestKit = ActorTestKit()

    override def beforeAll() : Unit = {
        super.beforeAll()
    }    

    override def afterAll() : Unit = {
        super.afterAll()
        testKit.shutdownTestKit()
    }

    "A PrepareResultConsumer" should "repeatedly compute work until it completes it all" in {
        val partialTable = PartialTable(MockPartialDataSource(), Seq())
        val mockChannelManager = MockitoChannelManager()
        val one = (partialTable, worker_query.QueryPlanItem().withPrepareResult(worker_query.PrepareResult(Some(partialTable.protobuf))))
        val two = (partialTable, worker_query.QueryPlanItem().withPrepareResult(worker_query.PrepareResult(Some(partialTable.protobuf))))
        
        when(mockChannelManager.workerComputeServiceStub.processQueryPlanItem(any())).thenReturn(Future.successful(worker_query.ProcessQueryPlanItemResult(true)))

        val promise = Promise[Map[ChannelManager, Seq[PartialTable]]]()
        val probe = testKit.createTestProbe[PrepareResultCounter.CounterEvent]()
        val consumer = testKit.spawn(PrepareResultConsumer(mockChannelManager, Seq(one, two), probe.ref))
        
        probe.receiveMessage() should be (PrepareResultCounter.Increment(mockChannelManager, Seq(one._1, two._1)))
    }

    it should "send an error message if an error occurs at any point" in {
        val partialTable = PartialTable(MockPartialDataSource(), Seq())
        val mockChannelManager = MockitoChannelManager()
        val one = (partialTable, worker_query.QueryPlanItem().withPrepareResult(worker_query.PrepareResult(Some(partialTable.protobuf))))
        val two = (partialTable, worker_query.QueryPlanItem().withPrepareResult(worker_query.PrepareResult(Some(partialTable.protobuf))))
        
        when(mockChannelManager.workerComputeServiceStub.processQueryPlanItem(any())).thenReturn(Future.failed(new IllegalStateException("Test exception")))

        val promise = Promise[Map[ChannelManager, Seq[PartialTable]]]()
        val probe = testKit.createTestProbe[PrepareResultCounter.CounterEvent]()
        val consumer = testKit.spawn(PrepareResultConsumer(mockChannelManager, Seq(one, two), probe.ref))
        
        val msg = probe.receiveMessage() 
        msg shouldBe a [PrepareResultCounter.Error]
        msg.asInstanceOf[PrepareResultCounter.Error].e shouldBe a [IllegalStateException]
    }
}

class PrepareResultCounterTest extends UnitSpec with MockitoSugar {
    "A PrepareResultCounter" should "complete the promise when the expected number of responses is reached" in {
        fail()
    }

    it should "complete the promise when the expected number of responses is reached (and this number is 1)" in {
        fail()
    }
}