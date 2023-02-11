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
import akka.actor.testkit.typed.scaladsl.BehaviorTestKit
import akka.actor.testkit.typed.scaladsl.TestInbox
import akka.actor.typed._
import akka.actor.typed.scaladsl._
import akka.actor.testkit.typed.Effect.Spawned

import scala.concurrent.{Promise, Await}
import scala.concurrent.duration.Duration

// Required for Promises
implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

// Testing reference: https://doc.akka.io/docs/akka/current/typed/testing-sync.html

/**
* This test tests the assembler function of WorkExecutionScheduler
*/
class WorkExecutionSchedulerTest extends UnitSpec with MockitoSugar {
	class TestWorkConsumerFactory(mocks : Iterator[Behavior[WorkConsumer.ConsumerEvent]]) extends WorkConsumerFactory:
		def createConsumer(stub: worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceBlockingStub, producers: Seq[ActorRef[WorkProducer.ProducerEvent]], assembler: ActorRef[WorkExecutionScheduler.AssemblerEvent]) : Behavior[WorkConsumer.ConsumerEvent] = mocks.next
	
	class TestWorkProducerFactory(mocks : Iterator[Behavior[WorkProducer.ProducerEvent]]) extends WorkProducerFactory:
		def createProducer(items : Seq[worker_query.ComputePartialResultCassandraRequest]) : Behavior[WorkProducer.ProducerEvent] = mocks.next
	
	"A WorkExecutionScheduler" should "create the correct number of producers and consumers based on the input data" in {
		val one = worker_query.ComputePartialResultCassandraRequest(dataSource=Some(data_source.CassandraDataSource("test", "one")))
		val two = worker_query.ComputePartialResultCassandraRequest(dataSource=Some(data_source.CassandraDataSource("test", "two")))
		val mockConsumer = Behaviors.receiveMessage[WorkConsumer.ConsumerEvent] { _ =>
			Behaviors.same[WorkConsumer.ConsumerEvent]
		}
		val mockProducer = Behaviors.receiveMessage[WorkProducer.ProducerEvent] { _ =>
			Behaviors.same[WorkProducer.ProducerEvent]
		}
		val items = Seq(
		(Seq(mock[worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceBlockingStub], mock[worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceBlockingStub]), Seq(one)),
		(Seq(mock[worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceBlockingStub]), Seq(two))
		)
		
		val testKit = BehaviorTestKit(WorkExecutionScheduler(items, (l) => l.reduce(_ ++ _), (_) => {}, TestWorkProducerFactory(Seq(mockProducer, mockProducer).iterator), TestWorkConsumerFactory(Seq(mockConsumer, mockConsumer, mockConsumer).iterator)))
		
		testKit.expectEffect(Spawned(mockProducer, "producer0"))
		testKit.expectEffect(Spawned(mockProducer, "producer1"))
		testKit.expectEffect(Spawned(mockConsumer, "consumer00"))
		testKit.expectEffect(Spawned(mockConsumer, "consumer01"))
		testKit.expectEffect(Spawned(mockConsumer, "consumer10"))
	}
	it should "call the callback when the expected number of responses is reached" in {
		val one = worker_query.ComputePartialResultCassandraRequest(dataSource=Some(data_source.CassandraDataSource("test", "one")))
		val two = worker_query.ComputePartialResultCassandraRequest(dataSource=Some(data_source.CassandraDataSource("test", "two")))
		
		val mockConsumer = Behaviors.receiveMessage[WorkConsumer.ConsumerEvent] { _ =>
			Behaviors.same[WorkConsumer.ConsumerEvent]
		}
		val mockProducer = Behaviors.receiveMessage[WorkProducer.ProducerEvent] { _ =>
			Behaviors.same[WorkProducer.ProducerEvent]
		}
		val items = Seq(
		(Seq(mock[worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceBlockingStub], mock[worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceBlockingStub]), Seq(one)),
		(Seq(mock[worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceBlockingStub]), Seq(two))
		)
		
		val p = Promise[Boolean]()
		val resultCallback : TableResult => Unit = {r => 
			p.success(true)
			
		}
		
		val testKit = BehaviorTestKit(WorkExecutionScheduler(items, (l) => l.reduce(_ ++ _), resultCallback, TestWorkProducerFactory(Seq(mockProducer, mockProducer).iterator), TestWorkConsumerFactory(Seq(mockConsumer, mockConsumer, mockConsumer).iterator)))
		testKit.run(WorkExecutionScheduler.SendResult(LazyTableResult(TableResultHeader(Seq(BaseIntField("name"))), Seq(Seq()))))
		testKit.run(WorkExecutionScheduler.SendResult(LazyTableResult(TableResultHeader(Seq(BaseIntField("name"))), Seq(Seq()))))
		Await.result(p.future, Duration(100, "millis")) should be (true)
	}
	it should "call the callback when the expected number of responses is reached (and this number is 1)" in {
		val one = worker_query.ComputePartialResultCassandraRequest(dataSource=Some(data_source.CassandraDataSource("test", "one")))
		val mockConsumer = Behaviors.receiveMessage[WorkConsumer.ConsumerEvent] { _ =>
			Behaviors.same[WorkConsumer.ConsumerEvent]
		}
		val mockProducer = Behaviors.receiveMessage[WorkProducer.ProducerEvent] { _ =>
			Behaviors.same[WorkProducer.ProducerEvent]
		}
		val items = Seq(
		(Seq(mock[worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceBlockingStub], mock[worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceBlockingStub]), Seq(one))
		)
		
		val p = Promise[Boolean]()
		val resultCallback : TableResult => Unit = {r => 
			p.success(true)
		}
		
		val testKit = BehaviorTestKit(WorkExecutionScheduler(items, (l) => l.reduce(_ ++ _), resultCallback, TestWorkProducerFactory(Seq(mockProducer, mockProducer).iterator), TestWorkConsumerFactory(Seq(mockConsumer, mockConsumer, mockConsumer).iterator)))
		testKit.run(WorkExecutionScheduler.SendResult(LazyTableResult(TableResultHeader(Seq(BaseIntField("name"))), Seq(Seq()))))
		Await.result(p.future, Duration(100, "millis")) should be (true)
	}
}