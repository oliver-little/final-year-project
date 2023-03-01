package org.oliverlittle.clusterprocess.scheduler

import org.oliverlittle.clusterprocess.AsyncUnitSpec
import org.oliverlittle.clusterprocess.connector.grpc.{WorkerHandler, ChannelManager}
import org.oliverlittle.clusterprocess.model.table._
import org.oliverlittle.clusterprocess.model.table.field._
import org.oliverlittle.clusterprocess.query._
import org.oliverlittle.clusterprocess.worker_query
import org.oliverlittle.clusterprocess.table_model

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar
import akka.actor.testkit.typed.scaladsl.BehaviorTestKit
import akka.actor.testkit.typed.scaladsl.TestInbox
import akka.actor.typed._
import akka.actor.typed.scaladsl._
import akka.actor.testkit.typed.Effect

import scala.concurrent.{Promise, Await}
import scala.concurrent.duration.Duration

// Required for Promises
given scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

// Testing reference: https://doc.akka.io/docs/akka/current/typed/testing-sync.html

class WorkExecutionSchedulerTest extends AsyncUnitSpec with MockitoSugar {
	"A WorkExecutionScheduler" should "create a QueryPlanItemScheduler when it starts" in {
		val items = Seq(IdentityQueryPlanItem())
		val mockWorkerHandler = mock[WorkerHandler]
		val promise = Promise[Option[Map[ChannelManager, Seq[PartialTable]]]]() 
		val testKit = BehaviorTestKit(WorkExecutionScheduler(items, mockWorkerHandler, promise))
		val effect = testKit.retrieveEffect()
		effect shouldBe a [Effect.Spawned[QueryInstruction]]
		val spawnedEffect = effect.asInstanceOf[Effect.Spawned[QueryInstruction]]
		spawnedEffect.behavior shouldBe a [Behavior[QueryInstruction]]
		spawnedEffect.childName should be ("QueryPlanItemScheduler0")
	}

	it should "spawn the next item when the previous is completed if there is one" in {
		val items = Seq(IdentityQueryPlanItem(), IdentityQueryPlanItem(), IdentityQueryPlanItem(), IdentityQueryPlanItem())
		val mockWorkerHandler = mock[WorkerHandler]
		val promise = Promise[Option[Map[ChannelManager, Seq[PartialTable]]]]() 
		val testKit = BehaviorTestKit(WorkExecutionScheduler(items, mockWorkerHandler, promise))
		testKit.retrieveEffect() // Initialisation effect
		testKit.run(InstructionComplete())
		assertSpawned(testKit.retrieveEffect(), "QueryPlanItemScheduler1")
		testKit.run(InstructionCompleteWithTableOutput(Map()))
		assertSpawned(testKit.retrieveEffect(), "QueryPlanItemScheduler2")
		testKit.run(InstructionCompleteWithDataSourceOutput(Map()))
		assertSpawned(testKit.retrieveEffect(), "QueryPlanItemScheduler3") should be (true)

	}

	it should "complete the promise with None when the last instruction is completed" in {
		val items = Seq(IdentityQueryPlanItem(), IdentityQueryPlanItem())
		val mockWorkerHandler = mock[WorkerHandler]
		val promise = Promise[Option[Map[ChannelManager, Seq[PartialTable]]]]() 
		val testKit = BehaviorTestKit(WorkExecutionScheduler(items, mockWorkerHandler, promise))
		testKit.retrieveEffect() // Initialisation effect
		testKit.run(InstructionComplete())
		assertSpawned(testKit.retrieveEffect(), "QueryPlanItemScheduler1")
		promise.isCompleted should be (false)
		testKit.run(InstructionComplete())
		promise.future map {res =>
			res should be (None)
		}		
	}

	it should "complete the promise with None when the last instruction is completed with data source partitions" in {
		val items = Seq(IdentityQueryPlanItem(), IdentityQueryPlanItem())
		val mockWorkerHandler = mock[WorkerHandler]
		val promise = Promise[Option[Map[ChannelManager, Seq[PartialTable]]]]() 
		val testKit = BehaviorTestKit(WorkExecutionScheduler(items, mockWorkerHandler, promise))
		testKit.retrieveEffect() // Initialisation effect
		testKit.run(InstructionComplete())
		assertSpawned(testKit.retrieveEffect(), "QueryPlanItemScheduler1")
		promise.isCompleted should be (false)
		testKit.run(InstructionCompleteWithDataSourceOutput(Map()))
		promise.future map {res =>
			res should be (None)
		}		
	}

	it should "complete the promise with Some(partitions) when the last instruction is completed with Table partitions" in {
		val items = Seq(IdentityQueryPlanItem(), IdentityQueryPlanItem())
		val mockWorkerHandler = mock[WorkerHandler]
		val promise = Promise[Option[Map[ChannelManager, Seq[PartialTable]]]]() 
		val testKit = BehaviorTestKit(WorkExecutionScheduler(items, mockWorkerHandler, promise))
		testKit.retrieveEffect() // Initialisation effect
		testKit.run(InstructionComplete())
		assertSpawned(testKit.retrieveEffect(), "QueryPlanItemScheduler1")
		promise.isCompleted should be (false)
		testKit.run(InstructionCompleteWithTableOutput(Map()))
		promise.future map {res =>
			res should be (Some(Map()))
		}
	}

	it should "fail the promise if an error occurs" in {
		val items = Seq(IdentityQueryPlanItem(), IdentityQueryPlanItem())
		val mockWorkerHandler = mock[WorkerHandler]
		val promise = Promise[Option[Map[ChannelManager, Seq[PartialTable]]]]() 
		val testKit = BehaviorTestKit(WorkExecutionScheduler(items, mockWorkerHandler, promise))
		testKit.retrieveEffect() // Initialisation effect
		val error = new IllegalStateException("Test exception")
		testKit.run(InstructionError(error))
		
		recoverToSucceededIf[IllegalStateException] {
			promise.future
		}
	}

	private def assertSpawned(effect : Effect, name : String) : Boolean = {
		effect shouldBe a [Effect.Spawned[QueryInstruction]]
		val spawnedEffect = effect.asInstanceOf[Effect.Spawned[QueryInstruction]]
		spawnedEffect.behavior shouldBe a [Behavior[QueryInstruction]]
		spawnedEffect.childName should be (name)
		return true
	}
}