package org.oliverlittle.clusterprocess.model.table

import org.oliverlittle.clusterprocess.model.table.TableStore
import org.oliverlittle.clusterprocess.connector.grpc.DelayedTableResultRunnable

import akka.actor.typed.scaladsl.{Behaviors, LoggerOps}
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}

import scala.util.{Success, Failure}

object TableStoreSystem:
    sealed trait TableStoreSystemEvent
    case class GetStore(replyTo : ActorRef[ActorRef[TableStore.TableStoreEvent]]) extends TableStoreSystemEvent

    def create() : ActorSystem[TableStoreSystemEvent] = ActorSystem(TableStoreSystem(), "root")

    def apply() : Behavior[TableStoreSystemEvent] = Behaviors.setup{context =>   
        val tableStore = context.spawn(TableStore(), "tableStore")
        handleMessage(tableStore)
    }

    def handleMessage(store : ActorRef[TableStore.TableStoreEvent]) : Behavior[TableStoreSystemEvent] = Behaviors.receiveMessage{
        case GetStore(replyTo) => 
            replyTo ! store
            Behaviors.same
    }

object DelayedRunnableActor:
    sealed trait DelayedRunnableActorEvent
    case class SetResult(result : TableResult) extends DelayedRunnableActorEvent
    case class Error(e : Throwable) extends DelayedRunnableActorEvent
    case class Run() extends DelayedRunnableActorEvent
    case class Stop() extends DelayedRunnableActorEvent
    def apply(delayedRunnable : DelayedTableResultRunnable) : Behavior[DelayedRunnableActorEvent] = Behaviors.setup{ context =>
        delayedRunnable.future.onComplete {
            case Success(v) => context.self ! Stop()
            case Failure(e) => 
        }(using context.executionContext)

        setData(delayedRunnable)
    }

    def setData(delayedRunnable : DelayedTableResultRunnable) : Behavior[DelayedRunnableActorEvent] = Behaviors.receive {(context, message) =>
        message match {
            case SetResult(result) => 
                delayedRunnable.setData(result)
                handleMessage(delayedRunnable)
            case Error(e) => 
                delayedRunnable.setError(e)
                Behaviors.stopped
            case Run() => Behaviors.same
            case Stop() => 
                context.log.info("Stopping delayedRunnable actor.")
                Behaviors.stopped
        }
    }

    def handleMessage(delayedRunnable : DelayedTableResultRunnable) : Behavior[DelayedRunnableActorEvent] = Behaviors.receive {(context, message) => 
        message match {
            case SetResult(result) => throw new IllegalArgumentException("SetResult received twice.")
            case Run() => 
                delayedRunnable.run()
                Behaviors.same
            case Stop() => 
                context.log.info("Stopping delayedRunnable actor.")
                Behaviors.stopped
            case Error(e) => 
                delayedRunnable.setError(e)
                Behaviors.stopped
        }
    }

class DelayedRunnableActorCaller(actor : ActorRef[DelayedRunnableActor.DelayedRunnableActorEvent]) extends Runnable:
    override def run(): Unit = {
        actor ! DelayedRunnableActor.Run()
    }