package org.oliverlittle.clusterprocess.scheduler

import org.oliverlittle.clusterprocess.worker_query
import org.oliverlittle.clusterprocess.connector.grpc.{ChannelManager, WorkerHandler}
import org.oliverlittle.clusterprocess.connector.cassandra.token._
import org.oliverlittle.clusterprocess.model.table.{Table, TableResult, LazyTableResult, PartialTable}
import org.oliverlittle.clusterprocess.query._

import akka.actor.typed.scaladsl.{Behaviors, LoggerOps, ActorContext}
import akka.actor.typed.{ActorRef, ActorSystem, Behavior, Terminated}
import akka.actor.typed.DispatcherSelector
import scala.concurrent.{Future, Promise, ExecutionContext}

object WorkExecutionScheduler {
    def startExecution(queryPlan : Seq[QueryPlanItem], workerHandler : WorkerHandler) : Future[Option[Map[ChannelManager, Seq[PartialTable]]]] = {
        val promise = Promise[Option[Map[ChannelManager, Seq[PartialTable]]]]()
        val system = ActorSystem(WorkExecutionScheduler(queryPlan, workerHandler, promise), "WorkExecutionScheduler")
        return promise.future
    }

    def apply(
        queryPlan : Seq[QueryPlanItem], 
        workerHandler : WorkerHandler,
        promise : Promise[Option[Map[ChannelManager, Seq[PartialTable]]]]
    ): Behavior[QueryInstruction] = Behaviors.setup{context => new WorkExecutionScheduler(workerHandler, promise, context).start(queryPlan)}
}

class WorkExecutionScheduler(workerHandler : WorkerHandler, promise : Promise[Option[Map[ChannelManager, Seq[PartialTable]]]], context : ActorContext[QueryInstruction]) {
    import WorkExecutionScheduler._

    implicit val executionContext : ExecutionContext = context.system.dispatchers.lookup(DispatcherSelector.sameAsParent())

    def start(queryPlan : Seq[QueryPlanItem]) : Behavior[QueryInstruction] = Behaviors.setup{context =>
        context.log.info("Starting execution")
        startItemScheduler(queryPlan.head, 0, workerHandler, context.self)
        receiveComplete(queryPlan.tail, 1)
    }

    def receiveComplete(queryPlan : Seq[QueryPlanItem], instructionCounter : Int) : Behavior[QueryInstruction] = Behaviors.receive { (context, message) =>
        message match {
            case InstructionError(e) => 
                promise.failure(e)
                context.system.terminate()
                Behaviors.stopped

            case InstructionComplete() if queryPlan.size == 0 => 
                promise.success(None)
                context.system.terminate()
                Behaviors.stopped
            case InstructionCompleteWithDataSourceOutput(partitions) if queryPlan.size == 0 =>
                promise.success(None)
                context.system.terminate()
                Behaviors.stopped
            case InstructionCompleteWithTableOutput(partitions) if queryPlan.size == 0 =>
                promise.success(Some(partitions))
                context.system.terminate()
                Behaviors.stopped
            
            case InstructionComplete() => 
                startItemScheduler(queryPlan.head, instructionCounter, workerHandler, context.self)
                receiveComplete(queryPlan.tail, instructionCounter + 1)
            case InstructionCompleteWithDataSourceOutput(partitions) =>
                startItemScheduler(queryPlan.head.usePartitions(partitions), instructionCounter, workerHandler, context.self)
                receiveComplete(queryPlan.tail, instructionCounter + 1)
            case InstructionCompleteWithTableOutput(partitions) =>
                startItemScheduler(queryPlan.head, instructionCounter, workerHandler, context.self)
                receiveComplete(queryPlan.tail, instructionCounter + 1)
        }
    }

    def startItemScheduler(item : QueryPlanItem, index : Int, workerHandler : WorkerHandler, replyTo : ActorRef[QueryInstruction]) : ActorRef[QueryInstruction] = context.spawn(
        QueryPlanItemScheduler(item, workerHandler, replyTo), 
        "QueryPlanItemScheduler" + index.toString
    )
}