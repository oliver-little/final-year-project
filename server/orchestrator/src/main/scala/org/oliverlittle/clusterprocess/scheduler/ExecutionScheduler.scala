package org.oliverlittle.clusterprocess.scheduler

import org.oliverlittle.clusterprocess.worker_query
import org.oliverlittle.clusterprocess.connector.grpc.{ChannelManager, WorkerHandler}
import org.oliverlittle.clusterprocess.connector.cassandra.token._
import org.oliverlittle.clusterprocess.model.table.{Table, TableResult, LazyTableResult}
import org.oliverlittle.clusterprocess.query._

import akka.actor.typed.scaladsl.{Behaviors, LoggerOps, ActorContext}
import akka.actor.typed.{ActorRef, ActorSystem, Behavior, Terminated}
import akka.actor.typed.DispatcherSelector
import scala.concurrent.ExecutionContext

object WorkExecutionScheduler {
    def startExecution(queryPlan : Seq[QueryPlanItem], workerHandler : WorkerHandler, resultCallback : () => Unit) : Unit = {
        val system = ActorSystem(WorkExecutionScheduler(queryPlan, workerHandler, resultCallback), "WorkExecutionScheduler")
    }

    def apply(
        queryPlan : Seq[QueryPlanItem], 
        workerHandler : WorkerHandler,
        resultCallback : () => Unit,
        producerFactory : WorkProducerFactory = BaseWorkProducerFactory(), 
        consumerFactory : WorkConsumerFactory = BaseWorkConsumerFactory(),
        counterFactory : CounterFactory = BaseCounterFactory()
    ): Behavior[QueryInstruction] = Behaviors.setup{context => new WorkExecutionScheduler(producerFactory, consumerFactory, counterFactory, workerHandler, resultCallback, context).start(queryPlan)}
}

class WorkExecutionScheduler(producerFactory : WorkProducerFactory, consumerFactory : WorkConsumerFactory, counterFactory : CounterFactory, workerHandler : WorkerHandler, resultCallback : () => Unit, context : ActorContext[QueryInstruction]) {
    import WorkExecutionScheduler._

    implicit val executionContext : ExecutionContext = context.system.dispatchers.lookup(DispatcherSelector.fromConfig("cluster-process-dispatcher"))

    def start(queryPlan : Seq[QueryPlanItem]) : Behavior[QueryInstruction] = Behaviors.setup{context =>
        startItemScheduler(queryPlan.head, 0, workerHandler, context)
        receiveComplete(queryPlan.tail, 1)
    }

    def receiveComplete(queryPlan : Seq[QueryPlanItem], instructionCounter : Int) : Behavior[QueryInstruction] = Behaviors.receive { (context, message) =>
        message match {
            case InstructionError(e) => 
                context.log.info(e.toString)
                context.system.terminate()
                Behaviors.stopped
            case InstructionComplete(partitions) if queryPlan.size == 0 => 
                resultCallback()
                context.system.terminate()
                Behaviors.stopped
            case InstructionComplete(partitions) => 
                if partitions.isDefined then startItemScheduler(queryPlan.head.usePartitions(partitions.get), instructionCounter, workerHandler, context) else startItemScheduler(queryPlan.head, instructionCounter, workerHandler, context)
                receiveComplete(queryPlan.tail, instructionCounter + 1)
            
        }
    }

    def startItemScheduler(item : QueryPlanItem, index : Int, workerHandler : WorkerHandler, context : ActorContext[QueryInstruction]) : ActorRef[QueryInstruction] = {
        context.log.info("Starting item: " + item.toString)
        return context.spawn(
            QueryPlanItemScheduler(item, workerHandler, context.self)(using producerFactory)(using consumerFactory)(using counterFactory), 
            "QueryPlanItemScheduler" + index.toString
        )
    }
}