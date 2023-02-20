package org.oliverlittle.clusterprocess.query

import org.oliverlittle.clusterprocess.connector.grpc.WorkerHandler

import akka.actor.typed.scaladsl.{Behaviors, LoggerOps}
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import scala.concurrent.ExecutionContext

// Container actor to ensure clean shutdown when this item is completed
object QueryPlanItemScheduler:
    def apply(item : QueryPlanItem, workerHandler : WorkerHandler, onResult : ActorRef[QueryInstruction])(using producerFactory : WorkProducerFactory)(using consumerFactory : WorkConsumerFactory)(using counterFactory : CounterFactory)(using ec : ExecutionContext)  : Behavior[QueryInstruction] = Behaviors.setup{context => 
        context.log.info("Starting item: " + item.toString)
        item.execute(workerHandler, context.self)(using context)
        context.log.info("Finished setup, waiting for complete confirmation")
        completed(onResult)
    }

    // Intercept the query instruction before forwarding it to stop everything we created in this Scheduler
    def completed(onResult : ActorRef[QueryInstruction]) : Behavior[QueryInstruction] = Behaviors.receive{(context, message) => 
        onResult ! message
        Behaviors.stopped
    }