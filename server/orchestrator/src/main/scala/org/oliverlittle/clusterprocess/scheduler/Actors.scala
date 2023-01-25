package org.oliverlittle.clusterprocess.scheduler

import org.oliverlittle.clusterprocess.worker_query

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.LoggerOps
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}

object WorkProducer {
    sealed trait ProducerEvent
    final case class RequestWork(replyTo : ActorRef[WorkConsumer.ConsumerEvent]) extends ProducerEvent
    
    def apply(items : Seq[worker_query.ComputePartialResultCassandraRequest]) : Behavior[ProducerEvent] = list(items)

    private def list(items : Seq[worker_query.ComputePartialResultCassandraRequest]) : Behavior[ProducerEvent] = Behaviors.receiveMessage{
        case RequestWork(replyTo) =>
            items.isEmpty match {
                case true => 
                    replyTo ! WorkConsumer.NoWork()
                    Behaviors.same
                case false => 
                    replyTo ! WorkConsumer.HasWork(items.head)
                    list(items.tail)
            }
    }
}

object WorkConsumer {
    sealed trait ConsumerEvent
    final case class HasWork(request : worker_query.ComputePartialResultCassandraRequest) extends ConsumerEvent
    final case class NoWork() extends ConsumerEvent

    def apply(stub : worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceBlockingStub, producers : Seq[ActorRef[WorkProducer.ProducerEvent]]) : Behavior[ConsumerEvent] = Behaviors.setup{context => 
        producers.head ! WorkProducer.RequestWork(context.self)
        computeWork(stub, producers)
    }

    private def computeWork(stub : worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceBlockingStub, producers : Seq[ActorRef[WorkProducer.ProducerEvent]]) : Behavior[ConsumerEvent] = Behaviors.receive{(context, message) => 
        message match {
            case NoWork() if producers.length == 0 => Behaviors.stopped
            case NoWork() => computeWork(stub, producers.tail)
            case HasWork(request) => 
                stub.computePartialResultCassandra(request)
                producers.head ! WorkProducer.RequestWork(context.self)
                Behaviors.same
        }
    }
}