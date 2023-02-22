package org.oliverlittle.clusterprocess.query

import org.oliverlittle.clusterprocess.worker_query
import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.model.table._
import org.oliverlittle.clusterprocess.model.table.field._
import org.oliverlittle.clusterprocess.connector.grpc.{ChannelManager}

import akka.actor.typed.scaladsl.{Behaviors, LoggerOps, ActorContext}
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}

import collection.mutable.{Buffer, ArrayBuffer}
import scala.concurrent.Promise

trait WorkProducerFactory:
    def createProducer(items : Seq[(PartitionElement, worker_query.QueryPlanItem)]) : Behavior[WorkProducer.ProducerEvent] 
        
class BaseWorkProducerFactory extends WorkProducerFactory:
    def createProducer(items : Seq[(PartitionElement, worker_query.QueryPlanItem)]) : Behavior[WorkProducer.ProducerEvent] = WorkProducer(items)

object WorkProducer {
    sealed trait ProducerEvent
    final case class RequestWork(replyTo : ActorRef[WorkConsumer.ConsumerEvent]) extends ProducerEvent
    
    def apply(items : Seq[(PartitionElement, worker_query.QueryPlanItem)]) : Behavior[ProducerEvent] = list(items)

    private def list(items : Seq[(PartitionElement, worker_query.QueryPlanItem)]) : Behavior[ProducerEvent] = Behaviors.receiveMessage {
        case RequestWork(replyTo) =>
            items.isEmpty match {
                case true => 
                    replyTo ! WorkConsumer.NoWork()
                    Behaviors.same
                case false => 
                    val (partition, request) = items.head
                    replyTo ! WorkConsumer.HasWork(partition, request)
                    list(items.tail)
            }
    }
}

trait WorkConsumerFactory:
    def createConsumer(channel : ChannelManager, producers : Seq[ActorRef[WorkProducer.ProducerEvent]], counter : ActorRef[Counter.CounterEvent]) : Behavior[WorkConsumer.ConsumerEvent]

class BaseWorkConsumerFactory extends WorkConsumerFactory:
    def createConsumer(channel : ChannelManager, producers : Seq[ActorRef[WorkProducer.ProducerEvent]], counter : ActorRef[Counter.CounterEvent]) : Behavior[WorkConsumer.ConsumerEvent] = WorkConsumer(channel, producers, counter)


object WorkConsumer:
    sealed trait ConsumerEvent
    final case class HasWork(partition : PartitionElement, request : worker_query.QueryPlanItem) extends ConsumerEvent
    final case class NoWork() extends ConsumerEvent

    def apply(channel : ChannelManager, producers : Seq[ActorRef[WorkProducer.ProducerEvent]], counter : ActorRef[Counter.CounterEvent]) : Behavior[ConsumerEvent] = Behaviors.setup{context => 
        producers.head ! WorkProducer.RequestWork(context.self)
        new WorkConsumer(channel, channel.workerComputeServiceBlockingStub, counter).computeWork(Seq(), producers)
    }

class WorkConsumer private (channel : ChannelManager, stub : worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceBlockingStub, counter : ActorRef[Counter.CounterEvent]) {
    import WorkConsumer._

    private def computeWork(partitions : Seq[PartitionElement], producers : Seq[ActorRef[WorkProducer.ProducerEvent]]) : Behavior[ConsumerEvent] = Behaviors.receive{(context, message) => 
        message match {
            case NoWork() if producers.length == 1 => 
                counter ! Counter.Increment(channel, partitions)
                Behaviors.stopped
            case NoWork() => 
                producers.tail.head ! WorkProducer.RequestWork(context.self)
                computeWork(partitions, producers.tail)
            case HasWork(partition, request) => 
                // Make the request to the worker node
                val result = stub.processQueryPlanItem(request)
                // Request more work from the producer
                producers.head ! WorkProducer.RequestWork(context.self)
                computeWork(partition +: partitions, producers)
        }
    }
}

trait CounterFactory:
    def createCounter(expectedResponses : Int, promise : Promise[Map[ChannelManager, Seq[PartitionElement]]]) : Behavior[Counter.CounterEvent]

class BaseCounterFactory extends CounterFactory:
    def createCounter(expectedResponses : Int, promise : Promise[Map[ChannelManager, Seq[PartitionElement]]]) : Behavior[Counter.CounterEvent] = Counter(expectedResponses, promise)

object Counter:
    sealed trait CounterEvent
    case class Increment(channel : ChannelManager, partition : Seq[PartitionElement]) extends CounterEvent

    def apply(expectedResponses : Int, promise : Promise[Map[ChannelManager, Seq[PartitionElement]]]) : Behavior[CounterEvent] = Behaviors.setup{context => 
        new Counter(expectedResponses, promise, context).start()
    }

class Counter private (expectedResponses : Int, promise : Promise[Map[ChannelManager, Seq[PartitionElement]]], context : ActorContext[Counter.CounterEvent]) {
    import Counter._

    /**
     * Handles receiving the first response from any consumer
     */
    private def start() : Behavior[Counter.CounterEvent] = Behaviors.receive {(context, message) =>
        message match {
            case Increment(channel, partitions) => 
                if expectedResponses == partitions.size then 
                    promise.success(Map(channel -> partitions))
                    Behaviors.stopped
                else getResponses(Map(channel -> partitions))
        }
    }

    /**
     *  Handles receiving all subsequent responses from consumers until expectedResponses is reached
     */
    private def getResponses(mapping : Map[ChannelManager, Seq[PartitionElement]]) : Behavior[Counter.CounterEvent] = Behaviors.receive { (context, message) =>
        message match {
            case Increment(channel, partitions) => 
                val newMap = mapping + (channel -> (mapping.getOrElse(channel, Seq()) ++ partitions))
                if newMap.values.map(_.size).sum == expectedResponses then
                    promise.success(newMap)
                    Behaviors.stopped
                else
                    getResponses(newMap)
        }
    }
}