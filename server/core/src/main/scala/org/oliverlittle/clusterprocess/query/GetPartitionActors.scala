package org.oliverlittle.clusterprocess.query

import org.oliverlittle.clusterprocess.worker_query
import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.model.table._
import org.oliverlittle.clusterprocess.model.table.sources.PartialDataSource
import org.oliverlittle.clusterprocess.model.table.field._
import org.oliverlittle.clusterprocess.connector.grpc.{ChannelManager}

import akka.actor.typed.scaladsl.{Behaviors, LoggerOps, ActorContext}
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}

import collection.mutable.{Buffer, ArrayBuffer}
import scala.concurrent.Promise
import scala.util.{Try, Success, Failure}

trait GetPartitionProducerFactory:
    def createProducer(items : Seq[(PartialDataSource, worker_query.QueryPlanItem)]) : Behavior[GetPartitionProducer.ProducerEvent] 
        
class BaseGetPartitionProducerFactory extends GetPartitionProducerFactory:
    def createProducer(items : Seq[(PartialDataSource, worker_query.QueryPlanItem)]) : Behavior[GetPartitionProducer.ProducerEvent] = GetPartitionProducer(items)

object GetPartitionProducer {
    sealed trait ProducerEvent
    final case class RequestWork(replyTo : ActorRef[GetPartitionConsumer.ConsumerEvent]) extends ProducerEvent
    
    def apply(items : Seq[(PartialDataSource, worker_query.QueryPlanItem)]) : Behavior[ProducerEvent] = list(items)

    private def list(items : Seq[(PartialDataSource, worker_query.QueryPlanItem)]) : Behavior[ProducerEvent] = Behaviors.receiveMessage {
        case RequestWork(replyTo) =>
            items.isEmpty match {
                case true => 
                    replyTo ! GetPartitionConsumer.NoWork()
                    Behaviors.same
                case false => 
                    val (partition, request) = items.head
                    replyTo ! GetPartitionConsumer.HasWork(partition, request)
                    list(items.tail)
            }
    }
}

trait GetPartitionConsumerFactory:
    def createConsumer(channel : ChannelManager, producers : Seq[ActorRef[GetPartitionProducer.ProducerEvent]], counter : ActorRef[GetPartitionCounter.CounterEvent]) : Behavior[GetPartitionConsumer.ConsumerEvent]

class BaseGetPartitionConsumerFactory extends GetPartitionConsumerFactory:
    def createConsumer(channel : ChannelManager, producers : Seq[ActorRef[GetPartitionProducer.ProducerEvent]], counter : ActorRef[GetPartitionCounter.CounterEvent]) : Behavior[GetPartitionConsumer.ConsumerEvent] = GetPartitionConsumer(channel, producers, counter)


object GetPartitionConsumer:
    sealed trait ConsumerEvent
    final case class HasWork(partition : PartialDataSource, request : worker_query.QueryPlanItem) extends ConsumerEvent
    final case class NoWork() extends ConsumerEvent

    def apply(channel : ChannelManager, producers : Seq[ActorRef[GetPartitionProducer.ProducerEvent]], counter : ActorRef[GetPartitionCounter.CounterEvent]) : Behavior[ConsumerEvent] = Behaviors.setup{context => 
        producers.head ! GetPartitionProducer.RequestWork(context.self)
        new GetPartitionConsumer(channel, channel.workerComputeServiceBlockingStub, counter).computeWork(Seq(), producers)
    }

class GetPartitionConsumer private (channel : ChannelManager, stub : worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceBlockingStub, counter : ActorRef[GetPartitionCounter.CounterEvent]) {
    import GetPartitionConsumer._

    private def computeWork(partitions : Seq[PartialDataSource], producers : Seq[ActorRef[GetPartitionProducer.ProducerEvent]]) : Behavior[ConsumerEvent] = Behaviors.receive{(context, message) => 
        message match {
            case NoWork() if producers.length == 1 => 
                counter ! GetPartitionCounter.Increment(channel, partitions)
                Behaviors.stopped
            case NoWork() => 
                producers.tail.head ! GetPartitionProducer.RequestWork(context.self)
                computeWork(partitions, producers.tail)
            case HasWork(partition, request) => 
                // Make the request to the worker node
                Try{stub.processQueryPlanItem(request)} match {
                    case Success(_) =>
                        // Request more work from the producer
                        producers.head ! GetPartitionProducer.RequestWork(context.self)
                        computeWork(partition +: partitions, producers)
                    case Failure(e) =>
                        counter ! GetPartitionCounter.Error(e)
                        Behaviors.stopped
                }
                
        }
    }
}

trait GetPartitionCounterFactory:
    def createCounter(expectedResponses : Int, promise : Promise[Map[ChannelManager, Seq[PartialDataSource]]]) : Behavior[GetPartitionCounter.CounterEvent]

class BaseGetPartitionCounterFactory extends GetPartitionCounterFactory:
    def createCounter(expectedResponses : Int, promise : Promise[Map[ChannelManager, Seq[PartialDataSource]]]) : Behavior[GetPartitionCounter.CounterEvent] = GetPartitionCounter(expectedResponses, promise)

object GetPartitionCounter:
    sealed trait CounterEvent
    case class Increment(channel : ChannelManager, partition : Seq[PartialDataSource]) extends CounterEvent
    case class Error(e : Throwable) extends CounterEvent

    def apply(expectedResponses : Int, promise : Promise[Map[ChannelManager, Seq[PartialDataSource]]]) : Behavior[CounterEvent] = Behaviors.setup{context => 
        new GetPartitionCounter(expectedResponses, promise, context).start()
    }

class GetPartitionCounter private (expectedResponses : Int, promise : Promise[Map[ChannelManager, Seq[PartialDataSource]]], context : ActorContext[GetPartitionCounter.CounterEvent]) {
    import GetPartitionCounter._

    /**
     * Handles receiving the first response from any consumer
     */
    private def start() : Behavior[GetPartitionCounter.CounterEvent] = Behaviors.receive {(context, message) =>
        message match {
            case Increment(channel, partitions) => 
                if expectedResponses == partitions.size then 
                    promise.success(Map(channel -> partitions))
                    Behaviors.stopped
                else getResponses(Map(channel -> partitions))
            case Error(e) => 
                promise.failure(e)
                Behaviors.stopped
        }
    }

    /**
     *  Handles receiving all subsequent responses from consumers until expectedResponses is reached
     */
    private def getResponses(mapping : Map[ChannelManager, Seq[PartialDataSource]]) : Behavior[GetPartitionCounter.CounterEvent] = Behaviors.receive { (context, message) =>
        message match {
            case Increment(channel, partitions) => 
                val newMap = mapping + (channel -> (mapping.getOrElse(channel, Seq()) ++ partitions))
                if newMap.values.map(_.size).sum == expectedResponses then
                    promise.success(newMap)
                    Behaviors.stopped
                else
                    getResponses(newMap)
            case Error(e) => 
                promise.failure(e)
                Behaviors.stopped
        }
    }
}