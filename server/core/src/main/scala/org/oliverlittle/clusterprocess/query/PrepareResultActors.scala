package org.oliverlittle.clusterprocess.query

import org.oliverlittle.clusterprocess.worker_query
import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.model.table._
import org.oliverlittle.clusterprocess.model.table.field._
import org.oliverlittle.clusterprocess.connector.grpc.{ChannelManager}

import akka.actor.typed.scaladsl.{Behaviors, LoggerOps, ActorContext}
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}

import collection.mutable.{Buffer, ArrayBuffer}
import scala.concurrent.{Future, Promise}
import org.oliverlittle.clusterprocess.query.PrepareResultConsumer.ConsumerEvent
import scala.util.{Success, Failure}

trait PrepareResultConsumerFactory:
    def createConsumer(channel : ChannelManager, items : Seq[(PartialTable, worker_query.QueryPlanItem)], counter : ActorRef[PrepareResultCounter.CounterEvent]) : Behavior[PrepareResultConsumer.ConsumerEvent]

class BasePrepareResultConsumerFactory extends PrepareResultConsumerFactory:
    def createConsumer(channel : ChannelManager, items : Seq[(PartialTable, worker_query.QueryPlanItem)], counter : ActorRef[PrepareResultCounter.CounterEvent]) : Behavior[PrepareResultConsumer.ConsumerEvent] = PrepareResultConsumer(channel, items, counter)


object PrepareResultConsumer:
    sealed trait ConsumerEvent
    final case class Completed() extends ConsumerEvent
    final case class Error(e : Throwable) extends ConsumerEvent

    def apply(channel : ChannelManager, items : Seq[(PartialTable, worker_query.QueryPlanItem)], counter : ActorRef[PrepareResultCounter.CounterEvent]) : Behavior[ConsumerEvent] = Behaviors.setup{context => 
        if items.size == 0 
        then Behaviors.stopped
        else new PrepareResultConsumer(channel, channel.workerComputeServiceStub, counter, context).start(Seq(), items)
    }

class PrepareResultConsumer private (channel : ChannelManager, stub : worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceStub, counter : ActorRef[PrepareResultCounter.CounterEvent], context : ActorContext[ConsumerEvent]) {
    import PrepareResultConsumer._

    private def start(partitions : Seq[PartialTable], items : Seq[(PartialTable, worker_query.QueryPlanItem)]) : Behavior[ConsumerEvent] = Behaviors.setup {context =>
        getResult(items.head._2)
        computeWork(Seq(items.head._1), items.tail)
    }

    private def computeWork(partitions : Seq[PartialTable], items : Seq[(PartialTable, worker_query.QueryPlanItem)]) : Behavior[ConsumerEvent] = Behaviors.receiveMessage{
        case Completed() if items.size == 0 =>
            counter ! PrepareResultCounter.Increment(channel, partitions) 
            Behaviors.stopped
        case Completed() => 
            getResult(items.head._2)
            computeWork(partitions :+ items.head._1, items.tail)
        case Error(e) => 
            counter ! PrepareResultCounter.Error(e)
            Behaviors.stopped
    }

    private def getResult(item : worker_query.QueryPlanItem) : Unit = {
        context.pipeToSelf(stub.processQueryPlanItem(item)) {
            case Success(value) => Completed()
            case Failure(e) => Error(e)
        }
    }
}

trait PrepareResultCounterFactory:
    def createCounter(expectedResponses : Int, promise : Promise[Map[ChannelManager, Seq[PartialTable]]]) : Behavior[PrepareResultCounter.CounterEvent]

class BasePrepareResultCounterFactory extends PrepareResultCounterFactory:
    def createCounter(expectedResponses : Int, promise : Promise[Map[ChannelManager, Seq[PartialTable]]]) : Behavior[PrepareResultCounter.CounterEvent] = PrepareResultCounter(expectedResponses, promise)

object PrepareResultCounter:
    sealed trait CounterEvent
    case class Increment(channel : ChannelManager, partition : Seq[PartialTable]) extends CounterEvent
    case class Error(e : Throwable) extends CounterEvent

    def apply(expectedResponses : Int, promise : Promise[Map[ChannelManager, Seq[PartialTable]]]) : Behavior[CounterEvent] = Behaviors.setup{context => 
        new PrepareResultCounter(expectedResponses, promise, context).start()
    }

class PrepareResultCounter private (expectedResponses : Int, promise : Promise[Map[ChannelManager, Seq[PartialTable]]], context : ActorContext[PrepareResultCounter.CounterEvent]) {
    import PrepareResultCounter._

    /**
     * Handles receiving the first response from any consumer
     */
    private def start() : Behavior[PrepareResultCounter.CounterEvent] = Behaviors.receive {(context, message) =>
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
    private def getResponses(mapping : Map[ChannelManager, Seq[PartialTable]]) : Behavior[PrepareResultCounter.CounterEvent] = Behaviors.receive { (context, message) =>
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