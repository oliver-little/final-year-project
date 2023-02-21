package org.oliverlittle.clusterprocess.query

import org.oliverlittle.clusterprocess.worker_query
import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.model.table._
import org.oliverlittle.clusterprocess.model.table.field._

import akka.actor.typed.scaladsl.{Behaviors, LoggerOps, ActorContext}
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}

import collection.mutable.{Buffer, ArrayBuffer}

trait WorkProducerFactory:
    def createProducer(items : Seq[worker_query.QueryPlanItem]) : Behavior[WorkProducer.ProducerEvent] 
        
class BaseWorkProducerFactory extends WorkProducerFactory:
    def createProducer(items : Seq[worker_query.QueryPlanItem]) : Behavior[WorkProducer.ProducerEvent] = WorkProducer(items)

object WorkProducer {
    sealed trait ProducerEvent
    final case class RequestWork(replyTo : ActorRef[WorkConsumer.ConsumerEvent]) extends ProducerEvent
    
    def apply(items : Seq[worker_query.QueryPlanItem]) : Behavior[ProducerEvent] = list(items)

    private def list(items : Seq[worker_query.QueryPlanItem]) : Behavior[ProducerEvent] = Behaviors.receiveMessage {
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

trait WorkConsumerFactory:
    def createConsumer(stub : worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceBlockingStub, producers : Seq[ActorRef[WorkProducer.ProducerEvent]], counter : ActorRef[Counter.CounterEvent]) : Behavior[WorkConsumer.ConsumerEvent]

class BaseWorkConsumerFactory extends WorkConsumerFactory:
    def createConsumer(stub: worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceBlockingStub, producers: Seq[ActorRef[WorkProducer.ProducerEvent]], counter: ActorRef[Counter.CounterEvent]) : Behavior[WorkConsumer.ConsumerEvent] = WorkConsumer(stub, producers, counter)


object WorkConsumer {
    sealed trait ConsumerEvent
    final case class HasWork(request : worker_query.QueryPlanItem) extends ConsumerEvent
    final case class NoWork() extends ConsumerEvent

    def apply(stub : worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceBlockingStub, producers : Seq[ActorRef[WorkProducer.ProducerEvent]], counter : ActorRef[Counter.CounterEvent]) : Behavior[ConsumerEvent] = Behaviors.setup{context => 
        producers.head ! WorkProducer.RequestWork(context.self)
        computeWork(stub, producers, counter)
    }

    private def computeWork(stub : worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceBlockingStub, producers : Seq[ActorRef[WorkProducer.ProducerEvent]], counter : ActorRef[Counter.CounterEvent]) : Behavior[ConsumerEvent] = Behaviors.receive{(context, message) => 
        message match {
            case NoWork() if producers.length == 1 => 
                Behaviors.stopped
            case NoWork() => 
                producers.tail.head ! WorkProducer.RequestWork(context.self)
                computeWork(stub, producers.tail, counter)
            case HasWork(request) => 
                // Make the request to the worker node
                val result = stub.processQueryPlanItem(request)
                // Process the results and send to the counter
                counter ! Counter.Increment()
                // Request more work from the producer
                producers.head ! WorkProducer.RequestWork(context.self)
                Behaviors.same
        }
    }

    /**
      * Combines an iterator of streamed results into a full TableResult
      *
      * @param resultIterator Iterator provided by gRPC
      */
    def processStreamedResults(resultIterator : Iterator[table_model.StreamedTableResult]) : TableResult = {
        var header : Option[TableResultHeader] = None
        var rows : Buffer[Seq[Option[TableValue]]] = ArrayBuffer()
        while resultIterator.hasNext do
            val result : table_model.StreamedTableResult = resultIterator.next
            result.data.number match {
                case 1 if header.isEmpty => header = Some(TableResultHeader.fromProtobuf(result.data.header.get))
                case 2 => rows += result.data.row.get.values.map(TableValue.fromProtobuf(_))
                case _ => throw new IllegalArgumentException("Unknown result value found, or header was defined twice.")
            }

        if header.isEmpty then throw new IllegalArgumentException("Header not defined in response")

        return EvaluatedTableResult(header.get, rows.toSeq)        
    }
}

trait CounterFactory:
    def createCounter(expectedResponses : Int, onComplete : () => Unit, onError : Throwable => Unit) : Behavior[Counter.CounterEvent]

class BaseCounterFactory extends CounterFactory:
    def createCounter(expectedResponses : Int, onComplete : () => Unit, onError : Throwable => Unit) : Behavior[Counter.CounterEvent] = Counter(expectedResponses, onComplete, onError)

object Counter:
    sealed trait CounterEvent
    case class Increment() extends CounterEvent


    def apply(expectedResponses : Int, onComplete : () => Unit, onError : Throwable => Unit) : Behavior[CounterEvent] = Behaviors.setup{context => 
        new Counter(expectedResponses, onComplete, onError, context).start()
    }

class Counter private (expectedResponses : Int, onComplete : () => Unit, onError : Throwable => Unit, context : ActorContext[Counter.CounterEvent]) {
    import Counter._

    /**
     * Handles receiving the first response from any consumer
     */
    private def start() : Behavior[Counter.CounterEvent] = Behaviors.receive {(context, message) =>
        message match {
            case Increment() => 
                if expectedResponses == 1 then 
                    onComplete()
                    Behaviors.stopped
                else getResponses(1)
        }
    }

    /**
     *  Handles receiving all subsequent responses from consumers until expectedResponses is reached
     */
    private def getResponses(numResponses : Int) : Behavior[Counter.CounterEvent] = Behaviors.receive { (context, message) =>
        message match {
            case Increment() => 
                if numResponses + 1 == expectedResponses then
                    onComplete()
                    Behaviors.stopped
                else
                    context.log.info("Got " + (numResponses + 1).toString + " responses, need " + expectedResponses.toString + " responses")
                    getResponses(numResponses + 1)
        }
    }
}