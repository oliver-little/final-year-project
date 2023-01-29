package org.oliverlittle.clusterprocess.scheduler

import org.oliverlittle.clusterprocess.worker_query
import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.model.table._
import org.oliverlittle.clusterprocess.model.table.field._

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.LoggerOps
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}

import collection.mutable.{Buffer, ArrayBuffer}

object WorkProducer {
    sealed trait ProducerEvent
    final case class RequestWork(replyTo : ActorRef[WorkConsumer.ConsumerEvent]) extends ProducerEvent
    
    def apply(items : Seq[worker_query.ComputePartialResultCassandraRequest]) : Behavior[ProducerEvent] = {
        println(items.size.toString + " items.")
        return list(items)
    }

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

    def apply(stub : worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceBlockingStub, producers : Seq[ActorRef[WorkProducer.ProducerEvent]], assembler : ActorRef[WorkExecutionScheduler.AssemblerEvent]) : Behavior[ConsumerEvent] = Behaviors.setup{context => 
        producers.head ! WorkProducer.RequestWork(context.self)
        computeWork(stub, producers, assembler)
    }

    private def computeWork(stub : worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceBlockingStub, producers : Seq[ActorRef[WorkProducer.ProducerEvent]], assembler : ActorRef[WorkExecutionScheduler.AssemblerEvent]) : Behavior[ConsumerEvent] = Behaviors.receive{(context, message) => 
        message match {
            case NoWork() if producers.length == 0 => Behaviors.stopped
            case NoWork() => computeWork(stub, producers.tail, assembler)
            case HasWork(request) => 
                // Make the request to the worker node
                val results : Iterator[table_model.StreamedTableResult] = stub.computePartialResultCassandra(request)
                // Process the results and send to the assembler
                assembler ! WorkExecutionScheduler.SendResult(processStreamedResults(results))
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