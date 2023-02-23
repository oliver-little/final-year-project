package org.oliverlittle.clusterprocess.scheduler

import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.worker_query
import org.oliverlittle.clusterprocess.connector.grpc.ChannelManager
import org.oliverlittle.clusterprocess.connector.cassandra.token._
import org.oliverlittle.clusterprocess.model.table.{Table, TableResult, LazyTableResult}

import io.grpc.stub.{StreamObserver, ServerCallStreamObserver}
import io.grpc.protobuf.StatusProto
import com.google.rpc.{Status, Code}
import akka.actor.typed.scaladsl.{Behaviors, LoggerOps, ActorContext}
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.Done

import scala.concurrent.Future

object ResultAssembler {
    sealed trait ResultEvent
    case class Data(row : table_model.StreamedTableResult) extends ResultEvent
    case class Complete() extends ResultEvent
    case class Error(e : Throwable) extends ResultEvent


    def startExecution(table : Table, channels : Seq[ChannelManager], output : ServerCallStreamObserver[table_model.StreamedTableResult]) : Future[Done] = {
        val system = ActorSystem(ResultAssembler(table, channels, output), "ResultAssembler")
        return system.whenTerminated
    }

    def apply(table : Table, channels : Seq[ChannelManager], output : ServerCallStreamObserver[table_model.StreamedTableResult]): Behavior[ResultEvent] = Behaviors.setup {context =>
        val query = worker_query.GetTableDataRequest(Some(table.protobuf))
        channels.map(_.workerComputeServiceStub.getTableData(query, MessageStreamedTableResultObserver(context.self)))
        new ResultAssembler(channels.size, output).getFirstHeader(0, Seq())
    }

    
}

class ResultAssembler(expectedResponses : Int, output : ServerCallStreamObserver[table_model.StreamedTableResult]) {
    import ResultAssembler._

    def getFirstHeader(numCompletes : Int, rows : Seq[table_model.StreamedTableResult]) : Behavior[ResultEvent] = Behaviors.receive { (context, message) =>
        message match {
            // Match header
            case Data(table_model.StreamedTableResult(table_model.StreamedTableResult.Data.Header(v), unknownFields)) if expectedResponses == 1 => receiveData(numCompletes)
            case Data(table_model.StreamedTableResult(table_model.StreamedTableResult.Data.Header(v), unknownFields)) => getHeaders(v, 1, numCompletes, rows)
            // Match row
            case Data(v) => getFirstHeader(numCompletes, rows :+ v)
            case Complete() if expectedResponses == numCompletes + 1 => handleError(IllegalStateException("Received all complete messages before headers were received"))
            case Complete() => getFirstHeader(numCompletes + 1, rows)
            case Error(e) => handleError(e)
        }
    }

    def getHeaders(header : table_model.TableResultHeader, numHeaders : Int, numCompletes : Int, rows : Seq[table_model.StreamedTableResult]) : Behavior[ResultEvent] = Behaviors.receive { (context, message) =>
        message match {
            // Mismatched headers
            case Data(table_model.StreamedTableResult(table_model.StreamedTableResult.Data.Header(v), unknownFields)) if v != header => handleError(IllegalStateException("Headers from workers do not match."))
            // Received all expected headers
            case Data(table_model.StreamedTableResult(table_model.StreamedTableResult.Data.Header(v), unknownFields)) if numHeaders + 1 == expectedResponses =>
                // Send a header
                output.onNext(table_model.StreamedTableResult().withHeader(header))
                // Send all rows we already had
                rows.foreach(row => output.onNext(row))
                // Wait for the rest of the rows
                receiveData(numCompletes)
            // Received another header
            case Data(table_model.StreamedTableResult(table_model.StreamedTableResult.Data.Header(v), unknownFields)) => getHeaders(header, numHeaders + 1, numCompletes, rows)
            // Received a row
            case Data(v) => getHeaders(header, numHeaders, numCompletes, rows :+ v)
            case Complete() if expectedResponses == numCompletes + 1 => handleError(IllegalStateException("Received all complete messages before headers were received"))
            case Complete() => getHeaders(header, numHeaders, numCompletes + 1, rows)
            case Error(e) => handleError(e)
        }
    }

    def receiveData(numCompletes : Int) : Behavior[ResultEvent] = Behaviors.receive {(context, message) =>
        message match {
            // Match header
            case Data(table_model.StreamedTableResult(table_model.StreamedTableResult.Data.Header(v), unknownFields)) => handleError(IllegalStateException("Received header from worker after all expected headers received."))
            // Match row
            case Data(v) => 
                output.onNext(v)
                Behaviors.same
            case Complete() if expectedResponses == numCompletes + 1 => 
                output.onCompleted
                Behaviors.stopped
            case Complete() => receiveData(numCompletes + 1)
            case Error(e) => handleError(e)
        }
    }

    def handleError(e : Throwable) : Behavior[ResultEvent] = Behaviors.setup {context =>
        val status = Status.newBuilder
            .setCode(Code.UNKNOWN.getNumber)
            .setMessage(e.getMessage)
            .build()
        output.onError(StatusProto.toStatusRuntimeException(status))
        context.system.terminate()
        Behaviors.stopped
    }
}

/**
  * Maps StreamObserver API to ResultAssembler Actor API
  */
class MessageStreamedTableResultObserver(replyTo : ActorRef[ResultAssembler.ResultEvent]) extends StreamObserver[table_model.StreamedTableResult]:
    override def onNext(value: table_model.StreamedTableResult) : Unit = replyTo ! ResultAssembler.Data(value)

    override def onError(e : Throwable) : Unit = replyTo ! ResultAssembler.Error(e)

    override def onCompleted(): Unit = replyTo ! ResultAssembler.Complete()