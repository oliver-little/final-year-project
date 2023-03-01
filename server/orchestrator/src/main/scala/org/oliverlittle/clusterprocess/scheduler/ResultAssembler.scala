package org.oliverlittle.clusterprocess.scheduler

import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.worker_query
import org.oliverlittle.clusterprocess.connector.grpc.{ChannelManager, TableResultRunnable, StreamedTableResult, StreamedTableResultCompiler}
import org.oliverlittle.clusterprocess.connector.cassandra.token._
import org.oliverlittle.clusterprocess.model.table.{Table, TableResult, LazyTableResult, Assembler}

import io.grpc.stub.{StreamObserver, ServerCallStreamObserver}
import io.grpc.protobuf.StatusProto
import com.google.rpc.{Status, Code}
import akka.actor.typed.scaladsl.{Behaviors, LoggerOps, ActorContext}
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.Done

import scala.concurrent.{Promise, Future}
import scala.util.{Success, Failure}

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

/**
  * ResultAssembler definition for alternative Assembler classes, does not apply short circuiting - all results will be held in memory until they are ready
  */
object CustomResultAssembler {
    sealed trait ResultEvent
    case class Data(result : TableResult) extends ResultEvent
    case class NoData() extends ResultEvent
    case class Error(e : Throwable) extends ResultEvent


    def startExecution(table : Table, assembler : Assembler, channels : Seq[ChannelManager], output : ServerCallStreamObserver[table_model.StreamedTableResult]) : Future[Boolean] = {
        val promise = Promise[Boolean]()
        val system = ActorSystem(CustomResultAssembler(table, assembler, channels, output, promise), "CustomResultAssembler")
        return promise.future
    }

    def apply(table : Table, assembler : Assembler, channels : Seq[ChannelManager], output : ServerCallStreamObserver[table_model.StreamedTableResult], onComplete : Promise[Boolean]): Behavior[ResultEvent] = Behaviors.setup {context =>
        val query = worker_query.GetTableDataRequest(Some(table.protobuf))
        // Map results to self
        channels.map {channel =>
            val promise = Promise[Option[TableResult]]()
            context.pipeToSelf(promise.future) {
                case Success(Some(t)) => Data(t)
                case Success(None) => NoData()
                case Failure(e) => Error(e)
            }

            channel.workerComputeServiceStub.getTableData(query, StreamedTableResultCompiler(promise))
        }
        new CustomResultAssembler(channels.size, assembler, output, onComplete, context).getResults(0, Seq())
    }
}

class CustomResultAssembler private (expectedResponses : Int, assembler : Assembler, output : ServerCallStreamObserver[table_model.StreamedTableResult], onComplete : Promise[Boolean], context : ActorContext[CustomResultAssembler.ResultEvent]) {
    import CustomResultAssembler._

    def getResults(numResponses : Int, results : Seq[TableResult]) : Behavior[ResultEvent] = Behaviors.receiveMessage {
        case Data(result) if numResponses + 1 == expectedResponses =>
            finished(results :+ result)
            Behaviors.stopped
        case NoData() if numResponses + 1 == expectedResponses =>
            finished(results)
            Behaviors.stopped
        case Data(result) => getResults(numResponses + 1, results :+ result)
        case NoData() => getResults(numResponses + 1, results)
        case Error(e) => 
            output.onError(e)
            onComplete.failure(e)
            Behaviors.stopped
    }

    def finished(results : Seq[TableResult]) : Unit = {
        val finalResult = assembler.assemblePartial(results) 
        val runnable = TableResultRunnable(output, StreamedTableResult.tableResultToIterator(finalResult), Some(onComplete))
        runnable.run()
    }
}
