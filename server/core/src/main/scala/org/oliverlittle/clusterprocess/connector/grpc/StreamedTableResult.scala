package org.oliverlittle.clusterprocess.connector.grpc

import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.model.table._
import org.oliverlittle.clusterprocess.model.table.field.TableValue

import io.grpc.stub.{StreamObserver, ServerCallStreamObserver}
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import collection.mutable.{Buffer, ArrayBuffer}
import scala.concurrent.Promise

object StreamedTableResult:
    def tableResultToIterator(tableResult : TableResult) : Iterator[table_model.StreamedTableResult] = {
        // Create a new iterator that sends the header, then all the rows
        val header = table_model.StreamedTableResult(table_model.StreamedTableResult.Data.Header(tableResult.header.protobuf))
        val rows = tableResult.rowsProtobuf.map(row => table_model.StreamedTableResult(table_model.StreamedTableResult.Data.Row(row)))
        return Iterator(header) ++ rows
    }

case class TableResultRunnable(responseObserver : ServerCallStreamObserver[table_model.StreamedTableResult], data : Iterator[table_model.StreamedTableResult], completedPromise : Option[Promise[Boolean]] = None) extends Runnable {
    private val logger = LoggerFactory.getLogger(classOf[TableResultRunnable].getName)

    var closed = false;
    def run(): Unit = {
        if closed then return;
        
        // Send data until we can't send anymore (either because the channel can't accept more yet, or because we don't have anything to send)
        while responseObserver.isReady && data.hasNext do
            responseObserver.onNext(data.next) 
        if !data.hasNext then
            logger.info("Completed sending data")
            responseObserver.onCompleted
            logger.info(completedPromise.isDefined.toString)
            if completedPromise.isDefined then
                logger.info("woihfoiewhfoiehfowiheoihfewoi")
                completedPromise.get.success(true)
            closed = true;
    }
}

/**
  * Modified TableResultRunnable that delays data sending until data is available.
  *
  * @param responseObserver
  */
class DelayedTableResultRunnable(responseObserver : ServerCallStreamObserver[table_model.StreamedTableResult], completedPromise : Promise[Boolean] = Promise[Boolean]()) extends Runnable {
    val future = completedPromise.future
    
    var data : Option[Iterator[table_model.StreamedTableResult]] = None
    var closed = false;

    private val promise = completedPromise
    private val logger = LoggerFactory.getLogger(classOf[DelayedTableResultRunnable].getName)

    /**
      * Starts execution with a TableResult
      *
      * @param tableResult
      */
    def setData(tableResult : TableResult) : Unit = {
        logger.info("Set data from single result.")
        val header = table_model.StreamedTableResult(table_model.StreamedTableResult.Data.Header(tableResult.header.protobuf))
        val rows = tableResult.rowsProtobuf.map(row => table_model.StreamedTableResult(table_model.StreamedTableResult.Data.Row(row)))
        val iterator = Iterator(header) ++ rows
        data = Some(iterator)
        run()
    }

    /**
      * Starts execution with an iterator of TableResults that are known to be the same type
      *
      * @param results
      */
    def setData(results : Iterator[TableResult]) : Unit = {
        logger.info("Set data from iterator")
        val tableResult = results.next
        val header = table_model.StreamedTableResult(table_model.StreamedTableResult.Data.Header(tableResult.header.protobuf))
        val rows = tableResult.rowsProtobuf.map(row => table_model.StreamedTableResult(table_model.StreamedTableResult.Data.Row(row)))

        // Create an iterator from the header of the first item, the rows of the first item, and the rows of the rest of the items
        val iterator = Iterator(header) ++ rows ++ results.flatMap(_.rowsProtobuf.map(row => table_model.StreamedTableResult(table_model.StreamedTableResult.Data.Row(row))))
        data = Some(iterator)
        run()
    }

    def setError(e : Throwable) : Unit = responseObserver.onError(e)

    def run(): Unit = {
        if closed then return;
        if data.isEmpty then return;

        val iterator = data.get
            
        // Send data until we can't send anymore (either because the channel can't accept more yet, or because we don't have anything to send)
        while responseObserver.isReady && iterator.hasNext do
            responseObserver.onNext(iterator.next) 
        if !iterator.hasNext then
            logger.info("Completed sending data")
            responseObserver.onCompleted
            promise.success(true)
            closed = true;
    }
}

class StreamedTableResultCompiler(onComplete : Promise[Option[TableResult]]) extends StreamObserver[table_model.StreamedTableResult]:

    var header : Option[TableResultHeader] = None
    var rows : Buffer[Seq[Option[TableValue]]] = ArrayBuffer()

    override def onNext(value: table_model.StreamedTableResult) : Unit = {
        value.data.number match {
            case 1 if header.isEmpty => header = Some(TableResultHeader.fromProtobuf(value.data.header.get))
            case 2 => rows += value.data.row.get.values.map(TableValue.fromProtobuf(_))
            case e => throw new IllegalArgumentException("Unknown result value found, or header was defined twice:" + e.toString)
        }
    }

    override def onError(t: Throwable) : Unit = onComplete.failure(t)

    override def onCompleted(): Unit = 
        if header.isEmpty then onComplete.success(None)
        else onComplete.success(Some(EvaluatedTableResult(header.get, rows.toSeq)))