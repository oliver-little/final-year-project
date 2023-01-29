package org.oliverlittle.clusterprocess.connector.grpc

import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.model.table.TableResult

import io.grpc.stub.ServerCallStreamObserver

import java.util.logging.Logger

object StreamedTableResult:
    def tableResultToIterator(tableResult : TableResult) : Iterator[table_model.StreamedTableResult] = {
        // Create a new iterator that sends the header, then all the rows
        val header = table_model.StreamedTableResult(table_model.StreamedTableResult.Data.Header(tableResult.header.protobuf))
        val rows = tableResult.rowsProtobuf.map(row => table_model.StreamedTableResult(table_model.StreamedTableResult.Data.Row(row)))
        return Iterator(header) ++ rows
    }

class TableResultRunnable(responseObserver : ServerCallStreamObserver[table_model.StreamedTableResult], data : Iterator[table_model.StreamedTableResult]) extends Runnable {
    private val logger = Logger.getLogger(classOf[TableResultRunnable].getName)

    var closed = false;
    def run(): Unit = {
        if closed then return;
        
        // Send data until we can't send anymore (either because the channel can't accept more yet, or because we don't have anything to send)
        while responseObserver.isReady && data.hasNext do
            responseObserver.onNext(data.next) 
        if !data.hasNext then
            logger.info("Completed sending data")
            responseObserver.onCompleted
            closed = true;
    }
}

/**
  * Modified TableResultRunnable that delays data sending until data is available.
  *
  * @param responseObserver
  */
class DelayedTableResultRunnable(responseObserver : ServerCallStreamObserver[table_model.StreamedTableResult]) extends Runnable {
    var data : Option[Iterator[table_model.StreamedTableResult]] = None
    var closed = false;

    private val logger = Logger.getLogger(classOf[DelayedTableResultRunnable].getName)
    def setData(tableResult : TableResult) : Unit = {
        val header = table_model.StreamedTableResult(table_model.StreamedTableResult.Data.Header(tableResult.header.protobuf))
        val rows = tableResult.rowsProtobuf.map(row => table_model.StreamedTableResult(table_model.StreamedTableResult.Data.Row(row)))
        val iterator = Iterator(header) ++ rows
        data = Some(iterator)
        run()
    }

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
            closed = true;
    }
}
