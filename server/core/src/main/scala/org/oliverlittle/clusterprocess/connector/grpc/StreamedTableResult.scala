package org.oliverlittle.clusterprocess.connector.grpc

import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.model.table.TableResult

import io.grpc.stub.ServerCallStreamObserver

import java.util.logging.Logger

object StreamedTableResult:
    def sendTableResult(responseObserver : ServerCallStreamObserver[table_model.StreamedTableResult], tableResult : TableResult) : Unit = {
        // Create a new iterator that sends the header, then all the rows
        val header = table_model.StreamedTableResult(table_model.StreamedTableResult.Data.Header(tableResult.header.protobuf))
        val rows = tableResult.rowsProtobuf.map(row => table_model.StreamedTableResult(table_model.StreamedTableResult.Data.Row(row)))
        val data = Iterator(header) ++ rows
        val runnable = TableResultRunnable(responseObserver, data)

        responseObserver.setOnReadyHandler(runnable)
        
        runnable.run()
    }

    class TableResultRunnable(responseObserver : ServerCallStreamObserver[table_model.StreamedTableResult], data : Iterator[table_model.StreamedTableResult]) extends Runnable {
        private val logger = Logger.getLogger(classOf[TableResultRunnable].getName)

        var closed = false;
        def run(): Unit = {
            logger.info("Running")
            logger.info(closed.toString)
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