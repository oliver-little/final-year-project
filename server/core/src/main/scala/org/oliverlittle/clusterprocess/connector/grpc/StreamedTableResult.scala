package org.oliverlittle.clusterprocess.connector.grpc

import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.model.table.TableResult

import io.grpc.stub.StreamObserver

object StreamedTableResult:
    def sendTableResult(responseObserver : StreamObserver[table_model.StreamedTableResult], tableResult : TableResult) : Unit = {
        // Send the header
        responseObserver.onNext(table_model.StreamedTableResult(table_model.StreamedTableResult.Data.Header(tableResult.header.protobuf)))

        // Send all rows
        // Need to rate limit this foreach, it fills the memory buffer and causes an out of memory error almost immediately
        // By trying to send the entire table at once
        // This should be done using ServerCallStreamObserver (think you have to cast it down explicitly but investigate the generated gRPC)
        // These links discuss similar issues and potential solutions
        // https://github.com/grpc/grpc-java/issues/2247
        // https://stackoverflow.com/questions/64066930/java-grpc-server-for-long-lived-streams-effective-implementation
        // In particular, setOnReadyHandler(Runnable) which can consume the iterator at the rate it is ready
        tableResult.rowsProtobuf.foreach(row => responseObserver.onNext(table_model.StreamedTableResult(table_model.StreamedTableResult.Data.Row(row))))

        // Send completion notice
        responseObserver.onCompleted()
    }