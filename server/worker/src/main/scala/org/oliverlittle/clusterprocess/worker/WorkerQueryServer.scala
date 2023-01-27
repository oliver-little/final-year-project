package org.oliverlittle.clusterprocess.worker

import io.grpc.ServerBuilder
import io.grpc.stub.StreamObserver;
import scala.concurrent.{ExecutionContext, Future}
import java.util.logging.Logger

import org.oliverlittle.clusterprocess.worker_query
import org.oliverlittle.clusterprocess.data_source
import org.oliverlittle.clusterprocess.model.table.sources.cassandra.CassandraDataSource
import org.oliverlittle.clusterprocess.model.table.{Table, TableTransformation}
import org.oliverlittle.clusterprocess.connector.cassandra.CassandraConnector

object WorkerQueryServer {
    private val logger = Logger.getLogger(classOf[WorkerQueryServer].getName)
    private val port = 50052

    def main(): Unit = {
        val server = new WorkerQueryServer(ExecutionContext.global)
        server.blockUntilShutdown()
    }
}

class WorkerQueryServer(executionContext: ExecutionContext) {
    private val server =  ServerBuilder.forPort(WorkerQueryServer.port).addService(worker_query.WorkerComputeServiceGrpc.bindService(new WorkerQueryServicer, executionContext)).build.start
    WorkerQueryServer.logger.info("gRPC Server started, listening on " + WorkerQueryServer.port)
    
    sys.addShutdownHook({
        WorkerQueryServer.logger.info("*** Shutting down gRPC server since JVM is shutting down.")
        this.stop()
        WorkerQueryServer.logger.info("*** gRPC server shut down.")
    })

    private def stop() : Unit = this.server.shutdown()

    private def blockUntilShutdown(): Unit = this.server.awaitTermination()

    private class WorkerQueryServicer extends worker_query.WorkerComputeServiceGrpc.WorkerComputeService {
        override def computePartialResultCassandra(request : worker_query.ComputePartialResultCassandraRequest, responseObserver : StreamObserver[worker_query.StreamedTableResult]) : Unit = {
            WorkerQueryServer.logger.info("computePartialResultCassandra")

            // Parse table
            val cassandraSourcePB = request.dataSource.get
            val dataSource = CassandraDataSource.inferDataSourceFromCassandra(cassandraSourcePB.keyspace, cassandraSourcePB.table)
            val tableTransformations = TableTransformation.fromProtobuf(request.table.get)
            val table = Table(dataSource, tableTransformations)
            WorkerQueryServer.logger.info("Created table instance.")
            
            if !table.isValid then responseObserver.onError(new IllegalArgumentException("Table cannot be computed."))

            val tableResult = table.compute

            // Send the header
            responseObserver.onNext(worker_query.StreamedTableResult(worker_query.StreamedTableResult.Data.Header(tableResult.header.protobuf)))

            // Send all rows
            tableResult.rowsProtobuf.foreach(row => responseObserver.onNext(worker_query.StreamedTableResult(worker_query.StreamedTableResult.Data.Row(row))))

            // Send completion notice
            responseObserver.onCompleted()
        }

        override def getLocalCassandraNode(request : worker_query.GetLocalCassandraNodeRequest) : Future[worker_query.GetLocalCassandraNodeResult] = {
            WorkerQueryServer.logger.info("getLocalCassandraNode")
            WorkerQueryServer.logger.info("Host: " + CassandraConnector.socket.getHostName + ", port: " + CassandraConnector.socket.getPort.toString)
            val response = worker_query.GetLocalCassandraNodeResult(address=Some(data_source.InetSocketAddress(host=CassandraConnector.socket.getHostName, port=CassandraConnector.socket.getPort)))
            Future.successful(response)
        }
    }
}
