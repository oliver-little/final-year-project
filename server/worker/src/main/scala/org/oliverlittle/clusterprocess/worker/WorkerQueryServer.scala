package org.oliverlittle.clusterprocess.worker

import io.grpc.ServerBuilder
import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}
import scala.concurrent.{ExecutionContext, Future}
import java.util.logging.Logger

import org.oliverlittle.clusterprocess.worker_query
import org.oliverlittle.clusterprocess.data_source
import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.model.table.{Table, TableTransformation}
import org.oliverlittle.clusterprocess.model.table.sources.cassandra.CassandraDataSource
import org.oliverlittle.clusterprocess.connector.cassandra.CassandraConnector
import org.oliverlittle.clusterprocess.connector.cassandra.token.CassandraTokenRange
import org.oliverlittle.clusterprocess.connector.grpc.StreamedTableResult

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
        override def computePartialResultCassandra(request : worker_query.ComputePartialResultCassandraRequest, responseObserver : StreamObserver[table_model.StreamedTableResult]) : Unit = {
            WorkerQueryServer.logger.info("computePartialResultCassandra")

            // Parse table
            val cassandraSourcePB = request.dataSource.get
            // Deserialise protobuf token range and pass into datasource here:
            val tokenRangeProtobuf = request.tokenRange.get
            val tokenRange = CassandraTokenRange.fromLong(tokenRangeProtobuf.start, tokenRangeProtobuf.end)
            val dataSource = CassandraDataSource.inferDataSourceFromCassandra(cassandraSourcePB.keyspace, cassandraSourcePB.table, Some(tokenRange))
            val tableTransformations = TableTransformation.fromProtobuf(request.table.get)
            val table = Table(dataSource, tableTransformations)
            WorkerQueryServer.logger.info("Created table instance.")
            
            if !table.isValid then responseObserver.onError(new IllegalArgumentException("Table cannot be computed."))
            
            WorkerQueryServer.logger.info("Computing table result")
            val result = table.compute
            WorkerQueryServer.logger.info("Table result ready.")

            // Able to make this unchecked cast because this is a response from a server
            StreamedTableResult.sendTableResult(responseObserver.asInstanceOf[ServerCallStreamObserver[table_model.StreamedTableResult]], result)
        }

        override def getLocalCassandraNode(request : worker_query.GetLocalCassandraNodeRequest) : Future[worker_query.GetLocalCassandraNodeResult] = {
            WorkerQueryServer.logger.info("getLocalCassandraNode")
            WorkerQueryServer.logger.info("Host: " + CassandraConnector.socket.getHostName + ", port: " + CassandraConnector.socket.getPort.toString)
            val response = worker_query.GetLocalCassandraNodeResult(address=Some(data_source.InetSocketAddress(host=CassandraConnector.socket.getHostName, port=CassandraConnector.socket.getPort)))
            Future.successful(response)
        }
    }
}
