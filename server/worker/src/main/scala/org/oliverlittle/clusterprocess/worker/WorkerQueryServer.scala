package org.oliverlittle.clusterprocess.worker

import io.grpc.ServerBuilder
import scala.concurrent.{ExecutionContext, Future}
import java.util.logging.Logger

import org.oliverlittle.clusterprocess.worker_query._
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
    private val server =  ServerBuilder.forPort(WorkerQueryServer.port).addService(WorkerComputeServiceGrpc.bindService(new WorkerQueryServicer, executionContext)).build.start
    WorkerQueryServer.logger.info("gRPC Server started, listening on " + WorkerQueryServer.port)
    
    sys.addShutdownHook({
        WorkerQueryServer.logger.info("*** Shutting down gRPC server since JVM is shutting down.")
        this.stop()
        WorkerQueryServer.logger.info("*** gRPC server shut down.")
    })

    private def stop() : Unit = this.server.shutdown()

    private def blockUntilShutdown(): Unit = this.server.awaitTermination()

    private class WorkerQueryServicer extends WorkerComputeServiceGrpc.WorkerComputeService {
        override def computePartialResultCassandra(request : ComputePartialResultCassandraRequest) : Future[ComputePartialResultCassandraResult] = {
            WorkerQueryServer.logger.info("computePartialResultCassandra")

            // Parse table
            val cassandraSourcePB = request.dataSource.get
            val dataSource = CassandraDataSource.inferDataSourceFromCassandra(cassandraSourcePB.keyspace, cassandraSourcePB.table)
            val tableTransformations = TableTransformation.fromProtobuf(request.table.get)
            val table = Table(dataSource, tableTransformations)
            WorkerQueryServer.logger.info("Created table instance.")
            
            if !table.isValid then throw new IllegalArgumentException("Table cannot be computed")

            val response = ComputePartialResultCassandraResult(success=true)
            Future.successful(response)
        }

        override def getLocalCassandraNode(request : GetLocalCassandraNodeRequest) : Future[GetLocalCassandraNodeResult] = {
            WorkerQueryServer.logger.info("getLocalCassandraNode")

            val response = GetLocalCassandraNodeResult(address=Some(data_source.InetSocketAddress(host=CassandraConnector.socket.getHostName(), port=CassandraConnector.socket.getPort())))
            Future.successful(response)
        }
    }
}
