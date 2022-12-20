package org.oliverlittle.clusterprocess.server

import io.grpc.{ServerBuilder, ManagedChannelBuilder}
import scala.concurrent.{ExecutionContext, Future}
import java.util.logging.Logger

import org.oliverlittle.clusterprocess.client_query.{TableClientServiceGrpc, ComputeTableRequest, ComputeTableResult}
import org.oliverlittle.clusterprocess.worker_query
import org.oliverlittle.clusterprocess.data_source 
import org.oliverlittle.clusterprocess.model.table.sources.DataSource
import org.oliverlittle.clusterprocess.model.table.{Table, TableTransformation}

object ClientQueryServer {
    private val logger = Logger.getLogger(classOf[ClientQueryServer].getName)
    private val port = 50051

    def main(): Unit = {
        val server = new ClientQueryServer(ExecutionContext.global)
        server.blockUntilShutdown()
    }
}

class ClientQueryServer(executionContext: ExecutionContext) {
    private val server =  ServerBuilder.forPort(ClientQueryServer.port).addService(TableClientServiceGrpc.bindService(new ClientQueryServicer, executionContext)).build.start
    ClientQueryServer.logger.info("gRPC Server started, listening on " + ClientQueryServer.port)
    
    sys.addShutdownHook({
        ClientQueryServer.logger.info("*** Shutting down gRPC server since JVM is shutting down.")
        this.stop()
        ClientQueryServer.logger.info("*** gRPC server shut down.")
    })

    private def stop() : Unit = this.server.shutdown()

    private def blockUntilShutdown(): Unit = this.server.awaitTermination()

    private class ClientQueryServicer extends TableClientServiceGrpc.TableClientService {
        override def computeTable(request: ComputeTableRequest): Future[ComputeTableResult] = {
            ClientQueryServer.logger.info("Compute table request received")

            // Parse table
            val dataSource = DataSource.fromProtobuf(request.dataSource.get)
            val tableTransformations = TableTransformation.fromProtobuf(request.table.get)
            val table = Table(dataSource, tableTransformations)
            ClientQueryServer.logger.info("Created table instance.")
            
            if !table.isValid then throw new IllegalArgumentException("Table cannot be computed")
            
            val channel = ManagedChannelBuilder.forAddress("worker-service", 50052).usePlaintext().build
            val blockingStub = worker_query.WorkerComputeServiceGrpc.blockingStub(channel)
            blockingStub.computePartialResultCassandra(worker_query.ComputePartialResultCassandraRequest().withTable(request.table.get).withDataSource(request.dataSource.get.source.cassandra.get).withTokenRange(data_source.CassandraTokenRange(start=1, end=2)))

            val response = ComputeTableResult(uuid="sample data")
            Future.successful(response)
        }
    }
}
