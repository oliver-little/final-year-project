package org.oliverlittle.clusterprocess.server

import org.oliverlittle.clusterprocess.worker_query
import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.client_query
import org.oliverlittle.clusterprocess.query.QueryPlan
import org.oliverlittle.clusterprocess.model.table.sources.DataSource
import org.oliverlittle.clusterprocess.model.table.{Table, TableTransformation, TableResult}
import org.oliverlittle.clusterprocess.scheduler.WorkExecutionScheduler
import org.oliverlittle.clusterprocess.connector.grpc.{StreamedTableResult, DelayedTableResultRunnable, WorkerHandler}
import org.oliverlittle.clusterprocess.connector.cassandra.CassandraConnector

import io.grpc.{ServerBuilder, ManagedChannelBuilder}
import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}

import java.util.logging.Logger
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Properties.{envOrElse, envOrNone}
import collection.JavaConverters._
import com.typesafe.config.ConfigFactory

object ClientQueryServer {
    private val logger = Logger.getLogger(classOf[ClientQueryServer].getName)
    private val port = 50051

    def main(): Unit = {
        // K8s specific environment variables
        val numWorkers = envOrNone("NUM_WORKERS")
        val nodeName = envOrNone("WORKER_NODE_NAME")
        val baseURL = envOrNone("WORKER_SERVICE_URL")
        val workerPort = envOrElse("WORKER_PORT", "50052").toInt

        if !(numWorkers.isDefined && nodeName.isDefined && baseURL.isDefined) then logger.warning("Missing environment variables (NUM_WORKERS, WORKER_NODE_NAME, WORKER_SERVICE_URL) to initialise orchestrator, defaulting to localhost.")

        val workerAddresses : Seq[(String, Int)] = 
            if numWorkers.isDefined then (0 to numWorkers.get.toInt).map(num => (nodeName.get + "-" + num.toString + "." + baseURL.get, workerPort))
            else ConfigFactory.load.getStringList("clusterprocess.test.worker_urls").asScala.toSeq.map((_, workerPort))
        val server = new ClientQueryServer(ExecutionContext.global, CassandraConnector(), workerAddresses)
        server.blockUntilShutdown()
    }
}

class ClientQueryServer(executionContext: ExecutionContext, connector : CassandraConnector, workerAddresses : Seq[(String, Int)]) {
    private val server =  ServerBuilder.forPort(ClientQueryServer.port).addService(client_query.TableClientServiceGrpc.bindService(new ClientQueryServicer(connector, new WorkerHandler(workerAddresses)), executionContext)).build.start
    ClientQueryServer.logger.info("gRPC Server started, listening on " + ClientQueryServer.port)
    
    sys.addShutdownHook({
        ClientQueryServer.logger.info("*** Shutting down gRPC server since JVM is shutting down.")
        this.stop()
        ClientQueryServer.logger.info("*** gRPC server shut down.")
    })

    private def stop() : Unit = server.shutdown()

    private def blockUntilShutdown(): Unit = server.awaitTermination()

    private class ClientQueryServicer(connector : CassandraConnector, workerHandler : WorkerHandler) extends client_query.TableClientServiceGrpc.TableClientService {
        override def computeTable(request: client_query.ComputeTableRequest, responseObserver : StreamObserver[table_model.StreamedTableResult]): Unit = {
            ClientQueryServer.logger.info("Compute table request received")

            // Parse table
            val dataSource = DataSource.fromProtobuf(connector, request.dataSource.get)

            val tableTransformations = TableTransformation.fromProtobuf(request.table.get)
            val table = Table(dataSource, tableTransformations)
            ClientQueryServer.logger.info("Created table instance.")
            
            if !table.isValid then responseObserver.onError(new IllegalArgumentException("Table cannot be computed"))

            // Generate a query plan
            val queryPlan = QueryPlan(table)

            // Able to make this unchecked cast because this is a response from a server
            val serverCallStreamObserver = responseObserver.asInstanceOf[ServerCallStreamObserver[table_model.StreamedTableResult]]
            
            // Prepare onReady hook 
            val runnable = DelayedTableResultRunnable(serverCallStreamObserver)
            serverCallStreamObserver.setOnReadyHandler(runnable)

            // Start execution, and add hook to send the data when finished
            WorkExecutionScheduler.startFromData(queryPlan, workerHandler, table, {result =>
                ClientQueryServer.logger.info("Result ready from workers.")
                runnable.setData(result)
            })
        } 
    }
}
