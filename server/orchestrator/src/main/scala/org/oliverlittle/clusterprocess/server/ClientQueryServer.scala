package org.oliverlittle.clusterprocess.server

import org.oliverlittle.clusterprocess.worker_query
import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.client_query
import org.oliverlittle.clusterprocess.model.table.sources.DataSource
import org.oliverlittle.clusterprocess.model.table.{Table, TableTransformation, TableResult}
import org.oliverlittle.clusterprocess.scheduler.{WorkExecutionScheduler, ResultAssembler}
import org.oliverlittle.clusterprocess.connector.grpc.{WorkerHandler, StreamedTableResult, DelayedTableResultRunnable}
import org.oliverlittle.clusterprocess.connector.cassandra.CassandraConnector

import io.grpc.{ServerBuilder, ManagedChannelBuilder}
import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Failure}
import scala.util.Properties.{envOrElse, envOrNone}
import scala.jdk.CollectionConverters._
import com.typesafe.config.ConfigFactory

object ClientQueryServer {
    private val logger = LoggerFactory.getLogger(classOf[ClientQueryServer].getName)
    private val port = 50051

    def main(): Unit = {
        // K8s specific environment variables
        val numWorkers = envOrNone("NUM_WORKERS")
        val nodeName = envOrNone("WORKER_NODE_NAME")
        val baseURL = envOrNone("WORKER_SERVICE_URL")
        val workerPort = envOrElse("WORKER_PORT", "50052").toInt

        if !(numWorkers.isDefined && nodeName.isDefined && baseURL.isDefined) then logger.warn("Missing environment variables (NUM_WORKERS, WORKER_NODE_NAME, WORKER_SERVICE_URL) to initialise orchestrator, defaulting to localhost.")

        val workerAddresses : Seq[(String, Int)] = 
            if numWorkers.isDefined then (0 to numWorkers.get.toInt).map(num => (nodeName.get + "-" + num.toString + "." + baseURL.get, workerPort))
            else ConfigFactory.load.getStringList("clusterprocess.test.worker_urls").asScala.toSeq.map((_, workerPort))
        val server = new ClientQueryServer(workerAddresses)(using ExecutionContext.global)
        server.blockUntilShutdown()
    }
}

class ClientQueryServer( workerAddresses : Seq[(String, Int)])(using executionContext : ExecutionContext) {
    private val server =  ServerBuilder.forPort(ClientQueryServer.port).addService(client_query.TableClientServiceGrpc.bindService(new ClientQueryServicer(new WorkerHandler(workerAddresses)), executionContext)).build.start
    ClientQueryServer.logger.info("gRPC Server started, listening on " + ClientQueryServer.port)
    
    sys.addShutdownHook({
        ClientQueryServer.logger.info("*** Shutting down gRPC server since JVM is shutting down.")
        this.stop()
        ClientQueryServer.logger.info("*** gRPC server shut down.")
    })

    private def stop() : Unit = server.shutdown()

    private def blockUntilShutdown(): Unit = server.awaitTermination()

    private class ClientQueryServicer(workerHandler : WorkerHandler)(using executionContext : ExecutionContext) extends client_query.TableClientServiceGrpc.TableClientService {
        override def computeTable(request: client_query.ComputeTableRequest, responseObserver : StreamObserver[table_model.StreamedTableResult]): Unit = {
            ClientQueryServer.logger.info("Compute table request received")

            val table = Table.fromProtobuf(request.table.get)
            ClientQueryServer.logger.info("Created table instance.")
            
            if !table.isValid then responseObserver.onError(new IllegalArgumentException("Table cannot be computed"))

            // Generate a query plan
            val calculateQueryPlan = table.getQueryPlan
            val getCleanupQueryPlan = table.getCleanupQueryPlan
            ClientQueryServer.logger.info("Calculated query plan")

            // Able to make this unchecked cast because this is a response from a server
            val serverCallStreamObserver = responseObserver.asInstanceOf[ServerCallStreamObserver[table_model.StreamedTableResult]]
            
            // Prepare onReady hook 
            val runnable = DelayedTableResultRunnable(serverCallStreamObserver)
            serverCallStreamObserver.setOnReadyHandler(runnable)

            // Start execution, and add hook to send the data when finished
            WorkExecutionScheduler.startExecution(calculateQueryPlan, workerHandler).onComplete {
                case Success(_) =>
                    ClientQueryServer.logger.info("Result ready from workers, pulling data")
                    ResultAssembler.startExecution(table, workerHandler.channels, serverCallStreamObserver)          
                case Failure(e) => throw e // Rethrow any errors to propagate them to the client    
            }
        } 
    }
}
