package org.oliverlittle.clusterprocess.server

import org.oliverlittle.clusterprocess.worker_query
import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.client_query
import org.oliverlittle.clusterprocess.model.table.sources.DataSource
import org.oliverlittle.clusterprocess.model.table._
import org.oliverlittle.clusterprocess.scheduler.{WorkExecutionScheduler, ResultAssembler, CustomResultAssembler}
import org.oliverlittle.clusterprocess.connector.grpc.{WorkerHandler, ChannelManager, BaseChannelManager, StreamedTableResult, DelayedTableResultRunnable}
import org.oliverlittle.clusterprocess.connector.cassandra.CassandraConnector

import io.grpc.{ServerBuilder, ManagedChannelBuilder}
import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}
import io.grpc.protobuf.StatusProto
import com.google.rpc.{Status, Code}
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Failure}
import scala.util.Properties.{envOrElse, envOrNone}
import scala.jdk.CollectionConverters._
import com.typesafe.config.ConfigFactory
import scala.util.Try

object ClientQueryServer {
    private val logger = LoggerFactory.getLogger(classOf[ClientQueryServer].getName)
    private val port = 50051

    def main(): Unit = {
        // K8s specific environment variables
        val numWorkers = envOrNone("NUM_WORKERS")
        val nodeName = envOrNone("WORKER_NODE_NAME")
        val baseURL = envOrNone("WORKER_SERVICE_URL")
        val workerPort = envOrElse("WORKER_PORT", "50052").toInt

        if !(numWorkers.isDefined && nodeName.isDefined && baseURL.isDefined) then logger.warn("Missing environment variables (NUM_WORKERS, WORKER_NODE_NAME, WORKER_SERVICE_URL) to initialise orchestrator, defaulting to application.conf/cluster-process.test.worker-urls")

        val workerAddresses : Seq[(String, Int)] = 
            if numWorkers.isDefined then (0 until numWorkers.get.toInt).map(num => (nodeName.get + "-" + num.toString + "." + baseURL.get, workerPort))
            else ConfigFactory.load.getStringList("clusterprocess.test.worker_urls").asScala.toSeq.map(_.split(":")).collect {case Array(host, port) => (host, port.toInt)}
        val channels = workerAddresses.map((host, port) => BaseChannelManager(host, port))
        val server = new ClientQueryServer(channels)(using ExecutionContext.global)
        server.blockUntilShutdown()
    }
}

class ClientQueryServer(channels : Seq[ChannelManager])(using executionContext : ExecutionContext) {
    private val server =  ServerBuilder.forPort(ClientQueryServer.port).addService(client_query.TableClientServiceGrpc.bindService(new ClientQueryServicer(new WorkerHandler(channels)), executionContext)).build.start
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

            // Attempt to construct the table
            Try{Table.fromProtobuf(request.table.get)} match {
                // Table created successfully
                case Success(table) => 
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
                    WorkExecutionScheduler.startExecution(calculateQueryPlan, workerHandler).flatMap { _ =>
                            ClientQueryServer.logger.info("Result ready from workers, pulling data")
                            table.assembler match {
                                case d : DefaultAssembler => ResultAssembler.startExecution(table, workerHandler.channels, serverCallStreamObserver)   
                                case a => CustomResultAssembler.startExecution(table, table.assembler, workerHandler.channels, serverCallStreamObserver)
                            }
                                    
                    }.onComplete {queryPlanTry =>
                        ClientQueryServer.logger.info("Completed query, cleaning up")
                        // No matter what, pop from the cache to clean-up
                        Future.sequence(workerHandler.channels.map(_.workerComputeServiceStub.clearCache(worker_query.ClearCacheRequest())))
                        queryPlanTry match {
                            // Do nothing on success
                            case Success(_) => 
                            // Report the error on failure
                            case Failure(e) => 
                                // Build a status exception for unknown error
                                val status = Status.newBuilder
                                    .setCode(Code.UNKNOWN.getNumber)
                                    .setMessage(e.getMessage)
                                    .build()
                                responseObserver.onError(StatusProto.toStatusRuntimeException(status))  
                        } 
                    } 

                // Table creation failed
                case Failure(e) => 
                    // Build a status exception for Illegal Argument
                    val status = Status.newBuilder
                        .setCode(Code.INVALID_ARGUMENT.getNumber)
                        .setMessage(e.getMessage)
                        .build()
                    responseObserver.onError(StatusProto.toStatusRuntimeException(status))
            }
        } 
    }
}
