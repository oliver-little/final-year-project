package org.oliverlittle.clusterprocess.server

import org.oliverlittle.clusterprocess.worker_query
import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.client_query
import org.oliverlittle.clusterprocess.model.table.sources.DataSource
import org.oliverlittle.clusterprocess.model.table.sources.cassandra.CassandraDataSource
import org.oliverlittle.clusterprocess.model.table.{Table, TableTransformation, TableResult}
import org.oliverlittle.clusterprocess.scheduler.WorkExecutionScheduler
import org.oliverlittle.clusterprocess.connector.grpc.StreamedTableResult

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
        val server = new ClientQueryServer(ExecutionContext.global, workerAddresses)
        server.blockUntilShutdown()
    }
}

class ClientQueryServer(executionContext: ExecutionContext, workerAddresses : Seq[(String, Int)]) {
    private val server =  ServerBuilder.forPort(ClientQueryServer.port).addService(client_query.TableClientServiceGrpc.bindService(new ClientQueryServicer(new WorkerHandler(workerAddresses)), executionContext)).build.start
    ClientQueryServer.logger.info("gRPC Server started, listening on " + ClientQueryServer.port)
    
    sys.addShutdownHook({
        ClientQueryServer.logger.info("*** Shutting down gRPC server since JVM is shutting down.")
        this.stop()
        ClientQueryServer.logger.info("*** gRPC server shut down.")
    })

    private def stop() : Unit = server.shutdown()

    private def blockUntilShutdown(): Unit = server.awaitTermination()

    private class ClientQueryServicer(workerHandler : WorkerHandler) extends client_query.TableClientServiceGrpc.TableClientService {
        override def computeTable(request: client_query.ComputeTableRequest, responseObserver : StreamObserver[table_model.StreamedTableResult]): Unit = {
            ClientQueryServer.logger.info("Compute table request received")

            // Parse table
            val dataSource = DataSource.fromProtobuf(request.dataSource.get)
            val tableTransformations = TableTransformation.fromProtobuf(request.table.get)
            val table = Table(dataSource, tableTransformations)
            ClientQueryServer.logger.info("Created table instance.")
            
            if !table.isValid then responseObserver.onError(new IllegalArgumentException("Table cannot be computed"))
            
            // Refactor into a subclassed server that specifically handles Cassandra (workerHandler should not be an argument)
            if dataSource.isCassandra then 
                val cassandraDataSource = dataSource.asInstanceOf[CassandraDataSource]
                val channelTokenRangeMap = workerHandler.distributeWorkToNodes(cassandraDataSource.keyspace, cassandraDataSource.name)

                val serverCallStreamObserver = responseObserver.asInstanceOf[ServerCallStreamObserver[table_model.StreamedTableResult]]
                val runnable = OutOfThreadTableResultRunnable(serverCallStreamObserver)
                serverCallStreamObserver.setOnReadyHandler(runnable)

                // Able to make this unchecked cast because this is a response from a server
                WorkExecutionScheduler.startFromData(channelTokenRangeMap, table, {result =>
                    ClientQueryServer.logger.info("Result ready")
                    runnable.setData(result)
                })
            else
                responseObserver.onError(new IllegalArgumentException("Data source is not supported."))
        } 
    }
}

class OutOfThreadTableResultRunnable(responseObserver : ServerCallStreamObserver[table_model.StreamedTableResult]) extends Runnable {
    var data : Option[Iterator[table_model.StreamedTableResult]] = None
    var closed = false;

    private val logger = Logger.getLogger(classOf[OutOfThreadTableResultRunnable].getName)
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
