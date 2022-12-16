package org.oliverlittle.clusterprocess.server

import io.grpc.ServerBuilder
import scala.concurrent.{ExecutionContext, Future}
import java.util.logging.Logger

import org.oliverlittle.clusterprocess.client_query.{TableClientServiceGrpc, ComputeTableRequest, ComputeTableResult}

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
            ClientQueryServer.logger.info("received files")
            ClientQueryServer.logger.info(request.toString())
            val response = ComputeTableResult(uuid="sample data")
            Future.successful(response)
        }
    }
}
