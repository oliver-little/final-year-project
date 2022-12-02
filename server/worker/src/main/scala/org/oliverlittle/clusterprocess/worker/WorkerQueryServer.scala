package org.oliverlittle.clusterprocess.worker

import io.grpc.ServerBuilder
import scala.concurrent.{ExecutionContext, Future}
import java.util.logging.Logger

import org.oliverlittle.clusterprocess.worker_query.{WorkerComputeServiceGrpc, ComputePartialResultCassandraRequest, ComputePartialResultCassandraResult}


object WorkerQueryServer {
    private val logger = Logger.getLogger(classOf[WorkerQueryServer].getName)
    private val port = 50051

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
        override def computePartialResultCassandra(request: ComputePartialResultCassandraRequest): Future[ComputePartialResultCassandraResult] = {
            WorkerQueryServer.logger.info("received files")
            WorkerQueryServer.logger.info(request.toString)
            val response = ComputePartialResultCassandraResult(success=true)
            Future.successful(response)
        }
    }
}
