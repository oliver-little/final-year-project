package org.oliverlittle.clusterprocess.connector.grpc

import org.oliverlittle.clusterprocess.worker_query

import io.grpc.{ManagedChannelBuilder, ManagedChannel}

/**
  * Wrapper object for managing a connection to a gRPC connection
  *
  * @param host
  * @param port
  */
case class ChannelManager(host : String, port : Int) {
    val url : String = host + ":" + port.toString
    val channel : ManagedChannel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build

    /**
      * Creates a new ChannelManager with the same parameters (effectively resets the connection)
      *
      * @return A fresh ChannelManager instance
      */
    def newInstance : ChannelManager = {
        if !channel.isShutdown then channel.shutdown
        return new ChannelManager(host, port)
    }

    def workerComputeServiceBlockingStub() : worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceBlockingStub = worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceBlockingStub(channel)
    def workerComputeServiceStub() : worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceStub = worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceStub(channel)

    override def toString = "ChannelManager: " + host + ":" + port.toString
}