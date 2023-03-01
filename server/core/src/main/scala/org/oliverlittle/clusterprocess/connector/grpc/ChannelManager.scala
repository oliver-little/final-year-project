package org.oliverlittle.clusterprocess.connector.grpc

import org.oliverlittle.clusterprocess.worker_query

import io.grpc.{ManagedChannelBuilder, ManagedChannel}

trait ChannelManager:
  val host : String
  val port : Int
  val channel : ManagedChannel

  /**
      * Creates a new ChannelManager with the same parameters (effectively resets the connection)
      *
      * @return A fresh ChannelManager instance
      */
    def newInstance : ChannelManager

    def workerComputeServiceBlockingStub : worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceBlockingStub
    def workerComputeServiceStub : worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceStub


/**
  * Wrapper object for managing a connection to a gRPC connection
  *
  * @param host
  * @param port
  */
case class BaseChannelManager(host : String, port : Int) extends ChannelManager {
    val url : String = host + ":" + port.toString
    val channel : ManagedChannel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build

    /**
      * Creates a new ChannelManager with the same parameters (effectively resets the connection)
      *
      * @return A fresh ChannelManager instance
      */
    def newInstance : ChannelManager = {
        if !channel.isShutdown then channel.shutdown
        return new BaseChannelManager(host, port)
    }

    lazy val workerComputeServiceBlockingStub : worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceBlockingStub = worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceBlockingStub(channel)
    lazy val workerComputeServiceStub : worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceStub = worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceStub(channel)

    override def toString = "ChannelManager: " + host + ":" + port.toString
}