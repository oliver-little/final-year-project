package org.oliverlittle.clusterprocess.connector.grpc

import io.grpc.{ManagedChannelBuilder, ManagedChannel}

/**
  * Wrapper object for managing a connection to a gRPC connection
  *
  * @param host
  * @param port
  */
case class ChannelManager(host : String, port : Int) {
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

    override def toString = "ChannelManager: " + host + ":" + port.toString
}