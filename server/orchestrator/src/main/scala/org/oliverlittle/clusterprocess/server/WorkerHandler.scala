package org.oliverlittle.clusterprocess.server

import org.oliverlittle.clusterprocess.worker_query
import org.oliverlittle.clusterprocess.data_source
import org.oliverlittle.clusterprocess.connector.grpc.ChannelManager
import java.net.InetSocketAddress
import java.util.logging.Logger

class WorkerHandler(workerAddresses : Seq[(String, Int)]) {
    private val logger = Logger.getLogger(classOf[WorkerHandler].getName)
    
    val channels : Seq[ChannelManager] = workerAddresses.map((host, port) => ChannelManager(host, port))
    val cassandraNodes : Seq[InetSocketAddress] = channels.map(getCassandraNodeForWorker(_))
    // Maps Cassandra Nodes to gRPC channels
    val channelMap : Map[InetSocketAddress, ChannelManager] = (cassandraNodes zip channels).toMap

    logger.info(f"Opened up channels with " + workerAddresses.length.toString + " workers.")
    workerAddresses.zip(cassandraNodes).foreach((address, node) => logger.info("Worker " + address._1 + ":" + address._2.toString + " has local Cassandra node " + node.getHostName + ":" + node.getPort))

    private def getCassandraNodeForWorker(manager : ChannelManager) : InetSocketAddress = {
        val result = worker_query.WorkerComputeServiceGrpc.blockingStub(manager.channel).getLocalCassandraNode(worker_query.GetLocalCassandraNodeRequest()).address
        return new InetSocketAddress(result.get.host, result.get.port)
    }
}
