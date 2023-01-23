package org.oliverlittle.clusterprocess.server

import org.oliverlittle.clusterprocess.worker_query
import org.oliverlittle.clusterprocess.data_source
import org.oliverlittle.clusterprocess.connector.grpc.ChannelManager
import org.oliverlittle.clusterprocess.connector.cassandra.CassandraConnector
import org.oliverlittle.clusterprocess.connector.cassandra.node.CassandraNode
import org.oliverlittle.clusterprocess.connector.cassandra.token._
import org.oliverlittle.clusterprocess.connector.cassandra.size_estimation.TableSizeEstimation

import com.datastax.oss.driver.api.core.cql.ResultSet
import com.datastax.oss.driver.api.core.metadata._
import com.datastax.oss.driver.api.core.metadata.token._
import java.net.InetSocketAddress
import scala.jdk.CollectionConverters._
import java.util.logging.Logger
import com.typesafe.config.ConfigFactory

class WorkerHandler(workerAddresses : Seq[(String, Int)]) {
    private val logger = Logger.getLogger(classOf[WorkerHandler].getName)
    
    // Setup channels and get nodes
    val channels : Seq[ChannelManager] = workerAddresses.map((host, port) => ChannelManager(host, port))

    logger.info(f"Opened up channels with " + workerAddresses.length.toString + " workers.")

    /**
      * Given a gRPC channel, finds the address of the Cassandra node it is connected to
      *
      * @param manager The gRPC channel to use
      * @return A Cassandra Node, or None if it does not exist
      *
      */
    private def getWorkerCassandraAddress(manager : ChannelManager) : InetSocketAddress = {
        val result = worker_query.WorkerComputeServiceGrpc.blockingStub(manager.channel).getLocalCassandraNode(worker_query.GetLocalCassandraNodeRequest()).address
        return new InetSocketAddress(result.get.host, result.get.port)
    }

    def distributeWorkToNodes(keyspace : String, table : String) : Seq[(Seq[ChannelManager], Seq[CassandraTokenRange])] = {
        val session = CassandraConnector.getSession
        val metadata = session.getMetadata
        val sizeEstimator = TableSizeEstimation.estimateTableSize(session, keyspace, table)
        val channelMap = getChannelMapping(channels)

        val chunkSize : Int = ConfigFactory.load.getString("clusterprocess.chunk.chunk_size_mb").toInt

        // Sense check
        val totalPercentage = channelMap.map(_._1.percentageOfFullRing).sum
        if (totalPercentage - 1).abs <= 0.01 then logger.info("Nodes do not cover entire ring: " + totalPercentage.toString + "% covered.")

        // For each node, split the token range to be small enough to fit the chunk size
        return channelMap.map((node, matchedChannels) => (matchedChannels, node.splitForFullSize(sizeEstimator.estimatedTableSizeMB, chunkSize)))
    }

    /**
      * Returns the matching CassandraNodes for each ChannelManager
      *
      * @param channelManagers The ChannelManagers to compare
      * @return A sequence of (CassandraNode, Seq[ChannelManager]) pairs, showing which ChannelManagers are colocated with a given CassandraNode
      */
    def getChannelMapping(channelManagers : Seq[ChannelManager]) : Seq[(CassandraNode, Seq[ChannelManager])] = {
        val metadata = CassandraConnector.getSession.getMetadata
        val cassandraNodes : Seq[CassandraNode] = CassandraNode.getNodes(metadata)
        // For each known Cassandra node, try to match it up to a channel by comparing addresses
        val channelMap : Seq[(CassandraNode, Seq[ChannelManager])] = cassandraNodes.map(node => 
            (node, 
            // For each channel, get the local cassandra address and store it as a map
            channelManagers.map(channel => (getWorkerCassandraAddress(channel), channel))
                // Find any channels where the cassandra node address matches (can't just use get as we have to compare multiple addresses)
                .filter((address, channel) => node.compareAddress(address))
                // Then extract the channel element for the Map
                .map(_._2)))

        channelMap.foreach((node, channels) => logger.info("Cassandra node: " + node.getAddressAsString + " has workers " + channels.toString))

        return channelMap
    }
}