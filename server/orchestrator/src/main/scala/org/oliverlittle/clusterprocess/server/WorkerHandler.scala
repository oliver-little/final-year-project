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

        val chunkSize : Int = ConfigFactory.load().getString("clusterprocess.chunk.chunk_size_mb").toInt

        // Sense check
        val totalPercentage = channelMap.keys.map(_.percentageOfFullRing).sum
        if (totalPercentage - 1).abs <= 0.01 then logger.info("Nodes do not cover entire ring: " + totalPercentage.toString + "% covered.")

        // This is unfinished
        // For each channelMap entry, need to calculate the tokenRanges based on their percentage of the full ring, and do any splitting that is required (according to chunkSize)
        // Then, reformat the list so that the output is pairs of list of possible channels to send to, and a list of chunked token ranges
        // An empty list signifies that there is no data locality, and the chunk can be sent to any node
        val assignedWork : Seq[(Seq[ChannelManager], Seq[CassandraTokenRange])] = for {
            (node, matchedChannels) <- channelMap.toSeq
            tokenRange <- node.primaryTokenRange
            splitRanges <- tokenRange.splitEvenly(((tokenRange.percentageOfFullRing * sizeEstimator.estimatedTableSizeMB) / chunkSize).ceil.toInt.max(1)).toSeq
        }
        yield (matchedChannels, splitRanges)

        return assignedWork.groupBy(_._1).map
    }

    def getChannelMapping(channelManagers : Seq[ChannelManager]) : Map[CassandraNode, Seq[ChannelManager]] = {
        val metadata = CassandraConnector.getSession.getMetadata
        val cassandraNodes : Seq[CassandraNode] = CassandraNode.getNodes(metadata)
        // For each channel, get the local cassandra address and store it as a Map
        val channelCassandraAddresses : Seq[(InetSocketAddress, ChannelManager)] = channelManagers.map(channel => (getWorkerCassandraAddress(channel), channel))
        // For each known Cassandra node, try to match it up to a channel by comparing addresses
        val channelMap : Map[CassandraNode, Seq[ChannelManager]] = cassandraNodes.map(node => 
            node -> channelCassandraAddresses
                // Find any channels, where the cassandra node address matches (can't just use get as we have to compare multiple addresses)
                .filter((address, channel) => node.compareAddress(address))
                // Then extract the channel element for the Map
                .flatMap((address, channel) => channel))

        channelMap.foreach((node, channels) => logger.info("Cassandra node: " + node.getAddressAsString + " has workers " + channels.toString))

        return channelMap
    }
}