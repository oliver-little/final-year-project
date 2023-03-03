package org.oliverlittle.clusterprocess.connector.grpc

import org.oliverlittle.clusterprocess.worker_query
import org.oliverlittle.clusterprocess.model.table.Table
import org.oliverlittle.clusterprocess.connector.grpc.{ChannelManager, BaseChannelManager}
import org.oliverlittle.clusterprocess.connector.cassandra.CassandraConnector
import org.oliverlittle.clusterprocess.connector.cassandra.node.CassandraNode
import org.oliverlittle.clusterprocess.connector.cassandra.token._
import org.oliverlittle.clusterprocess.connector.cassandra.size_estimation.TableSizeEstimation

import com.datastax.oss.driver.api.core.cql.ResultSet
import com.datastax.oss.driver.api.core.metadata._
import com.datastax.oss.driver.api.core.metadata.token._
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.net.InetSocketAddress
import com.typesafe.config.ConfigFactory
import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.concurrent.{Future, ExecutionContext}

case class WorkerHandler(channels : Seq[ChannelManager]) {
    private val logger = LoggerFactory.getLogger(classOf[WorkerHandler].getName)
    
    val workerAddresses : Seq[(String, Int)]= channels.map(channel => (channel.host, channel.port))

    logger.info(f"Opened up channels with " + workerAddresses.length.toString + " workers.")

    /**
      * Given a gRPC channel, finds the address of the Cassandra node it is connected to
      *
      * @param manager The gRPC channel to use
      * @return A Cassandra Node, or None if it does not exist
      *
      */
    private def getWorkerCassandraAddress(manager : ChannelManager) : InetSocketAddress = {
        val result = manager.workerComputeServiceBlockingStub.getLocalCassandraNode(worker_query.GetLocalCassandraNodeRequest()).address
        return new InetSocketAddress(result.get.host, result.get.port)
    }

    lazy val chunkSize : Int = ConfigFactory.load.getString("clusterprocess.chunk.chunk_size_mb").toInt

    /**
      * Provides a mapping from ChannelManager to CassandraPartition
      * Essentially this represents the ideal partition allocation based on data locality
      *
      * @param connector A CassandraConnector instance
      * @param keyspace The keyspace of the table to use
      * @param table The table name to use
      * @return A Sequence of (Sequence[ChannelManager], Sequence[CassandraPartition]) pairs, representing ideal allocations
      */
    def distributeWorkToNodes(connector : CassandraConnector, keyspace : String, table : String) : Seq[(Seq[ChannelManager], Seq[CassandraPartition])] = {
        val session = connector.getSession
        val metadata = session.getMetadata
        val tokenMap = metadata.getTokenMap.get
        val sizeEstimator = TableSizeEstimation.estimateTableSize(session, keyspace, table)
        logger.info("Estimated size of " + keyspace + "." + table + " is " + sizeEstimator.estimatedTableSizeMB.toString + "MB.")
        val channelMap = getChannelMapping(channels, metadata)

        // For each node, split the token range to be small enough to fit the chunk size (or if the table is really small, just output the full tokenRange)
        val channelAssignment = channelMap.map((node, matchedChannels) => 
                (matchedChannels, 
                // Split the node's tokenRange as required
                node.joinAndSplitForFullSize(sizeEstimator.estimatedTableSizeMB, chunkSize, tokenMap)
                )
            )
        // Sense check
        val fullRing = Try{
            channelAssignment.map(_._2) // Get partitions
            .flatMap(_.flatMap(_.ranges)) // Get list of token ranges
            .sorted // Sort in order
            .map(_.toTokenRange(tokenMap)) // Convert to DataStax instances
            .reduce(_ mergeWith _).isFullRing // Repeatedly reduce to one range
        }.getOrElse(false) // Try to get the result out, or false if an error occurred
        logger.info("Ring is " + (if !fullRing then "not " else "") + "fully covered.")

        return channelAssignment
    }

    /**
      * Returns the matching CassandraNodes for each ChannelManager
      *
      * @param channelManagers The ChannelManagers to compare
      * @return A sequence of (CassandraNode, Seq[ChannelManager]) pairs, showing which ChannelManagers are colocated with a given CassandraNode
      */
    def getChannelMapping(channelManagers : Seq[ChannelManager], metadata : Metadata) : Seq[(CassandraNode, Seq[ChannelManager])] = {
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

    def getNumPartitionsForTable(table : Table)(using ec : ExecutionContext)  : Future[Int] = getTableStoreEstimatedSizeMB(table).map(size => Math.max(size.toDouble / chunkSize, 1).round.toInt)

    def getTableStoreEstimatedSizeMB(table : Table)(using ec : ExecutionContext) : Future[Long] = {
        val request = worker_query.GetEstimatedTableSizeRequest(Some(table.protobuf))
        Future.sequence(channels.map(_.workerComputeServiceStub.getEstimatedTableSize(request))).map(result => result.map(_.estimatedSizeMb).sum)
    }
}