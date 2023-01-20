package org.oliverlittle.clusterprocess.server

import org.oliverlittle.clusterprocess.worker_query
import org.oliverlittle.clusterprocess.data_source
import org.oliverlittle.clusterprocess.connector.grpc.ChannelManager
import org.oliverlittle.clusterprocess.connector.cassandra.CassandraConnector
import org.oliverlittle.clusterprocess.util.ByteBufferOperations

import com.datastax.oss.driver.api.core.cql.ResultSet
import com.datastax.oss.driver.api.core.metadata._
import com.datastax.oss.driver.api.core.metadata.token._
import java.net.InetSocketAddress
import java.util.logging.Logger
import collection.JavaConverters._

class WorkerHandler(workerAddresses : Seq[(String, Int)]) {
    private val logger = Logger.getLogger(classOf[WorkerHandler].getName)
    
    val channels : Seq[ChannelManager] = workerAddresses.map((host, port) => ChannelManager(host, port))

    private val metadata = CassandraConnector.getSession.getMetadata
    val cassandraNodes : Seq[Option[Node]] = channels.map(getCassandraNodeForWorker(_, metadata))
    // Maps Cassandra Nodes to gRPC channels, but remove any that we couldn't find a node for
    val channelMap : Map[Node, ChannelManager] = (cassandraNodes zip channels).collect({
        case (Some(node), channel) => (node, channel)
    }).toMap

    logger.info(f"Opened up channels with " + workerAddresses.length.toString + " workers.")
    workerAddresses.zip(cassandraNodes).foreach((address, node) => node match {
        case Some(x) => logger.info("Worker " + address._1 + ":" + address._2.toString + " has local Cassandra node " + x.getBroadcastRpcAddress.get.getHostName + ":" + x.getBroadcastRpcAddress.get.getPort)
        case None => logger.info("Worker " + address._1 + ":" + address._2.toString + " could not be matched to a local Cassandra node")
    })

    /**
      * Given a gRPC channel, finds the corresponding Cassandra Node if it exists
      *
      * @param manager The gRPC channel to use
      * @param metadata The metadata instance to use
      * @return A Cassandra Node, or None if it does not exist
      *
      */
    private def getCassandraNodeForWorker(manager : ChannelManager, metadata : Metadata) : Option[Node] = {
        val result = worker_query.WorkerComputeServiceGrpc.blockingStub(manager.channel).getLocalCassandraNode(worker_query.GetLocalCassandraNodeRequest()).address
        return findCassandraNodeFromAddress(new InetSocketAddress(result.get.host, result.get.port), metadata)
    }

    /**
      * A more comprehensive findNode, which searches nodes more thoroughly to find any matches (in particular handles localhost a little better)
      *
      * @param address The address to search
      * @param metadata The metadata instance to use
      * @return A Cassandra Node
      */
    private def findCassandraNodeFromAddress(address : InetSocketAddress, metadata : Metadata) : Option[Node] = metadata.getNodes.asScala.values.find(node => node.getBroadcastRpcAddress.equals(address) || node.getEndPoint.resolve.equals(address))

    /**
      * Calculates the TokenRange for a Cassandra Node
      *
      * @param nodes The Cassandra Node to calculate the TokenRange of
      * @param metadata The metadata instance to use
      * @return A TokenRange for that CassandraNode
      */
    private def getTokenRangesForCassandraNodes(node : Node, metadata : Metadata) : TokenRange = metadata.getTokenMap.get.getTokenRanges(node).asScala.reduce((l, r) => l mergeWith r)

    // This is intended to eventually return a Map of ChannelManager -> Seq[TokenRange] pairs
    // Each of these token ranges should be roughly equal size, and be one unit of work to that complete
    // Going to assume for now that the entire token range is covered by our worker discovery, but it's fairly trivial to
    // Merge all the token ranges together and use isFullRing to check if we have the entire set
    // (Use metadata.getAllNodes to discover what we're missing if that's the case)
    def distributeWorkToNodes(keyspace : String, table : String) : Map[Option[ChannelManager], Long] = {
        val session = CassandraConnector.getSession
        val metadata = session.getMetadata
        val tokenRangesByChannel : Seq[(TokenRange, ChannelManager)] = channelMap.map((node, channel) => (getTokenRangesForCassandraNodes(node, metadata), channel)).toSeq
        val tokenMap = metadata.getTokenMap.get

        // Sense check, do we have the full token range covered with our current TokenRanges
        if !tokenRangesByChannel.map(_._1).reduce(_ mergeWith _).isFullRing then logger.info("Colocated node ring for workers is not complete, job will not be able to be entirely locally assigned.")

        val res = session.execute("SELECT * FROM system.size_estimates WHERE keyspace_name=? AND table_name=?", keyspace, table)

        // Currently this is not super accurate, as this is only the picture from the node that was queried
        val channelDataMapping = 
            for {
                row <- res.iterator.asScala // Iterate over all rows
                tokenRange <- Seq(tokenMap.newTokenRange(tokenMap.parse(row.getString("range_start")), tokenMap.parse(row.getString("range_end")))) // Generate a tokenRange covered by the row (wrapped in a Seq to satisfy flatMap requirements)
                totalData <- Seq(row.getLong("mean_partition_size") * row.getLong("partitions_count")) // Generate the amount of data for this tokenRange 
                matchedChannel <- tokenRangesByChannel
                    // Filter on tokenRange for each channel to find any that intersect with the current tokenRange
                    .filter((channelTokenRange, channel) => channelTokenRange.intersects(tokenRange))
                    // Remove the tokenRange element and just keep the channel if present
                    // Then, pattern match to find out if there were no matches at all, and create a None element in place in this case
                    .map((channelTokenRange, channel) => Some(channel)) match {
                        case Nil => Seq(None)
                        case x => x
                    }
            }
            // Return a list of combinations of channel and data stored in that channel. If a TokenRange bridges multiple nodes (which it shouldn't)
            // Then this will increase the data estimate, but that shouldn't cause much of a problem
            yield (matchedChannel, totalData)

        // The result we have is a sequence of (Channel, Data (MB)) mappings, so we now aggregate this by Channel to get the estimate of data stored in each channel
        val dataPerChannel = channelDataMapping.toSeq.groupBy(_._1).mapValues(_.map(_._2).reduce(_ + _)).toMap

        return dataPerChannel
    }
}

class CassandraTokenRangeAnalyser(metadata : Metadata) {

}