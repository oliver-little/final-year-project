package org.oliverlittle.clusterprocess.server

import org.oliverlittle.clusterprocess.worker_query
import org.oliverlittle.clusterprocess.data_source
import org.oliverlittle.clusterprocess.connector.grpc.ChannelManager
import org.oliverlittle.clusterprocess.connector.cassandra.CassandraConnector

import com.datastax.oss.driver.api.core.metadata._
import com.datastax.oss.driver.api.core.metadata.token._
import java.net.InetSocketAddress
import java.util.logging.Logger
import collection.JavaConverters._

class WorkerHandler(workerAddresses : Seq[(String, Int)]) {
    private val logger = Logger.getLogger(classOf[WorkerHandler].getName)
    
    val channels : Seq[ChannelManager] = workerAddresses.map((host, port) => ChannelManager(host, port))

    private val metadata = CassandraConnector.getSession.getMetadata
    val cassandraNodes : Seq[Node] = channels.map(getCassandraNodeForWorker(_, metadata))
    // Maps Cassandra Nodes to gRPC channels
    val channelMap : Map[Node, ChannelManager] = (cassandraNodes zip channels).toMap

    logger.info(f"Opened up channels with " + workerAddresses.length.toString + " workers.")
    workerAddresses.zip(cassandraNodes.map(_.getBroadcastRpcAddress.get)).foreach((address, node) => logger.info("Worker " + address._1 + ":" + address._2.toString + " has local Cassandra node " + node.getHostName + ":" + node.getPort))

    private def getCassandraNodeForWorker(manager : ChannelManager, metadata : Metadata) : Node = {
        val result = worker_query.WorkerComputeServiceGrpc.blockingStub(manager.channel).getLocalCassandraNode(worker_query.GetLocalCassandraNodeRequest()).address
        return metadata.findNode(new InetSocketAddress(result.get.host, result.get.port)).get
    }

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
    def distributeWorkToNodes(keyspace : String, table : String) : Unit = {
        val session = CassandraConnector.getSession
        // Something like
        val prep = session.prepare("SELECT * FROM system.size_estimates WHERE keyspace_name='?' AND table_name='?'")
        val res = session.execute(prep.bind(keyspace, table))
        
        // This returns a set of range_start, range_end, mean_partition_size and partitions_count for the table
        // res is a ResultSet
        // Get an iterator to map over using res.iterator.asScala
        // This returns rows, use getLong, getDouble, etc with the column names to get values out
        // Should be able to use this, combined with the TokenRanges returned per node in TokenMap to work out how much data each node owns roughly
    }
}
