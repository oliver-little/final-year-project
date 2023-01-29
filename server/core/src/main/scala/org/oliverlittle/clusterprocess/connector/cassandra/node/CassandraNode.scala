package org.oliverlittle.clusterprocess.connector.cassandra.node

import org.oliverlittle.clusterprocess.connector.cassandra.token.CassandraTokenRange

import java.net.InetSocketAddress
import scala.jdk.CollectionConverters._
import com.datastax.oss.driver.api.core.metadata.Node
import com.datastax.oss.driver.api.core.metadata.{TokenMap, Metadata}

object CassandraNode {
    def getNodes(metadata : Metadata) : Seq[CassandraNode] = metadata.getNodes.asScala.values.map(node => 
        CassandraNode(
            node, 
            metadata.getTokenMap.get.getTokenRanges(node).asScala.map(range => CassandraTokenRange.fromTokenRange(metadata.getTokenMap.get, range)).toSet
        )).toSeq
}

case class CassandraNode(node : Node, primaryTokenRange : Set[CassandraTokenRange]) {

    lazy val getAddressAsString = node.getBroadcastRpcAddress.get.getHostName + ":" + node.getBroadcastRpcAddress.get.getPort.toString
    lazy val percentageOfFullRing = primaryTokenRange.map(_.percentageOfFullRing).sum

    /**
      * Compares a given address to determine if this node is at the same address
      *
      * @param address The address to compare to
      * @return A Cassandra Node
      */
    def compareAddress(address : InetSocketAddress) : Boolean = node.getBroadcastRpcAddress.equals(address) || node.getEndPoint.resolve.equals(address)

    /**
      * Splits this Cassandra node's primary token range according to a given full size of the ring.
      *
      * @param fullSizeMB The full size of the ring
      * @param chunkSizeMB The chunk size to ensure each token range is smaller than
      * @return A set of token ranges, each smaller than the chunk size
      */
    def splitForFullSize(fullSizeMB : Double, chunkSizeMB : Double, tokenMap : TokenMap) : Seq[CassandraTokenRange] = primaryTokenRange.toSeq.map(tokenRange => tokenRange.splitForFullSize(fullSizeMB, chunkSizeMB, tokenMap)).flatten
}