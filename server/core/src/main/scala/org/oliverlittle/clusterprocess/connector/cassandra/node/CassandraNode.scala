package org.oliverlittle.clusterprocess.connector.cassandra.node

import org.oliverlittle.clusterprocess.connector.cassandra.token.CassandraTokenRange

import com.datastax.oss.driver.api.core.metadata.Node
import com.datastax.oss.driver.api.core.metadata.{TokenMap, Metadata}

import java.net.InetSocketAddress
import scala.jdk.CollectionConverters._
import scala.util.Try

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
		* @param chunkSizeMB The chunk size to ensure each token range is around as large as
		* @param tokenMap A TokenMap instance
		* @return A seq of token ranges, split if they were larger than the chunk size
		*/
	def splitForFullSize(fullSizeMB : Double, chunkSizeMB : Double, tokenMap : TokenMap) : Seq[CassandraTokenRange] = primaryTokenRange.toSeq.flatMap(tokenRange => tokenRange.splitForFullSize(fullSizeMB, chunkSizeMB, tokenMap))

	/**
		* Adapted from:
		*	https://stackoverflow.com/questions/28423154/how-can-i-implement-partial-reduce-in-scala
		*
		* @param fullSizeMB The full size of the ring
		* @param chunkSizeMB The chunk size to ensure each token range is around as large as
		* @param tokenMap A TokenMap instance
		* @return A seq of token ranges, joined if they were smaller than the chunk size and they intersected
		*/
	def joinForFullSize(fullSizeMB : Double, chunkSizeMB : Double, tokenMap : TokenMap) : Seq[CassandraTokenRange] = primaryTokenRange.toSeq.sorted.foldLeft(List[CassandraTokenRange]()) {
    (rs, s) => 
			// Check if the head element is smaller than the chunk size, and if it intersects with the next element
      if (rs.nonEmpty && rs.head.percentageOfFullRing * fullSizeMB < chunkSizeMB) 
				// If it does, merge them
        Try{rs.head.mergeWith(tokenMap, s) :: rs.tail}.getOrElse(s :: rs)
      else 
        s :: rs
  }.reverse

	/**
		* Performs a join, then splits for a given full size of a ring
		* This ensures that any contiguous token ranges are as close to the chunk size as possible
		* Non-contiguous token ranges will remain as they were in the primary range
		*
		* @param fullSizeMB The full size of the ring
		* @param chunkSizeMB The chunk size to aim for
		* @param tokenMap A TokenMap instance
		* @return A seq of token ranges, each around the chunk size
		*/
	def joinAndSplitForFullSize(fullSizeMB : Double, chunkSizeMB : Double, tokenMap : TokenMap) : Seq[CassandraTokenRange] = joinForFullSize(fullSizeMB, chunkSizeMB, tokenMap).flatMap(tokenRange => tokenRange.splitForFullSize(fullSizeMB, chunkSizeMB, tokenMap))
}

