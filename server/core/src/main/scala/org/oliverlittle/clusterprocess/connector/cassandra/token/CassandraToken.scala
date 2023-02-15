package org.oliverlittle.clusterprocess.connector.cassandra.token

import org.oliverlittle.clusterprocess.data_source
import org.oliverlittle.clusterprocess.util.PartialFold

import com.datastax.oss.driver.api.core.metadata._
import com.datastax.oss.driver.api.core.metadata.token._
import scala.util.Try
import scala.jdk.CollectionConverters._

object CassandraToken {
    val MAX_TOKEN : BigInt = BigInt(Long.MaxValue)
    val MIN_TOKEN : BigInt = BigInt(Long.MinValue)
    val NUM_TOKENS : BigInt = MAX_TOKEN - MIN_TOKEN

    def fromToken(tokenMap : TokenMap, token : Token) : CassandraToken = CassandraToken(tokenMap.format(token).toLong)
}

case class CassandraToken(token : Long) extends Ordered[CassandraToken] {
    lazy val toBigInt : BigInt = BigInt(token)
    def toToken(tokenMap : TokenMap) : Token = tokenMap.parse(token.toString)
    lazy val toTokenString : String = token.toString

    implicit def toLong : Long = token

    def compare(that: CassandraToken): Int = token compare that.token
}

// Convert this to a trait and create a R
object CassandraTokenRange {
    val FullRing = CassandraTokenRange(CassandraToken(Long.MinValue), CassandraToken(Long.MinValue))

    def fromLong(start : Long, end : Long) = CassandraTokenRange(CassandraToken(start), CassandraToken(end))
    def fromString(start : String, end : String) = fromLong(start.toLong, end.toLong)
    def fromToken(tokenMap : TokenMap, start : Token, end : Token) : CassandraTokenRange = CassandraTokenRange(CassandraToken.fromToken(tokenMap, start), CassandraToken.fromToken(tokenMap, end))
    def fromTokenRange(tokenMap : TokenMap, range : TokenRange) : CassandraTokenRange = CassandraTokenRange(CassandraToken.fromToken(tokenMap, range.getStart), CassandraToken.fromToken(tokenMap, range.getEnd))
}

case class CassandraTokenRange(start : CassandraToken, end : CassandraToken) extends Ordered[CassandraTokenRange] {
    lazy val percentageOfFullRing : Double = (((BigDecimal(end.toLong) - BigDecimal(start.toLong)) % BigDecimal(CassandraToken.MAX_TOKEN)) / BigDecimal(CassandraToken.NUM_TOKENS)).toDouble
    lazy val protobuf : data_source.CassandraTokenRange = data_source.CassandraTokenRange(start=start.toLong, end=end.toLong)
    def toTokenRange(tokenMap : TokenMap) : TokenRange = tokenMap.newTokenRange(start.toToken(tokenMap), end.toToken(tokenMap))

    def toQueryString(partitionKeyString : String) = "(token(" + partitionKeyString + ") > " + start.toTokenString + " AND token(" + partitionKeyString + ") <= " + end.toTokenString + ")"

    def mergeWith(tokenMap : TokenMap, that : CassandraTokenRange) = CassandraTokenRange.fromTokenRange(tokenMap, toTokenRange(tokenMap).mergeWith(that.toTokenRange(tokenMap)))
    def intersects(tokenMap : TokenMap, that : CassandraTokenRange) : Boolean = toTokenRange(tokenMap).intersects(that.toTokenRange(tokenMap))
    def intersectWith(tokenMap : TokenMap, that : CassandraTokenRange) : Iterable[CassandraTokenRange] = toTokenRange(tokenMap).intersectWith(that.toTokenRange(tokenMap)).asScala.map(range => CassandraTokenRange.fromTokenRange(tokenMap, range))
    def splitEvenly(tokenMap : TokenMap, numSplits : Int) : Iterable[CassandraTokenRange] = toTokenRange(tokenMap).splitEvenly(numSplits).asScala.map(range => CassandraTokenRange.fromTokenRange(tokenMap, range))
    def unwrap(tokenMap : TokenMap) : Iterable[CassandraTokenRange] = toTokenRange(tokenMap).unwrap.asScala.map(range => CassandraTokenRange.fromTokenRange(tokenMap, range))

    /**
      * Splits this token range based on a given full size of the ring to ensure that each new range is at least smaller than a given chunk size.
      *
      * @param fullSizeMB The size of the data in MB for the full token range
      * @param chunkSizeMB The chunk size to ensure each token range is smaller than
      * @return A set of token ranges, each part of the original, and each smaller than the chunk size provided
      */
    def splitForFullSize(fullSizeMB : Double, chunkSizeMB : Double, tokenMap : TokenMap) : Seq[CassandraTokenRange] = ((fullSizeMB * percentageOfFullRing) / chunkSizeMB).abs match {
        case x if x > 1 => splitEvenly(tokenMap, x.ceil.toInt).toSeq
        case x => Seq(this)
    }

    def compare(that: CassandraTokenRange): Int = start.token compare that.start.token match {
        case 0 => end.token compare that.end.token
        case x => x
    }
}

object CassandraPartition {
    /**
		*  Joins contiguous token ranges if they are smaller than the chunk size
		*
		* @param fullSizeMB The full size of the ring
		* @param chunkSizeMB The chunk size to ensure each token range is around as large as
		* @param tokenMap A TokenMap instance
		* @return A seq of token ranges, joined if they were smaller than the chunk size and they intersected
		*/
	def joinForFullSize(tokenRange : Seq[CassandraTokenRange], fullSizeMB : Double, chunkSizeMB : Double, tokenMap : TokenMap) : Seq[CassandraPartition] = PartialFold.partialFold(
		// Firstly, try to join any contiguous ranges, then convert to partitions and sort
		PartialFold.partialFold(
			tokenRange.toSeq.sorted,
			l => l.nonEmpty && l.head.percentageOfFullRing * fullSizeMB < chunkSizeMB,
			(list, item) => Try{list.head.mergeWith(tokenMap, item) :: list.tail}.getOrElse(item :: list)
		).map(t => CassandraPartition(Seq(t))).sortBy(_.percentageOfFullRing),
		// Join any partitions which are smaller than the chunk size into a larger partition
		l => l.nonEmpty && l.head.percentageOfFullRing * fullSizeMB < chunkSizeMB,
		(list, item) => (list.head ++ item) :: list.tail
	)
}

case class CassandraPartition(ranges : Seq[CassandraTokenRange]) {
    lazy val protobuf : Seq[data_source.CassandraTokenRange] = ranges.map(_.protobuf)
    lazy val percentageOfFullRing : Double = ranges.map(_.percentageOfFullRing).sum

    def toQueryString(partitionKeyString : String) : String = ranges.map(_.toQueryString(partitionKeyString)).reduce((l, r) => l + " OR " + r)

    def sizeInMB(fullSizeMB : Double) = fullSizeMB * percentageOfFullRing

    def ++(that : CassandraPartition) : CassandraPartition = copy(ranges=ranges ++ that.ranges)
}