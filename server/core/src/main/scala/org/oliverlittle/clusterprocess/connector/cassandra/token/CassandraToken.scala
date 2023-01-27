package org.oliverlittle.clusterprocess.connector.cassandra.token

import org.oliverlittle.clusterprocess.data_source

import com.datastax.oss.driver.api.core.metadata._
import com.datastax.oss.driver.api.core.metadata.token._
import scala.jdk.CollectionConverters._

object CassandraToken {
    val MAX_TOKEN : BigInt = BigInt(Long.MaxValue)
    val MIN_TOKEN : BigInt = BigInt(Long.MinValue)
    val NUM_TOKENS : BigInt = MAX_TOKEN - MIN_TOKEN

    def fromToken(tokenMap : TokenMap, token : Token) : CassandraToken = CassandraToken(tokenMap, tokenMap.format(token).toLong)
}

case class CassandraToken(tokenMap : TokenMap, token : Long) {
    lazy val toBigInt : BigInt = BigInt(token)
    lazy val toToken : Token = tokenMap.parse(token.toString)
    lazy val toTokenString : String = token.toString

    implicit def toLong : Long = token
}

object CassandraTokenRange {
    def fromLong(tokenMap : TokenMap, start : Long, end : Long) = CassandraTokenRange(tokenMap, CassandraToken(tokenMap, start), CassandraToken(tokenMap, end))
    def fromString(tokenMap : TokenMap, start : String, end : String) = fromLong(tokenMap, start.toLong, end.toLong)
    def fromToken(tokenMap : TokenMap, start : Token, end : Token) : CassandraTokenRange = CassandraTokenRange(tokenMap, CassandraToken.fromToken(tokenMap, start), CassandraToken.fromToken(tokenMap, end))
    def fromTokenRange(tokenMap : TokenMap, range : TokenRange) : CassandraTokenRange = fromToken(tokenMap, range.getStart, range.getEnd)
}

case class CassandraTokenRange(tokenMap : TokenMap, start : CassandraToken, end : CassandraToken) {
    lazy val percentageOfFullRing : Double = (((end.toBigInt - start.toBigInt) % CassandraToken.MAX_TOKEN) / CassandraToken.NUM_TOKENS).toDouble
    lazy val toTokenRange : TokenRange = tokenMap.newTokenRange(start.toToken, end.toToken)
    lazy val protobuf : data_source.CassandraTokenRange = data_source.CassandraTokenRange(start=start.toLong, end=end.toLong)

    def toQueryString(partitionKeyString : String) = "token(" + partitionKeyString + ") > " + start + " AND token(" + partitionKeyString + ") <= " + end

    def mergeWith(that : CassandraTokenRange) = CassandraTokenRange.fromTokenRange(tokenMap, toTokenRange.mergeWith(that.toTokenRange))
    def intersects(that : CassandraTokenRange) : Boolean = toTokenRange.intersects(that.toTokenRange)
    def intersectWith(that : CassandraTokenRange) : Iterable[CassandraTokenRange] = toTokenRange.intersectWith(that.toTokenRange).asScala.map(range => CassandraTokenRange.fromTokenRange(tokenMap, range))
    def splitEvenly(numSplits : Int) : Iterable[CassandraTokenRange] = toTokenRange.splitEvenly(numSplits).asScala.map(range => CassandraTokenRange.fromTokenRange(tokenMap, range))

    /**
      * Splits this token range based on a given full size of the ring to ensure that each new range is at least smaller than a given chunk size.
      *
      * @param fullSizeMB The size of the data in MB for the full token range
      * @param chunkSizeMB The chunk size to ensure each token range is smaller than
      * @return A set of token ranges, each part of the original, and each smaller than the chunk size provided
      */
    def splitForFullSize(fullSizeMB : Double, chunkSizeMB : Double) : Seq[CassandraTokenRange] = ((fullSizeMB * percentageOfFullRing) / chunkSizeMB).abs match {
        case x if x > 1 => splitEvenly(x.ceil.toInt).toSeq
        case x => Seq(this)
    }
}