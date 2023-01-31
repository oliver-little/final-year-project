package org.oliverlittle.clusterprocess.connector.cassandra.token

import org.oliverlittle.clusterprocess.data_source
import org.oliverlittle.clusterprocess.UnitSpec

import com.datastax.oss.driver.api.core.metadata.token._
import com.datastax.oss.driver.api.core.metadata.{TokenMap}

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar

import scala.math.pow
import java.util.Arrays

class CassandraTokenTest extends UnitSpec with MockitoSugar {
    "A CassandraToken" should "perform conversions correctly" in {
        CassandraToken(1).toBigInt should be (BigInt(1))
        CassandraToken(1).toTokenString should be ("1")
    }

    it should "compare correctly" in {
        CassandraToken(1).compare(CassandraToken(2)) should be (-1)
        CassandraToken(2).compare(CassandraToken(2)) should be (0)
        CassandraToken(2).compare(CassandraToken(1)) should be (1)
    }
}

class CassandraTokenRangeTest extends UnitSpec with MockitoSugar {
    "A CassandraTokenRange" should "calculate the correct percentage of the full ring" in {
        CassandraTokenRange(CassandraToken(0), CassandraToken(pow(10, 18).toLong)).percentageOfFullRing should be (0.0542d +- 0.01)
    }

    it should "convert to a protobuf correctly" in {
        CassandraTokenRange(CassandraToken(0), CassandraToken(100)).protobuf should be (data_source.CassandraTokenRange(start=0, end=100))
    }

    it should "convert to a CQL query string correctly" in {
        CassandraTokenRange(CassandraToken(0), CassandraToken(100)).toQueryString("a") should be ("token(a) > 0 AND token(a) <= 100")
    }

    it should "perform a split correctly depending on the estimated size" in {
        val mockMap = mock[TokenMap]
        
        val mockToken = mock[Token]
        val mockTokenRange = mock[TokenRange]
        when(mockMap.newTokenRange(any[Token](), any[Token]())).thenReturn(mockTokenRange)
        when(mockMap.parse(any[String]())).thenReturn(mockToken)
        when(mockMap.format(any[Token]())).thenReturn("0")
        when(mockTokenRange.splitEvenly(2)).thenReturn(Arrays.asList(mockTokenRange, mockTokenRange))
        when(mockTokenRange.getStart).thenReturn(mockToken)
        when(mockTokenRange.getEnd).thenReturn(mockToken)

        val noSplitRes = CassandraTokenRange(CassandraToken(0), CassandraToken(pow(10, 18).toLong)).splitForFullSize(100, 10, mockMap) 
        noSplitRes should have length 1
        noSplitRes should be (Seq(CassandraTokenRange(CassandraToken(0), CassandraToken(pow(10, 18).toLong))))
        verify(mockMap, times(0)).newTokenRange

        val splitRes = CassandraTokenRange(CassandraToken(0), CassandraToken(pow(10, 18).toLong)).splitForFullSize(200, 10, mockMap) 
        splitRes should have length 2
        splitRes should be (Seq(CassandraTokenRange(CassandraToken(0), CassandraToken(0)), CassandraTokenRange(CassandraToken(0), CassandraToken(0))))
    }

    it should "compare tokenRanges correctly" in {
        CassandraTokenRange(CassandraToken(1), CassandraToken(1)).compare(CassandraTokenRange(CassandraToken(0), CassandraToken(1))) should be (1)
        CassandraTokenRange(CassandraToken(-1), CassandraToken(1)).compare(CassandraTokenRange(CassandraToken(0), CassandraToken(1))) should be (-1)
        CassandraTokenRange(CassandraToken(0), CassandraToken(1)).compare(CassandraTokenRange(CassandraToken(0), CassandraToken(1))) should be (0)
        CassandraTokenRange(CassandraToken(0), CassandraToken(1)).compare(CassandraTokenRange(CassandraToken(0), CassandraToken(2))) should be (-1)
        CassandraTokenRange(CassandraToken(0), CassandraToken(1)).compare(CassandraTokenRange(CassandraToken(0), CassandraToken(0))) should be (1)
    }
}