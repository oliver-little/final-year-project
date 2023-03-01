package org.oliverlittle.clusterprocess.connector.cassandra.node

import org.oliverlittle.clusterprocess.connector.cassandra.token._
import org.oliverlittle.clusterprocess.UnitSpec

import com.datastax.oss.driver.api.core.metadata.{Node, EndPoint}
import com.datastax.oss.driver.api.core.metadata.{TokenMap, Metadata}

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar

import java.net.InetSocketAddress
import java.util.Optional

class CassandraNodeTest extends UnitSpec with MockitoSugar {
    "A CassandraNode" should "return the correct address as a string" in {
        val mockNode = mock[Node]
        val addr = new InetSocketAddress("localhost", 9000)
        when(mockNode.getBroadcastRpcAddress).thenReturn(Optional.of(addr))

        val node = CassandraNode(mockNode, Set())
        node.getAddressAsString should be ("localhost:9000")
        verify(mockNode, times(2)).getBroadcastRpcAddress
    }
    
    it should "calculate the correct sum of ring percentages" in {
        val mockNode = mock[Node]
        val mockRanges = (1 to 10).map(i => mock[CassandraTokenRange]).toSet
        mockRanges.foreach(r => when(r.percentageOfFullRing).thenReturn(0.1))

        val node = CassandraNode(mockNode, mockRanges)
        node.percentageOfFullRing should be (1d +- 0.01)

        mockRanges.foreach(r => verify(r, times(1)).percentageOfFullRing)
    }

    it should "correctly compare a given address" in {
        val mockNode = mock[Node]
        val addr = new InetSocketAddress("localhost", 9000)
        when(mockNode.getBroadcastRpcAddress).thenReturn(Optional.of(addr))
        val mockEndPoint = mock[EndPoint]
        when(mockNode.getEndPoint).thenReturn(mockEndPoint)
        val altAddress = new InetSocketAddress("0.0.0.0", 9000)
        when(mockEndPoint.resolve).thenReturn(altAddress)

        val node = CassandraNode(mockNode, Set())
        node.compareAddress(new InetSocketAddress("localhost", 9000)) should be (true)
        node.compareAddress(new InetSocketAddress("127.0.0.1", 9000)) should be (true)
        node.compareAddress(new InetSocketAddress("0.0.0.0", 9000)) should be (true)
        node.compareAddress(new InetSocketAddress("192.168.2.6", 9000)) should be (false)
    }

    it should "split the primary token range correctly based on the given full size" in {
        // This test really just verifies that splitForFullSize on each range was called
        val mockNode = mock[Node]
        val mockMap = mock[TokenMap]
        val mockRanges = (1 to 10).map(i => mock[CassandraTokenRange]).toSet
        mockRanges.foreach(r => when(r.splitForFullSize(100, 10, mockMap)).thenReturn(Seq(r)))

        val node = CassandraNode(mockNode, mockRanges)
        val res = node.splitForFullSize(100, 10, mockMap) 
        res should have length 10
        res.toSet should be (mockRanges.toSet)


        mockRanges.foreach(r => verify(r, times(1)).splitForFullSize(100, 10, mockMap))
    }

    it should "join the primary token range if the range is contiguous and the table is small" in {
        val mockNode = mock[Node]
        val mockMap = mock[TokenMap]
        val mockRanges = (1 to 10).map(i => mock[CassandraTokenRange]).toSet
        mockRanges.foreach(r => when(r.percentageOfFullRing).thenReturn(0.01))
        mockRanges.foreach(r => when(r.splitForFullSize(100, 10, mockMap)).thenReturn(Seq(r)))
        mockRanges.foreach(r => when(r.mergeWith(any[TokenMap](), any[CassandraTokenRange]())).thenReturn(r))

        val node = CassandraNode(mockNode, mockRanges)
        val res = node.joinAndSplitForFullSize(100, 10, mockMap) 
        res should have length 1
        res should be (Seq(CassandraPartition(Seq(mockRanges.toSeq.head))))
    }
}