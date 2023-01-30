package org.oliverlittle.clusterprocess.connector.cassandra.node

import org.oliverlittle.clusterprocess.connector.cassandra.token._
import org.oliverlittle.clusterprocess.UnitSpec

import com.datastax.oss.driver.api.core.metadata.Node
import com.datastax.oss.driver.api.core.metadata.{TokenMap, Metadata}

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
// Overwrite mock to remove ambiguous import
import org.mockito.Mockito.{mock => oldMock, _}
import org.scalatestplus.mockito.MockitoSugar._

import java.net.InetSocketAddress
import java.util.Optional

class CassandraNodeTest extends UnitSpec {
    "A CassandraNode" should "return the correct address as a string" in {
        val mockNode = mock[Node]
        val addr = new InetSocketAddress("localhost", 9000)
        when(mockNode.getBroadcastRpcAddress).thenReturn(Optional.of(addr))

        val node = CassandraNode(mockNode, Set())
        node.getAddressAsString should be ("localhost:9000")
        verify(mockNode, times(2)).getBroadcastRpcAddress
    }
}