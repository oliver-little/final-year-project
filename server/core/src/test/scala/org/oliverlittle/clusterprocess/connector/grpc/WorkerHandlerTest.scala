package org.oliverlittle.clusterprocess.connector.grpc

import org.oliverlittle.clusterprocess.worker_query
import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.AsyncUnitSpec
import org.oliverlittle.clusterprocess.model.table._
import org.oliverlittle.clusterprocess.model.table.sources._
import org.oliverlittle.clusterprocess.connector.cassandra.node.CassandraNode
import org.oliverlittle.clusterprocess.connector.cassandra.CassandraConnector
import org.oliverlittle.clusterprocess.connector.cassandra.token._


import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.mockito.AdditionalAnswers


import com.datastax.oss.driver.api.core.metadata._
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.metadata.token._
import com.datastax.oss.driver.api.core.cql._

import scala.concurrent.{ExecutionContext, Future}
import java.net.InetSocketAddress
import java.util.UUID
import scala.jdk.CollectionConverters._
import java.util.Optional

class WorkerHandlerTest extends worker_query.WorkerComputeServiceTestServer(3) {
    class WorkerComputeServicePartialImpl() {
        def getLocalCassandraNode(request : worker_query.GetLocalCassandraNodeRequest) : Future[worker_query.GetLocalCassandraNodeResult] = 
            Future.successful(worker_query.GetLocalCassandraNodeResult(Some(table_model.InetSocketAddress("localhost", 50002))))

        def getEstimatedTableSize(request : worker_query.GetEstimatedTableSizeRequest) : Future[worker_query.GetEstimatedTableSizeResult] =
            Future.successful(worker_query.GetEstimatedTableSizeResult(
                    // Hacky way of adjusting the estimated table size without providing an actual implementation
                    // Return a different value if the table has any transformations
                    if request.table.get.transformations.size > 0 then 10 else 50
                ))
    }

    val serviceImpl : worker_query.WorkerComputeServiceGrpc.WorkerComputeService = mock[worker_query.WorkerComputeServiceGrpc.WorkerComputeService](AdditionalAnswers.delegatesTo(WorkerComputeServicePartialImpl())) 

    "A WorkerHandler" should "calculate partitions for Cassandra" in {
        // Huge amounts of mocking because these are all dependencies of DataStax Java driver
        val mockConnector = mock[CassandraConnector]
        val mockSession = mock[CqlSession]
        val mockMetadata = mock[Metadata]
        val mockTokenMap = mock[TokenMap]
        val mockTokenRange = mock[TokenRange]
        val mockToken = mock[Token]
        val mockNode = mock[Node]
        val mockResultSet = mock[ResultSet]
        val mockRow = mock[Row]
        val mockEndPoint = mock[EndPoint]
        when(mockTokenMap.format(any())).thenReturn("0")
        when(mockMetadata.getNodes).thenReturn(Map(UUID.randomUUID() -> mockNode).asJava)
        when(mockNode.getBroadcastRpcAddress).thenReturn(Optional.ofNullable(new InetSocketAddress("localhost", 50002)))
        when(mockNode.getEndPoint).thenReturn(mockEndPoint)
        when(mockEndPoint.resolve).thenReturn(new InetSocketAddress("localhost", 50002))
        when(mockMetadata.getTokenMap).thenReturn(Optional.ofNullable(mockTokenMap))
        when(mockTokenMap.getTokenRanges(any())).thenReturn(Set(mockTokenRange).asJava)
        when(mockTokenRange.getStart).thenReturn(mockToken)
        when(mockTokenRange.getEnd).thenReturn(mockToken)
        when(mockTokenMap.format(mockToken)).thenReturn("0")
        when(mockConnector.getSession).thenReturn(mockSession)
        when(mockSession.getMetadata).thenReturn(mockMetadata)
        when(mockSession.execute(any[String](), any(), any())).thenReturn(mockResultSet)
        when(mockResultSet.iterator).thenReturn(Iterator(mockRow).asJava)
        when(mockRow.getString("range_start")).thenReturn("0")
        when(mockRow.getString("range_end")).thenReturn("0")
        when(mockRow.getLong("mean_partition_size")).thenReturn(10L)
        when(mockRow.getLong("partitions_count")).thenReturn(5L)

        workerHandler.distributeWorkToNodes(mockConnector, "test", "test") should be (Seq((channels, Seq(CassandraPartition(Seq(CassandraTokenRange.fromLong(0, 0)))))))
    }

    it should "calculate the CassandraNode corresponding to each ChannelManager" in {
        val mockMetadata = mock[Metadata]
        val mockTokenMap = mock[TokenMap]
        val mockNode = mock[Node]
        val mockEndPoint = mock[EndPoint]
        when(mockTokenMap.format(any())).thenReturn("0")
        when(mockMetadata.getNodes).thenReturn(Map(UUID.randomUUID() -> mockNode).asJava)
        when(mockNode.getBroadcastRpcAddress).thenReturn(Optional.ofNullable(new InetSocketAddress("localhost", 50002)))
        when(mockNode.getEndPoint).thenReturn(mockEndPoint)
        when(mockEndPoint.resolve).thenReturn(new InetSocketAddress("localhost", 50002))
        when(mockMetadata.getTokenMap).thenReturn(Optional.ofNullable(mockTokenMap))
        when(mockTokenMap.getTokenRanges(any())).thenReturn(Set().asJava)

        workerHandler.getChannelMapping(channels, mockMetadata) should be (Seq((CassandraNode(mockNode, Set()), channels)))
    }

    it should "calculate the number of partitions for a stored table" in {
        workerHandler.getNumPartitionsForTable(Table(MockDataSource())) map {res => res should be (3)}
    }

    it should "ensure the number of partitions calculated is at least 1" in {
        workerHandler.getNumPartitionsForTable(Table(MockDataSource(), Seq(SelectTransformation()))) map {res => res should be (1)}
    }

    it should "estimate the size of a table from all workers" in {
        workerHandler.getTableStoreEstimatedSizeMB(Table(MockDataSource())) map {res => res should be (150)}
    }
}