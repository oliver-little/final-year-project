package org.oliverlittle.clusterprocess.connector.grpc

import org.oliverlittle.clusterprocess.worker_query
import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.UnitSpec
import org.oliverlittle.clusterprocess.connector.cassandra.node.CassandraNode

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar
import org.mockito.AdditionalAnswers
import org.scalatest.BeforeAndAfterAll

import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import io.grpc.Server
import com.datastax.oss.driver.api.core.metadata._

import scala.concurrent.{ExecutionContext, Future}
import java.net.InetSocketAddress
import java.util.UUID
import scala.jdk.CollectionConverters._
import java.util.Optional



class WorkerHandlerTest extends UnitSpec with MockitoSugar with BeforeAndAfterAll {

    private class WorkerComputeServicePartialImpl() {
        def getLocalCassandraNode(request : worker_query.GetLocalCassandraNodeRequest) : Future[worker_query.GetLocalCassandraNodeResult] = 
            Future.successful(worker_query.GetLocalCassandraNodeResult(Some(table_model.InetSocketAddress("localhost", 50002))))
    }
    
    var serviceImpl = mock[worker_query.WorkerComputeServiceGrpc.WorkerComputeService](AdditionalAnswers.delegatesTo(WorkerComputeServicePartialImpl()))
    var server : Server = null
    var channels : Seq[MockChannelManager] = null
    var workerHandler : WorkerHandler = null


    override protected def beforeAll(): Unit = {
        // Create a server, add service, create clients
        val serverName : String = InProcessServerBuilder.generateName()
        server = InProcessServerBuilder.forName(serverName).directExecutor().addService(worker_query.WorkerComputeServiceGrpc.bindService(serviceImpl, ExecutionContext.global)).build().start()
        channels = (1 to 3).map(_ => MockChannelManager(serverName))
        workerHandler = WorkerHandler(channels)
    }

    override protected def afterAll(): Unit = {
        channels.foreach(_.channel.shutdown())
        server.shutdown()
    }

    "A WorkerHandler" should "calculate partitions for Cassandra" in {
        
    }

    it should "calculate the CassandraNode corresponding to each ChannelManager" in {
        val mockMetadata = mock[Metadata]
        val mockTokenMap = mock[TokenMap]
        val mockNode = mock[Node]
        val mockEndPoint = mock[EndPoint]
        when(mockTokenMap.format(any())).thenReturn("0")
        val map = 
        when(mockMetadata.getNodes).thenReturn(Map(UUID.randomUUID() -> mockNode).asJava)
        when(mockNode.getBroadcastRpcAddress).thenReturn(Optional.ofNullable(new InetSocketAddress("localhost", 50002)))
        when(mockNode.getEndPoint).thenReturn(mockEndPoint)
        when(mockEndPoint.resolve).thenReturn(new InetSocketAddress("localhost", 50002))
        when(mockMetadata.getTokenMap).thenReturn(Optional.ofNullable(mockTokenMap))
        when(mockTokenMap.getTokenRanges(any())).thenReturn(Set().asJava)

        workerHandler.getChannelMapping(channels, mockMetadata) should be (Seq((CassandraNode(mockNode, Set()), channels)))
    }

    it should "calculate the number of partitions for a stored table" in {
        fail()
    }

    it should "ensure the number of partitions calculated is at least 1" in {
        fail()
    }

    it should "estimate the size of a table from all workers" in {
        fail()
    }
}