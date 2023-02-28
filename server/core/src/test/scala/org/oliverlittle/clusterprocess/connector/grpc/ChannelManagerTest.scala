package org.oliverlittle.clusterprocess.connector.grpc

import org.oliverlittle.clusterprocess.worker_query

import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar
import io.grpc.ManagedChannel
import io.grpc.inprocess.InProcessChannelBuilder

class MockChannelManager(serverName : String) extends ChannelManager with MockitoSugar:
    val host = "localhost"
    val port = 50002

    val channel = InProcessChannelBuilder.forName(serverName).directExecutor().build()

    override def newInstance: ChannelManager = this

    override val workerComputeServiceBlockingStub = worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceBlockingStub(channel)

    override lazy val workerComputeServiceStub = worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceStub(channel)

class MockitoChannelManager() extends ChannelManager with MockitoSugar:
    val host = "localhost"
    val port = 50002

    val channel = mock[ManagedChannel]

    override def newInstance: ChannelManager = this

    override val workerComputeServiceBlockingStub = mock[worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceBlockingStub]

    override lazy val workerComputeServiceStub = mock[worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceStub]