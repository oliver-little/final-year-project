package org.oliverlittle.clusterprocess.connector.grpc

import org.oliverlittle.clusterprocess.worker_query

import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar
import io.grpc.ManagedChannel

class MockChannelManager() extends ChannelManager with MockitoSugar:
    val host = "localhost"
    val port = 50002

    val channel = mock[ManagedChannel]

    override def newInstance: ChannelManager = MockChannelManager()

    override val workerComputeServiceBlockingStub = mock[worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceBlockingStub]

    override lazy val workerComputeServiceStub = mock[worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceStub] 