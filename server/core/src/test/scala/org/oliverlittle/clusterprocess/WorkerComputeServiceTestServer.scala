package org.oliverlittle.clusterprocess.worker_query

import org.oliverlittle.clusterprocess.AsyncUnitSpec
import org.oliverlittle.clusterprocess.connector.grpc.{MockChannelManager, WorkerHandler}

import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar
import org.mockito.AdditionalAnswers
import org.scalatest.BeforeAndAfterAll

import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import io.grpc.Server
import scala.concurrent.ExecutionContext

// Uses AsyncUnitSpec as a backend so will require assertions as the last line of every test
abstract class WorkerComputeServiceTestServer(numChannels : Int) extends AsyncUnitSpec with MockitoSugar with BeforeAndAfterAll {

    // Must be defined by subclasses - use mock with AdditionalAnswers for default implementation
    def serviceImpl : WorkerComputeServiceGrpc.WorkerComputeService
    var server : Server = null
    var channels : Seq[MockChannelManager] = null
    var workerHandler : WorkerHandler = null


    override protected def beforeAll(): Unit = {
        // Create a server, add service, create clients
        val serverName : String = InProcessServerBuilder.generateName()
        server = InProcessServerBuilder.forName(serverName).directExecutor().addService(WorkerComputeServiceGrpc.bindService(serviceImpl, ExecutionContext.global)).build().start()
        channels = (1 to numChannels).map(_ => MockChannelManager(serverName))
        workerHandler = WorkerHandler(channels)
    }

    override protected def afterAll(): Unit = {
        channels.foreach(_.channel.shutdown())
        server.shutdown()
    }
}
