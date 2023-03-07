package org.oliverlittle.clusterprocess.worker

import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.worker_query
import org.oliverlittle.clusterprocess.AsyncUnitSpec
import org.oliverlittle.clusterprocess.connector.grpc._
import org.oliverlittle.clusterprocess.model.table._
import org.oliverlittle.clusterprocess.connector.cassandra.{CassandraConfig, CassandraConnector}
import org.oliverlittle.clusterprocess.query._

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar
import org.mockito.AdditionalAnswers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import akka.actor.typed.{ActorRef, ActorSystem}
import akka.actor.typed.scaladsl.AskPattern._
import akka.util.Timeout

import io.grpc.inprocess.InProcessServerBuilder
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.{Server, ManagedChannel, StatusRuntimeException}

import scala.concurrent.{Future, Promise, ExecutionContext, Await}
import scala.concurrent.duration._
import java.net.InetSocketAddress

object MockCassandraConfig:
    def apply() : MockConfigHolder = MockConfigHolder(mock(classOf[CassandraConnector]))

    case class MockConfigHolder(connector : CassandraConnector) extends CassandraConfig

class WorkerQueryServerTest extends AsyncUnitSpec with MockitoSugar with BeforeAndAfterAll with BeforeAndAfterEach {
    implicit val tableStoreSystem : ActorSystem[TableStoreSystem.TableStoreSystemEvent] = TableStoreSystem.create()
    
    val mockConfig = MockCassandraConfig()

    var server : Server = null
    var workerHandler : WorkerHandler = null
    var channel : ManagedChannel = null
    var blockingStub : worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceBlockingStub = null
    var stub : worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceStub = null

    var store : ActorRef[TableStore.TableStoreEvent] = null

    override protected def beforeAll(): Unit = {
        // Create a server, add service, create clients
        val serverName : String = InProcessServerBuilder.generateName()
        workerHandler = WorkerHandler((1 to 3).map(_ => mock[ChannelManager]))
        implicit val ec: ExecutionContext = tableStoreSystem.executionContext
        implicit val scheduler = tableStoreSystem.scheduler
        val tableStoreFuture : Future[ActorRef[TableStore.TableStoreEvent]] = tableStoreSystem.ask(ref => TableStoreSystem.GetStore(ref))
        store = Await.result(tableStoreFuture, 3.seconds)

        server = InProcessServerBuilder.forName(serverName).directExecutor().addService(worker_query.WorkerComputeServiceGrpc.bindService((WorkerQueryServicer(store)(using tableStoreSystem)(using ExecutionContext.global)(using mockConfig)), ExecutionContext.global)).build().start()
        channel = InProcessChannelBuilder.forName(serverName).directExecutor().build()
        blockingStub = worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceBlockingStub(channel)
        stub = worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceStub(channel)
    }

    override protected def afterAll(): Unit = {
        server.shutdown()
        channel.shutdown()
        tableStoreSystem.terminate()
    }

    override protected def afterEach() : Unit = {
        store ! TableStore.Reset()
    }

    "A WorkerQueryServer" should "return the local Cassandra node from CassandraConnector" in {
        when(mockConfig.connector.socket).thenReturn(new InetSocketAddress("localhost", 4000))
        val result = blockingStub.getLocalCassandraNode(worker_query.GetLocalCassandraNodeRequest())
        result should be (worker_query.GetLocalCassandraNodeResult(address=Some(table_model.InetSocketAddress(host="localhost", port=4000))))
    }

    it should "process a single PartialQueryPlanItem" in {
        val query = worker_query.QueryPlanItem().withIdentity(worker_query.Identity(false))
        val result = blockingStub.processQueryPlanItem(query)
        result should be (worker_query.ProcessQueryPlanItemResult(true))
    }

    it should "return an unknown error if the PartialQueryPlanItem computation fails for any reason" in {
        val query = worker_query.QueryPlanItem().withIdentity(worker_query.Identity(true))
        assertThrows[StatusRuntimeException] {
            blockingStub.processQueryPlanItem(query)
        }
    }

    it should "return the TableStoreData as a protobuf" in {
        val result = blockingStub.getTableStoreData(worker_query.GetTableStoreDataRequest())
        result should be (worker_query.TableStoreData(Seq(), Seq()))
    }

    it should "apply cache reset requests" in {
        val result = blockingStub.clearCache(worker_query.ClearCacheRequest())
        result should be (worker_query.ClearCacheResult(true))
    }
}