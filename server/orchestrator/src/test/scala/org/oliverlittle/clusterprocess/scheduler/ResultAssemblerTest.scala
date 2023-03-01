package org.oliverlittle.clusterprocess.scheduler

import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.worker_query
import org.oliverlittle.clusterprocess.UnitSpec
import org.oliverlittle.clusterprocess.connector.grpc.{ChannelManager, WorkerHandler, TableResultRunnable, StreamedTableResult}
import org.oliverlittle.clusterprocess.model.table.sources._
import org.oliverlittle.clusterprocess.model.table._
import org.oliverlittle.clusterprocess.model.table.field._
import org.oliverlittle.clusterprocess.model.field.expressions.{Max, F}
import org.oliverlittle.clusterprocess.query._

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.mockito.AdditionalAnswers
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterAll

import akka.actor.testkit.typed.scaladsl.{ActorTestKit, BehaviorTestKit}
import akka.actor.testkit.typed.scaladsl.TestInbox
import akka.actor.typed._
import akka.actor.typed.scaladsl._
import akka.actor.testkit.typed.Effect
import akka.util.Timeout

import io.grpc.stub.{StreamObserver, ServerCallStreamObserver}

import scala.concurrent.{ExecutionContext, Future}

// Helper classes, simulates a root data source because we can't use Cassandra
class RootDataSource(valid : Boolean = true) extends DataSource {
    def getHeaders = TableResultHeader(Seq(BaseStringField("key"), BaseIntField("value")))

    def empty : TableResult = EvaluatedTableResult(getHeaders, Seq())

    def getPartitions(workerHandler : WorkerHandler)(using ec : ExecutionContext) : Future[Seq[(Seq[ChannelManager], Seq[PartialDataSource])]] = Future.failed(new IllegalArgumentException("Unimplemented"))

    def getQueryPlan = Seq(GetPartition(this))
    def getCleanupQueryPlan = Seq(DeletePartition(this))

    def isValid = valid

    def protobuf = table_model.DataSource().withCassandra(table_model.CassandraDataSource(keyspace="test", table="test"))
}

case class PartialRootDataSource(parent : RootDataSource) extends PartialDataSource {
    val sampleResult : TableResult = EvaluatedTableResult(getHeaders, Seq(Seq(Some(IntValue(1))), Seq(Some(IntValue(2)))))

    def protobuf: table_model.PartialDataSource = table_model.PartialDataSource().withCassandra(table_model.PartialCassandraDataSource("test", "test_table", Seq(table_model.CassandraTokenRange(0, 1))))

    val partialData = LazyTableResult(parent.getHeaders, Seq(Seq(Some(IntValue(1)))))

    override def getPartialData(store : ActorRef[TableStore.TableStoreEvent], workerChannels : Seq[ChannelManager])(using t : Timeout)(using system : ActorSystem[_])(using ec : ExecutionContext = system.executionContext) : Future[TableResult] = 
        Future.successful(partialData)
}

class ResultAssemblerFullTest extends worker_query.WorkerComputeServiceTestServer(5) with MockitoSugar {
    class WorkerComputeServiceImpl() {
        def getTableData(request : worker_query.GetTableDataRequest, responseObserver : StreamObserver[table_model.StreamedTableResult]) : Unit = {
            val scso = responseObserver.asInstanceOf[ServerCallStreamObserver[table_model.StreamedTableResult]]
            TableResultRunnable(scso, StreamedTableResult.tableResultToIterator(testResult)).run()
        }
    }

    val testResult = EvaluatedTableResult(TableResultHeader(Seq(BaseStringField("key"), BaseIntField("value"))), Seq(
            Seq(Some(StringValue("a")), Some(IntValue(1))),
            Seq(Some(StringValue("a")), Some(IntValue(2))),
            Seq(Some(StringValue("b")), Some(IntValue(3))),
            Seq(Some(StringValue("b")), Some(IntValue(4))),
        ))

    val delegateClass = new WorkerComputeServiceImpl()
    // Define server implementation - note, verifying does not work on this
    val serviceImpl = mock[worker_query.WorkerComputeServiceGrpc.WorkerComputeService](AdditionalAnswers.delegatesTo(delegateClass))

    "A ResultAssembler (full)" should "collect results and forward them" in {
        val table = Table(RootDataSource())
        val mockScso = mock[ServerCallStreamObserver[table_model.StreamedTableResult]]
        when(mockScso.isReady).thenReturn(true)
        val completed = ResultAssembler.startExecution(table, channels, mockScso)
        completed map {res =>
            verify(mockScso, times(21)).onNext(any())
            verify(mockScso, times(1)).onCompleted
            assert(true)
        }
    }
}

class ResultAssemblerTest extends UnitSpec with MockitoSugar {
    "A ResultAssembler" should "send all held rows once all headers are received" in {
        val table = Table(RootDataSource())
        val mockScso = mock[ServerCallStreamObserver[table_model.StreamedTableResult]]
        val mockChannels = (1 to 2).map(_ => mock[ChannelManager])
        mockChannels.map(channel => when(channel.workerComputeServiceStub).thenReturn(mock[worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceStub]))
        val testKit = BehaviorTestKit(ResultAssembler(table, mockChannels, mockScso))
        val header = table_model.StreamedTableResult().withHeader(table_model.TableResultHeader(Seq()))
        val row = table_model.StreamedTableResult().withRow(table_model.TableResultRow(Seq()))
        testKit.run(ResultAssembler.Data(header))
        testKit.run(ResultAssembler.Data(row))
        testKit.run(ResultAssembler.Data(row))
        testKit.run(ResultAssembler.Data(header))
        verify(mockScso, times(3)).onNext(any())
    } 

    it should "send rows as they are received once all headers are received" in {
        val table = Table(RootDataSource())
        val mockScso = mock[ServerCallStreamObserver[table_model.StreamedTableResult]]
        val mockChannels = (1 to 2).map(_ => mock[ChannelManager])
        mockChannels.map(channel => when(channel.workerComputeServiceStub).thenReturn(mock[worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceStub]))
        val testKit = BehaviorTestKit(ResultAssembler(table, mockChannels, mockScso))
        val header = table_model.StreamedTableResult().withHeader(table_model.TableResultHeader(Seq()))
        val row = table_model.StreamedTableResult().withRow(table_model.TableResultRow(Seq()))
        testKit.run(ResultAssembler.Data(header))
        testKit.run(ResultAssembler.Data(header))
        verify(mockScso, times(1)).onNext(any())
        testKit.run(ResultAssembler.Data(row))
        testKit.run(ResultAssembler.Data(row))
        verify(mockScso, times(3)).onNext(any())
    }

    it should "send a completed message when all completed messages are received" in {
        val table = Table(RootDataSource())
        val mockScso = mock[ServerCallStreamObserver[table_model.StreamedTableResult]]
        val mockChannels = (1 to 2).map(_ => mock[ChannelManager])
        mockChannels.map(channel => when(channel.workerComputeServiceStub).thenReturn(mock[worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceStub]))
        val testKit = BehaviorTestKit(ResultAssembler(table, mockChannels, mockScso))
        val header = table_model.StreamedTableResult().withHeader(table_model.TableResultHeader(Seq()))
        val row = table_model.StreamedTableResult().withRow(table_model.TableResultRow(Seq()))
        testKit.run(ResultAssembler.Data(header))
        testKit.run(ResultAssembler.Data(header))
        verify(mockScso, times(1)).onNext(any())
        testKit.run(ResultAssembler.Complete())
        verify(mockScso, times(0)).onCompleted()
        testKit.run(ResultAssembler.Complete())
        verify(mockScso, times(1)).onCompleted()
    }

    it should "send an error message if an error message is received" in {
        val table = Table(RootDataSource())
        val mockScso = mock[ServerCallStreamObserver[table_model.StreamedTableResult]]
        val mockChannels = (1 to 2).map(_ => mock[ChannelManager])
        mockChannels.map(channel => when(channel.workerComputeServiceStub).thenReturn(mock[worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceStub]))
        val testKit = BehaviorTestKit(ResultAssembler(table, mockChannels, mockScso))
        val header = table_model.StreamedTableResult().withHeader(table_model.TableResultHeader(Seq()))
        val row = table_model.StreamedTableResult().withRow(table_model.TableResultRow(Seq()))
        val error = new IllegalStateException("Test exception")
        testKit.run(ResultAssembler.Error(error))
        verify(mockScso, times(1)).onError(any())
    }
}

class MessageStreamedTableResultObserverTest extends UnitSpec with MockitoSugar {
    "A MessageStreamedTableResultObserver" should "forward each message" in {
        val inbox = TestInbox[ResultAssembler.ResultEvent]()
        val observer = MessageStreamedTableResultObserver(inbox.ref)
        val data = table_model.StreamedTableResult()
        observer.onNext(data)
        inbox.receiveMessage() should be (ResultAssembler.Data(data))
        observer.onNext(data)
        inbox.receiveMessage() should be (ResultAssembler.Data(data))
    }

    it should "forward error messages" in {
        val inbox = TestInbox[ResultAssembler.ResultEvent]()
        val observer = MessageStreamedTableResultObserver(inbox.ref)
        val error = new IllegalStateException("Test exception")
        observer.onError(error)
        inbox.receiveMessage() should be (ResultAssembler.Error(error))
    }

    it should "send completed messages" in {
        val inbox = TestInbox[ResultAssembler.ResultEvent]()
        val observer = MessageStreamedTableResultObserver(inbox.ref)
        observer.onCompleted()
        inbox.receiveMessage() should be (ResultAssembler.Complete())
    }
}

class CustomResultAssemblerFullTest extends worker_query.WorkerComputeServiceTestServer(5) with MockitoSugar {
    class WorkerComputeServiceImpl() {
        def getTableData(request : worker_query.GetTableDataRequest, responseObserver : StreamObserver[table_model.StreamedTableResult]) : Unit = {
            val scso = responseObserver.asInstanceOf[ServerCallStreamObserver[table_model.StreamedTableResult]]
            TableResultRunnable(scso, StreamedTableResult.tableResultToIterator(testResult)).run()
        }
    }

    val testResult = EvaluatedTableResult(TableResultHeader(Seq(BaseStringField("key"), BaseIntField("Max_value"))), Seq(
            Seq(Some(StringValue("a")), Some(IntValue(1))),
            Seq(Some(StringValue("a")), Some(IntValue(2))),
            Seq(Some(StringValue("b")), Some(IntValue(3))),
            Seq(Some(StringValue("b")), Some(IntValue(4))),
        ))

    val delegateClass = new WorkerComputeServiceImpl()
    // Define server implementation - note, verifying does not work on this
    val serviceImpl = mock[worker_query.WorkerComputeServiceGrpc.WorkerComputeService](AdditionalAnswers.delegatesTo(delegateClass))

    "A CustomResultAssembler (full)" should "collect results, apply the ResultAssembler and forward them" in {
        val table = Table(RootDataSource(), Seq(AggregateTransformation(Max(F("value")))))
        val mockScso = mock[ServerCallStreamObserver[table_model.StreamedTableResult]]
        when(mockScso.isReady).thenReturn(true)
        val completed = CustomResultAssembler.startExecution(table, table.assembler, channels, mockScso)
        completed map {res =>
            verify(mockScso, times(2)).onNext(any())
            verify(mockScso, times(1)).onCompleted
            assert(true)
        }
    }
}
