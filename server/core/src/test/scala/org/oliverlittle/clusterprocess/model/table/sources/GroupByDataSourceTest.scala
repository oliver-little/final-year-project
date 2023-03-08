package org.oliverlittle.clusterprocess.model.table.sources

import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.worker_query
import org.oliverlittle.clusterprocess.{AsyncUnitSpec, UnitSpec}
import org.oliverlittle.clusterprocess.connector.grpc.{WorkerHandler, ChannelManager, MockitoChannelManager, TableResultRunnable, StreamedTableResult}
import org.oliverlittle.clusterprocess.query._
import org.oliverlittle.clusterprocess.model.table._
import org.oliverlittle.clusterprocess.model.table.field._
import org.oliverlittle.clusterprocess.model.field.expressions.{F, Max}

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.mockito.AdditionalAnswers
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterAll

import io.grpc.stub.{StreamObserver, ServerCallStreamObserver}
import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.util.Timeout

import scala.concurrent.duration._
import scala.concurrent.{Future, ExecutionContext}

// Helper classes, simulates a root data source because we can't use Cassandra
class GroupByRootDataSource(valid : Boolean = true) extends DataSource {
    def getHeaders = TableResultHeader(Seq(BaseStringField("key"), BaseIntField("value")))

    def empty : TableResult = EvaluatedTableResult(getHeaders, Seq())

    def getPartitions(workerHandler : WorkerHandler)(using ec : ExecutionContext) : Future[Seq[(Seq[ChannelManager], Seq[PartialDataSource])]] = 
        Future.successful(Seq((Seq(MockitoChannelManager()), Seq(PartialGroupByRootDataSource(this)))))

    def getQueryPlan = Seq(GetPartition(this))
    def getCleanupQueryPlan = Seq(DeletePartition(this))

    def isValid = valid

    def protobuf = table_model.DataSource().withCassandra(table_model.CassandraDataSource(keyspace="test", table="test"))
}

case class PartialGroupByRootDataSource(parent : GroupByRootDataSource) extends PartialDataSource {
    val sampleResult : TableResult = EvaluatedTableResult(getHeaders, Seq(Seq(Some(IntValue(1))), Seq(Some(IntValue(2)))))

    def protobuf: table_model.PartialDataSource = table_model.PartialDataSource().withCassandra(table_model.PartialCassandraDataSource("test", "test_table", Seq(table_model.CassandraTokenRange(0, 1))))

    val partialData = LazyTableResult(parent.getHeaders, Seq(Seq(Some(IntValue(1)))))

    override def getPartialData(store : ActorRef[TableStore.TableStoreEvent], workerChannels : Seq[ChannelManager])(using t : Timeout)(using system : ActorSystem[_])(using ec : ExecutionContext = system.executionContext) : Future[TableResult] = 
        Future.successful(partialData)
}

given Timeout = 3.seconds

class GroupByDataSourceTest extends AsyncUnitSpec with MockitoSugar {

    val dependencySource = GroupByRootDataSource()
    val testDataSource = GroupByDataSource(Table(dependencySource), Seq(F("key") as "GroupByKey"), Seq(Max(F("value") as "GroupByValue")))

    "A GroupByDataSource" should "return the correct headers" in {
        testDataSource.getHeaders should be (TableResultHeader(Seq(BaseStringField("GroupByKey"), BaseIntField("Max_GroupByValue"))))
    }

    it should "get the Table above as a dependency" in {
        testDataSource.source should be (Table(dependencySource))
    }

    it should "be valid if the table above is valid, as well as the unique and aggregate fields" in {
        testDataSource.isValid should be (true)
    }

    it should "be invalid if any dependency is invalid" in {
        GroupByDataSource(Table(dependencySource), Seq(F("missing_field") as "GroupByKey"), Seq(Max(F("value") as "GroupByValue"))).isValid should be (false)
        GroupByDataSource(Table(dependencySource), Seq(F("key") as "GroupByKey"), Seq(Max(F("missing_field") as "GroupByValue"))).isValid should be (false)
        
        // This throws an error because table checks for data source validity specifically
        assertThrows[IllegalArgumentException] {
            GroupByDataSource(Table(GroupByRootDataSource(false)), Seq(F("key") as "GroupByKey"), Seq(Max(F("value") as "GroupByValue"))).isValid should be (false)
        }
    }

    it should "get partitions based on the stored data size" in {
        val mockWorkerHandler = mock[WorkerHandler]
        val channels = (1 to 3).map(_ => MockitoChannelManager())
        when(mockWorkerHandler.channels).thenReturn(channels)
        when(mockWorkerHandler.getNumPartitionsForTable(any[Table]())(using any[ExecutionContext]())).thenReturn(Future.successful(12))
        testDataSource.getPartitions(mockWorkerHandler)(using ExecutionContext.global).map {partitions =>
            partitions.map(_._1).flatten should be (channels)
            partitions.map(_._2) should have length (4)
            partitions.map(_._2).flatten should have length (12)
        }

        when(mockWorkerHandler.getNumPartitionsForTable(any[Table]())(using any[ExecutionContext]())).thenReturn(Future.successful(3))
        testDataSource.getPartitions(mockWorkerHandler)(using ExecutionContext.global).map {partitions =>
            partitions.map(_._1).flatten should be (channels)
            partitions.map(_._2) should have length (3)
            partitions.map(_._2).flatten should have length (3)
        }
    }

    it should "get partitions when there are less partitions than the number of workers" in {
        val mockWorkerHandler = mock[WorkerHandler]
        val channels = (1 to 3).map(_ => MockitoChannelManager())
        when(mockWorkerHandler.channels).thenReturn(channels)
        when(mockWorkerHandler.getNumPartitionsForTable(any[Table]())(using any[ExecutionContext]())).thenReturn(Future.successful(1))
        testDataSource.getPartitions(mockWorkerHandler)(using ExecutionContext.global).map {partitions =>
            partitions.map(_._1).flatten should be (channels)
            partitions.map(_._2) should have length (3)
            partitions.map(_._2).flatten should have length (1)
        }
    }

    it should "hash the data based on the unique column" in {
        val result = EvaluatedTableResult(TableResultHeader(Seq(BaseStringField("key"), BaseIntField("value"))), Seq(
            Seq(Some(StringValue("a")), Some(IntValue(1))),
            Seq(Some(StringValue("a")), Some(IntValue(2))),
            Seq(Some(StringValue("b")), Some(IntValue(3))),
            Seq(Some(StringValue("b")), Some(IntValue(4))),
        ))

        // Not the best test, because the hashing is random - not guaranteed that every partition will have data in it
        val hashes = testDataSource.hashPartitionedData(result, 2).toMap
        hashes.size should be (2)
        hashes(0).rows.size should be (2)
        hashes(1).rows.size should be (2)
    }

    it should "convert to GroupBy protobuf correctly" in {
        testDataSource.groupByProtobuf should be (table_model.GroupByDataSource(Some(Table(dependencySource).protobuf), Seq(F("key").as("GroupByKey").protobuf), Seq(Max(F("value").as("GroupByValue")).protobuf)))
    }

    it should "convert to DataSource protobuf correctly" in {
        testDataSource.protobuf should be (table_model.DataSource().withGroupBy(testDataSource.groupByProtobuf))
    }
}

class PartialGroupByDataSourceTest extends worker_query.WorkerComputeServiceTestServer(5) with MockitoSugar with BeforeAndAfterAll {
    // TestKit setup and teardown
    val testKit : ActorTestKit = ActorTestKit()

    override def beforeAll() : Unit = {
        super.beforeAll()
    }    

    override def afterAll() : Unit = {
        super.afterAll()
        testKit.shutdownTestKit()
    }

    class WorkerComputeServiceImpl() {
        def getHashedPartitionData(request : worker_query.GetHashedPartitionDataRequest, responseObserver : StreamObserver[table_model.StreamedTableResult]) : Unit = {
            val scso = responseObserver.asInstanceOf[ServerCallStreamObserver[table_model.StreamedTableResult]]
            TableResultRunnable(scso, StreamedTableResult.tableResultToIterator(testResult)).run()
        }
    }

    val delegateClass = new WorkerComputeServiceImpl()
    // Define server implementation - note, verifying does not work on this
    val serviceImpl = mock[worker_query.WorkerComputeServiceGrpc.WorkerComputeService](AdditionalAnswers.delegatesTo(delegateClass))


    given ActorSystem[_] = testKit.system

    val dependencySource = GroupByRootDataSource()
    val parentDataSource = GroupByDataSource(Table(dependencySource), Seq(F("key") as "GroupByKey"), Seq(Max(F("value") as "GroupByValue")))
    val testDataSource = PartialGroupByDataSource(parentDataSource, 1, 3)

    val testResult = EvaluatedTableResult(TableResultHeader(Seq(BaseStringField("key"), BaseIntField("value"))), Seq(
            Seq(Some(StringValue("a")), Some(IntValue(1))),
            Seq(Some(StringValue("a")), Some(IntValue(2))),
            Seq(Some(StringValue("b")), Some(IntValue(3))),
            Seq(Some(StringValue("b")), Some(IntValue(4))),
        ))

    "A PartialGroupByDataSource" should "convert to protobuf correctly" in {
        testDataSource.protobuf should be (table_model.PartialDataSource().withGroupBy(table_model.PartialGroupByDataSource(Some(parentDataSource.groupByProtobuf), Some(table_model.PartitionInformation(1, 3)))))
    }

    it should "get a TableResult of hashed partition data from a single worker" in {
        val promise = testDataSource.getHashedPartitionData(Table(dependencySource), channels(0))
        promise.future map {res =>
            res.isDefined should be (true)
            res.get.header should be (testResult.header)
            res.get.rows should be (testResult.rows)
        }
    }

    it should "perform a group by without aggregate functions correctly" in {
        val uniqueOnlyParent = GroupByDataSource(Table(dependencySource), Seq(F("key") as "GroupByKey"), Seq())
        val uniqueOnlyTest = PartialGroupByDataSource(uniqueOnlyParent, 1, 3)

        val result = uniqueOnlyTest.performGroupBy(testResult)
        result.header should be (uniqueOnlyParent.getHeaders)
        result.rows.size should be (2)
        result.rows.toSet should be (Set(
                Seq(Some(StringValue("a"))),
                Seq(Some(StringValue("b")))
            ))
    }

    it should "perform a group by with aggregate functions correctly" in {
        val result = testDataSource.performGroupBy(testResult)
        result.header should be (parentDataSource.getHeaders)
        result.rows.size should be (2)
        result.rows.toSet should be ((Set(
                Seq(Some(StringValue("a")), Some(IntValue(2))),
                Seq(Some(StringValue("b")), Some(IntValue(4)))
            )))
    }

    it should "get partial data from the other workers and data store" in {
        val store = testKit.spawn(TableStore())

        store.ask(ref => TableStore.AddResult(PartialTable(PartialGroupByRootDataSource(dependencySource)), testResult, ref)) flatMap { res =>
            store.ask(ref => TableStore.HashPartition(parentDataSource, 3, ref))
        } flatMap {res =>
            testDataSource.getPartialData(store, channels) 
        } map {res =>
            res.header should be (testDataSource.parent.getHeaders) 
            res.rows.toSet should be (Set(
                Seq(Some(StringValue("a")), Some(IntValue(2))),
                Seq(Some(StringValue("b")), Some(IntValue(4)))
            ))
            res.rows.size should be (2)
        }
    }
}