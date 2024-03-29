package org.oliverlittle.clusterprocess.model.table.sources

import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.UnitSpec
import org.oliverlittle.clusterprocess.connector.grpc.{WorkerHandler, ChannelManager, MockitoChannelManager}
import org.oliverlittle.clusterprocess.query._
import org.oliverlittle.clusterprocess.model.table._
import org.oliverlittle.clusterprocess.model.table.field._

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.util.Timeout

import scala.concurrent.{Future, ExecutionContext}
import java.util.UUID
import scala.collection.MapView

case class MockRootDataSource(randomiser : UUID = UUID.randomUUID()) extends DataSource with MockitoSugar {
    def getHeaders = TableResultHeader(Seq(BaseIntField("a")))

    def empty : TableResult = EvaluatedTableResult(getHeaders, Seq())

    def getPartitions(workerHandler : WorkerHandler)(using ec : ExecutionContext) : Future[Seq[(Seq[ChannelManager], Seq[PartialDataSource])]] = 
        Future.successful(Seq((Seq(MockitoChannelManager()), Seq(MockPartialRootDataSource(this)))))

    def getQueryPlan = Seq(GetPartition(this))
    def getCleanupQueryPlan = Seq(DeletePartition(this))

    def isValid = true

    def protobuf = table_model.DataSource().withCassandra(table_model.CassandraDataSource(keyspace="test", table="test"))

    override lazy val getDependencies: Seq[Table] = Seq()
}

case class MockPartialRootDataSource(parent : MockRootDataSource = MockRootDataSource(), randomiser : UUID = UUID.randomUUID()) extends PartialDataSource with MockitoSugar {
    val sampleResult : TableResult = EvaluatedTableResult(getHeaders, Seq(Seq(Some(IntValue(1))), Seq(Some(IntValue(2)))))

    def protobuf: table_model.PartialDataSource = table_model.PartialDataSource().withCassandra(table_model.PartialCassandraDataSource("test", "test_table", Seq(table_model.CassandraTokenRange(0, 1))))

    val partialData = LazyTableResult(parent.getHeaders, Seq(Seq(Some(IntValue(1)))))

    override def getPartialData(store : ActorRef[TableStore.TableStoreEvent], workerChannels : Seq[ChannelManager])(using t : Timeout)(using system : ActorSystem[_])(using ec : ExecutionContext = system.executionContext) : Future[TableResult] = 
        Future.successful(partialData)
}

case class MockDataSource(randomiser : UUID = UUID.randomUUID()) extends DependentDataSource with MockitoSugar {
    def getHeaders = TableResultHeader(Seq(BaseIntField("a")))

    def empty : TableResult = EvaluatedTableResult(getHeaders, Seq())

    def getPartitions(workerHandler : WorkerHandler)(using ec : ExecutionContext) : Future[Seq[(Seq[ChannelManager], Seq[PartialDataSource])]] = 
        Future.successful(Seq((Seq(MockitoChannelManager()), Seq(MockPartialDataSource(this)))))

    def getQueryPlan = Seq(GetPartition(this))
    def getCleanupQueryPlan = Seq(DeletePartition(this))

    def isValid = true

    def protobuf = table_model.DataSource().withCassandra(table_model.CassandraDataSource(keyspace="test", table="test"))

    lazy val partial = MockPartialDataSource(this)

    override lazy val getDependencies: Seq[Table] = Seq(Table(MockDataSource(), Seq()))

    val partitionHash : Map[Int, TableResult] = Map(0 -> EvaluatedTableResult(getHeaders, Seq(Seq(Some(IntValue(1))))), 1 -> empty)

    def hashPartitionedData(result: TableResult, numPartitions: Int): Map[Int, TableResult] = partitionHash
}

case class MockPartialDataSource(parent : MockDataSource = MockDataSource(), randomiser : UUID = UUID.randomUUID()) extends PartialDataSource with MockitoSugar {

    val sampleResult : TableResult = EvaluatedTableResult(getHeaders, Seq(Seq(Some(IntValue(1))), Seq(Some(IntValue(2)))))

    def protobuf: table_model.PartialDataSource = table_model.PartialDataSource().withCassandra(table_model.PartialCassandraDataSource("test", "test_table", Seq(table_model.CassandraTokenRange(0, 1))))

    val partialData = LazyTableResult(parent.getHeaders, Seq(Seq(Some(IntValue(1)))))

    override def getPartialData(store : ActorRef[TableStore.TableStoreEvent], workerChannels : Seq[ChannelManager])(using t : Timeout)(using system : ActorSystem[_])(using ec : ExecutionContext = system.executionContext) : Future[TableResult] = 
        Future.successful(partialData)
}