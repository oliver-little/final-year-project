package org.oliverlittle.clusterprocess.model.table.sources

import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.UnitSpec
import org.oliverlittle.clusterprocess.connector.grpc.{WorkerHandler, ChannelManager}
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

class MockDataSource extends DataSource with MockitoSugar {
    def getHeaders = TableResultHeader(Seq(BaseIntField("a")))
    def getPartitions(workerHandler : WorkerHandler)(using ec : ExecutionContext) : Future[Seq[(Seq[ChannelManager], Seq[PartialDataSource])]] = 
        Future.successful(Seq((Seq(mock[ChannelManager]), Seq(MockPartialDataSource()))))

    def getQueryPlan = Seq(GetPartition(this))
    def getCleanupQueryPlan = Seq(DeletePartition(this))

    def isValid = true

    def protobuf = table_model.DataSource().withCassandra(table_model.CassandraDataSource(keyspace="test", table="test"))
}

class MockPartialDataSource extends PartialDataSource with MockitoSugar {
    val parent = MockDataSource()

    def protobuf: table_model.PartialDataSource = table_model.PartialDataSource().withCassandra(table_model.PartialCassandraDataSource("test", "test_table", Seq(table_model.CassandraTokenRange(0, 1))))

    override def getPartialData(store : ActorRef[TableStore.TableStoreEvent], workerChannels : Seq[ChannelManager])(using t : Timeout)(using system : ActorSystem[_])(using ec : ExecutionContext = system.executionContext) : Future[TableResult] = 
        Future.successful(LazyTableResult(parent.getHeaders, Seq(Seq(Some(IntValue(1))))))
}