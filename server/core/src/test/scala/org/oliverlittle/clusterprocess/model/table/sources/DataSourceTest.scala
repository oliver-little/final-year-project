package org.oliverlittle.clusterprocess.model.table.sources

import org.oliverlittle.clusterprocess.UnitSpec
import org.oliverlittle.clusterprocess.connector.grpc.ChannelManager
import org.oliverlittle.clusterprocess.query._
import org.oliverlittle.clusterprocess.model.table.field._

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.Future

class MockDataSource extends DataSource with MockitoSugar {
    def getHeaders = TableResultHeader(Seq(BaseIntField("a")))
    def getPartitions(workerHandler : WorkerHandler)(using ec : ExecutionContext) : Future[Seq[(Seq[ChannelManager], Seq[PartialDataSource])]] = 
        Future.successful(Seq(Seq(mock[ChannelManager]), Seq(MockPartialDataSource())))

    def getQueryPlan = Seq(GetPartition(this))
    def getCleanupQueryPlan = Seq(DeletePartition(this))

    def isValid = true

    def protobuf = worker_query.DataSource().withCassandra(worker_query.CassandraDataSource(keyspace="test", table="test"))
}

class MockPartialDataSource extends PartialDataSource with MockitoSugar {
    val parent = MockDataSource()

    override def getPartialData(store : ActorRef[TableStore.TableStoreEvent], workerChannels : Seq[ChannelManager])(using t : Timeout)(using system : ActorSystem[_])(using ec : ExecutionContext = system.executionContext) : Future[TableResult] = 
        LazyTableResult(parent.getHeaders, Seq(Seq(IntValue(1))))
}