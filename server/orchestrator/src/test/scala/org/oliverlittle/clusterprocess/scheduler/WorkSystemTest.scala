package org.oliverlittle.clusterprocess.scheduler

import org.oliverlittle.clusterprocess.UnitSpec
import org.oliverlittle.clusterprocess.model.table._
import org.oliverlittle.clusterprocess.model.table.field._
import org.oliverlittle.clusterprocess.worker_query
import org.oliverlittle.clusterprocess.table_model

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar
import akka.actor.typed._
import akka.actor.typed.scaladsl._

import scala.concurrent.{Promise, Await}
import scala.concurrent.duration.Duration

// Testing reference: https://doc.akka.io/docs/akka/current/typed/testing-async.html

/**
* This class tests the system as a whole - assembler/producer/consumer - to ensure everything works together correctly
*/
class WorkSystemTest extends UnitSpec with MockitoSugar {
    "The WorkSystem" should "collect responses when the expected number of responses is reached" in {
        val one = worker_query.ComputePartialResultCassandraRequest(dataSource=Some(table_model.CassandraDataSource("test", "one")))
        val three = worker_query.ComputePartialResultCassandraRequest(dataSource=Some(table_model.CassandraDataSource("test", "three")))
        val two = worker_query.ComputePartialResultCassandraRequest(dataSource=Some(table_model.CassandraDataSource("test", "two")))

        val response : Seq[table_model.StreamedTableResult] = Seq(
            table_model.StreamedTableResult().withHeader(TableResultHeader(Seq()).protobuf),
            table_model.StreamedTableResult().withRow(table_model.TableResultRow(Seq()))
        )
        val mockStub = mock[worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceBlockingStub]
        when(mockStub.computePartialResultCassandra(one)).thenReturn(response.iterator)
        when(mockStub.computePartialResultCassandra(two)).thenReturn(response.iterator)
        when(mockStub.computePartialResultCassandra(three)).thenReturn(response.iterator)

        val items = Seq(
            (Seq(mockStub), Seq(one, three)),
            (Seq(mockStub), Seq(two))
        )

        val p = Promise[Boolean]()
        val resultCallback : TableResult => Unit = {r => 
            p.success(true)
        }

        val system = ActorSystem(WorkExecutionScheduler(items, (l) => l.reduce(_ ++ _), resultCallback), "WorkExecutionScheduler")
        Await.result(p.future, Duration(5, "s")) should be (true)

        // Verify
        verify(mockStub, times(1)).computePartialResultCassandra(one)
        verify(mockStub, times(1)).computePartialResultCassandra(three)
        verify(mockStub, times(1)).computePartialResultCassandra(three)

        system.terminate()
    }
    
    // Add tests for error state here once implemented
}