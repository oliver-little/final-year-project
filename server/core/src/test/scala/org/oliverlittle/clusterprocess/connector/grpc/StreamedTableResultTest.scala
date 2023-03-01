package org.oliverlittle.clusterprocess.connector.grpc

import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.{AsyncUnitSpec, UnitSpec}
import org.oliverlittle.clusterprocess.model.table._
import org.oliverlittle.clusterprocess.model.table.field._

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatest.RecoverMethods._
import org.scalatestplus.mockito.MockitoSugar

import io.grpc.stub.{StreamObserver, ServerCallStreamObserver}

import scala.concurrent.Promise

class StreamedTableResultTest extends UnitSpec {
    "A StreamedTableResult" should "create an iterator of gRPC rows from a table result" in {
        val header = TableResultHeader(Seq(BaseIntField("a")))
        val rows = Seq(Seq(Some(IntValue(1))), Seq(Some(IntValue(1))))
        val result = LazyTableResult(header, rows)
        val iterator = StreamedTableResult.tableResultToIterator(result)
        iterator.toSeq should be (Seq(
             table_model.StreamedTableResult(table_model.StreamedTableResult.Data.Header(header.protobuf))) ++
             result.rowsProtobuf.map(row => table_model.StreamedTableResult(table_model.StreamedTableResult.Data.Row(row)))    
        )
    }
}

class TableResultRunnableTest extends UnitSpec with MockitoSugar {
    "A TableResultRunnable" should "send data until the iterator completes" in {
        val header = TableResultHeader(Seq(BaseIntField("a")))
        val rows = Seq(Seq(Some(IntValue(1))), Seq(Some(IntValue(1))))
        val result = LazyTableResult(header, rows)
        val iterator = StreamedTableResult.tableResultToIterator(result)

        val mockResponseObserver = mock[ServerCallStreamObserver[table_model.StreamedTableResult]]
        when(mockResponseObserver.isReady).thenReturn(true)
        val runnable = TableResultRunnable(mockResponseObserver, iterator)
        runnable.run()
        verify(mockResponseObserver, times(3)).onNext(any())
        verify(mockResponseObserver, times(1)).onCompleted()
    }

    it should "pause if the responseObserver is not ready" in {
        val header = TableResultHeader(Seq(BaseIntField("a")))
        val rows = Seq(Seq(Some(IntValue(1))), Seq(Some(IntValue(1))))
        val result = LazyTableResult(header, rows)
        val iterator = StreamedTableResult.tableResultToIterator(result)

        val mockResponseObserver = mock[ServerCallStreamObserver[table_model.StreamedTableResult]]
        when(mockResponseObserver.isReady).thenReturn(true, false, true)
        val runnable = TableResultRunnable(mockResponseObserver, iterator)
        runnable.run()
        verify(mockResponseObserver, times(1)).onNext(any())
        verify(mockResponseObserver, never()).onCompleted()
        runnable.run()
        verify(mockResponseObserver, times(3)).onNext(any())
        verify(mockResponseObserver, times(1)).onCompleted()
    }
}

class DelayedTableResultRunnableTest extends UnitSpec with MockitoSugar {
    "A DelayedTableResultRunnable" should "start running when setData is called" in {
        val header = TableResultHeader(Seq(BaseIntField("a")))
        val rows = Seq(Seq(Some(IntValue(1))), Seq(Some(IntValue(1))))
        val result = LazyTableResult(header, rows)

        val mockResponseObserver = mock[ServerCallStreamObserver[table_model.StreamedTableResult]]
        when(mockResponseObserver.isReady).thenReturn(true)
        val runnable = DelayedTableResultRunnable(mockResponseObserver)

        runnable.setData(result)
        verify(mockResponseObserver, times(3)).onNext(any())
        verify(mockResponseObserver, times(1)).onCompleted()
    }

    it should "pause if the responseObserver is not ready" in {
        val header = TableResultHeader(Seq(BaseIntField("a")))
        val rows = Seq(Seq(Some(IntValue(1))), Seq(Some(IntValue(1))))
        val result = LazyTableResult(header, rows)

        val mockResponseObserver = mock[ServerCallStreamObserver[table_model.StreamedTableResult]]
        when(mockResponseObserver.isReady).thenReturn(true, false, true)
        val runnable = DelayedTableResultRunnable(mockResponseObserver)
        runnable.setData(result)
        verify(mockResponseObserver, times(1)).onNext(any())
        verify(mockResponseObserver, never()).onCompleted()
        runnable.run()
        verify(mockResponseObserver, times(3)).onNext(any())
        verify(mockResponseObserver, times(1)).onCompleted()
    }
}

class StreamedTableResultCompilerTest extends AsyncUnitSpec with MockitoSugar {
    "A StreamedTableResultCompiler" should "collect data until onCompleted is called" in {
        val promise = Promise[Option[TableResult]]()
        val compiler = StreamedTableResultCompiler(promise)

        val header = TableResultHeader(Seq(BaseIntField("a")))
        val rows = Seq(Seq(Some(IntValue(1))), Seq(Some(IntValue(1))))
        val result = LazyTableResult(header, rows)
        val data = Seq(
             table_model.StreamedTableResult(table_model.StreamedTableResult.Data.Header(header.protobuf))) ++
             result.rowsProtobuf.map(row => table_model.StreamedTableResult(table_model.StreamedTableResult.Data.Row(row)))    
        

        data.foreach(compiler.onNext(_))
        compiler.onCompleted()

        promise.future.map {res => assert(Some(EvaluatedTableResult(header, rows)) == res) }
    }

    it should "fail the promise if onError is called" in {
        val promise = Promise[Option[TableResult]]()
        val compiler = StreamedTableResultCompiler(promise)

        compiler.onError(new IllegalStateException("Exception"))

        recoverToSucceededIf[IllegalStateException] {
            promise.future
        }
    }

    it should "return None if the table is incomplete" in {
        val promise = Promise[Option[TableResult]]()
        val compiler = StreamedTableResultCompiler(promise)

        val header = TableResultHeader(Seq(BaseIntField("a")))
        val rows = Seq(Seq(Some(IntValue(1))), Seq(Some(IntValue(1))))
        val result = LazyTableResult(header, rows)
        val data = result.rowsProtobuf.map(row => table_model.StreamedTableResult(table_model.StreamedTableResult.Data.Row(row)))   
        

        data.foreach(compiler.onNext(_))
        compiler.onCompleted()

        promise.future.map {res => assert(None == res) }
    }
}