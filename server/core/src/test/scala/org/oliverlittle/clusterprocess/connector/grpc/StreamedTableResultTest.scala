package org.oliverlittle.clusterprocess.connector.grpc

import org.oliverlittle.clusterprocess.UnitSpec

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar

class StreamedTableResultTest extends UnitSpec {
    "A StreamedTableResult" should "create an iterator of gRPC rows from a table result" in {
        fail()
    }
}

class TableResultRunnableTest extends UnitSpec with MockitoSugar {
    "A TableResultRunnable" should "send data until the iterator completes" in {
        fail()
    }

    it should "pause if the responseObserver is not ready" in {
        fail()
    }
}

class DelayedTableResultRunnableTest extends UnitSpec with MockitoSugar {
    "A DelayedTableResultRunnable" should "start executing when setData is called" in {
        fail()
    }

    it should "send data until the iterator completes" in {
        fail()
    }

    it should "pause if the responseObserver is not ready" in {
        fail()
    }
}

class StreamedTableResultCompilerTest extends UnitSpec with MockitoSugar {
    "A StreamedTableResultCompiler" should "collect data until onCompleted is called" in {
        fail()
    }

    it should "fail the promise if onError is called" in {
        fail()
    }
}