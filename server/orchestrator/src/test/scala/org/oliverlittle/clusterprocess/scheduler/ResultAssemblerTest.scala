package org.oliverlittle.clusterprocess.scheduler

import org.oliverlittle.clusterprocess.UnitSpec

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar

class ResultAssemblerTest extends UnitSpec with MockitoSugar {
    "A ResultAssembler" should "collect headers without sending data" in {
        fail()
    }

    it should  "send all held rows once all headers are received" in {
        fail()
    } 

    it should "send rows as they are received once all headers are received" in {
        fail()
    }

    it should "send a completed message when all completed messages are received" in {
        fail()
    }

    it should "send an error message if an error message is received" in {
        fail()
    }

    it should "hold data until all results are received if an alternate assembler function is used" in {
        fail()
    }
}

class MessageStreamedTableResultObserverTest extends UnitSpec with MockitoSugar {
    "A MessageStreamedTableResultObserver" should "forward each message" in {
        fail()
    }

    it should "forward error messages" in {
        fail()
    }

    it should "send completed messages" in {
        fail()
    }
}