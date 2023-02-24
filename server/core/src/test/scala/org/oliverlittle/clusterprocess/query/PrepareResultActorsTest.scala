package org.oliverlittle.clusterprocess.query

import org.oliverlittle.clusterprocess.UnitSpec

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar
import akka.actor.testkit.typed.scaladsl.BehaviorTestKit
import akka.actor.testkit.typed.scaladsl.TestInbox
import akka.actor.typed._
import akka.actor.typed.scaladsl._

class PrepareResultConsumerTest extends UnitSpec with MockitoSugar {
    "A PrepareResultConsumer" should "repeatedly compute work until it completes it all" in {
        fail()
    }

    it should "send an error message if an error occurs at any point" in {
        fail()
    }
}

class PrepareResultCounterTest extends UnitSpec with MockitoSugar {
    "A PrepareResultCounter" should "complete the promise when the expected number of responses is reached" in {
        fail()
    }

    it should "complete the promise when the expected number of responses is reached (and this number is 1)" in {
        fail()
    }
}