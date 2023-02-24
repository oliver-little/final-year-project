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

class QueryPlanItemSchedulerTest extends UnitSpec with MockitoSugar {
    "A QueryPlanItemScheduler" should "call the execute method of its QueryPlanItem" in {
        fail()
    }

    it should "intercept the completed message and stop" in {
        fail()
    }
}