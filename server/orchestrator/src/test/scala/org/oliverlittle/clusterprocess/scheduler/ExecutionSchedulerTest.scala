package org.oliverlittle.clusterprocess.scheduler

import org.oliverlittle.clusterprocess.UnitSpec
import org.oliverlittle.clusterprocess.model.table._
import org.oliverlittle.clusterprocess.model.table.field._
import org.oliverlittle.clusterprocess.worker_query
import org.oliverlittle.clusterprocess.data_source
import org.oliverlittle.clusterprocess.table_model

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar
import akka.actor.testkit.typed.scaladsl.BehaviorTestKit
import akka.actor.testkit.typed.scaladsl.TestInbox
import akka.actor.typed._
import akka.actor.typed.scaladsl._

// Testing reference: https://doc.akka.io/docs/akka/current/typed/testing-sync.html

/**
  * This test tests the assembler function of WorkExecutionScheduler
  */
class WorkExecutionSchedulerTest extends UnitSpec with MockitoSugar {
    "A WorkExecutionScheduler" should "create the correct number of producers and consumers based on the input data" in {

    }

    it should "collect responses as they arrive" in {

    }

    it should "call the callback when the expected number of responses is reached" in {

    }

    it should "call the callback when the expected number of responses is reached (and this number is 1)" in {

    }

    it should "apply alternative assembler functions" in {

    }
}