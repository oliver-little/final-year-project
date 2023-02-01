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

// Testing reference: https://doc.akka.io/docs/akka/current/typed/testing-async.html

/**
  * This class tests the system as a whole - assembler/producer/consumer - to ensure everything works together correctly
  */
class WorkSystemTest extends UnitSpec with MockitoSugar {
    "The WorkSystem" should "create the appropriate number of consumers and producers" in {

    }

    it should "collect responses when the expected number of responses is reached" in {

    }

    // Add tests for error state here once implemented
}