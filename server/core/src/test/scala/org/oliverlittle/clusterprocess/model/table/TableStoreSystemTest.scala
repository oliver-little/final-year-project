package org.oliverlittle.clusterprocess.model.table

import org.oliverlittle.clusterprocess.UnitSpec

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar
import akka.util.Timeout
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.Behavior

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext

given Timeout = 3.seconds

class TableStoreSystemTest extends UnitSpec{
    "A TableStoreSystem" should "create a TableStore actor as a child" in {
        val system = TableStoreSystem.create()

        implicit val ec: ExecutionContext = system.executionContext
        implicit val scheduler = system.scheduler

        system.ask(ref => TableStoreSystem.GetStore(ref)) map {
            store => store shouldBe a [Behavior[TableStore.TableStoreEvent]]
        }
    }
}