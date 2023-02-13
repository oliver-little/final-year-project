package org.oliverlittle.clusterprocess.store

import org.oliverlittle.clusterprocess.model.table._

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.LoggerOps
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}

object TableStore {
    sealed trait TableStoreEvent
    final case class AddResult(table : Table, result : TableResult) extends TableStoreEvent
    final case class RemoveResult(table : Table) extends TableStoreEvent
    final case class GetResult(table : Table, replyTo : ActorRef[Option[TableResult]]) extends TableStoreEvent

    def apply() : Behavior[TableStoreEvent] = processResults(Map())
        
    def processResults(items : Map[Table, TableResult]) : Behavior[TableStoreEvent] = Behavior.receive{ (context, message) =>
        case AddResult(table, result) =>
            processResults(items + (table -> result))
        case RemoveResult(table) => 
            processResults(items - table)
        case GetResult(table) =>
            replyTo ! items.get(table)
            Behaviors.same
    }
}
