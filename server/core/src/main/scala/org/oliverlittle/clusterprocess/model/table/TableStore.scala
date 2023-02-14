package org.oliverlittle.clusterprocess.model.table

import org.oliverlittle.clusterprocess.model.table.sources.PartialDataSource

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.LoggerOps
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}

object TableStore {
    sealed trait TableStoreEvent
    final case class AddResult(table : PartialTable, result : TableResult) extends TableStoreEvent
    final case class RemoveResult(table : PartialTable) extends TableStoreEvent
    final case class GetResult(table : PartialTable, replyTo : ActorRef[Option[TableResult]]) extends TableStoreEvent
    final case class AddPartition(partition : PartialDataSource, result : TableResult) extends TableStoreEvent
    final case class RemovePartition(partition : PartialDataSource) extends TableStoreEvent
    final case class GetPartition(partition : PartialDataSource, replyTo : ActorRef[Option[TableResult]]) extends TableStoreEvent

    def apply() : Behavior[TableStoreEvent] = processResults(Map(), Map())
        
    def processResults(tables : Map[PartialTable, TableResult], partitions : Map[PartialDataSource, TableResult]) : Behavior[TableStoreEvent] = Behaviors.receive{ (context, message) =>
        message match {
            case AddResult(table, result) =>
                processResults(tables + (table -> result), partitions)
            case RemoveResult(table) => 
                processResults(tables - table, partitions)
            case GetResult(table, replyTo) =>
                replyTo ! tables.get(table)
                Behaviors.same
            case AddPartition(partition, result) =>
                processResults(tables, partitions + (partition -> result))
            case RemovePartition(partition) =>
                processResults(tables, partitions - partition)
            case GetPartition(partition, replyTo) => 
                replyTo ! partitions.get(partition)
                Behaviors.same
        }
    }
}
