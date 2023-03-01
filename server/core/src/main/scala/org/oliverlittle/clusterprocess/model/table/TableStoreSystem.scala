package org.oliverlittle.clusterprocess.model.table

import org.oliverlittle.clusterprocess.model.table.TableStore

import akka.actor.typed.scaladsl.{Behaviors, LoggerOps}
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}

object TableStoreSystem:
    sealed trait TableStoreSystemEvent
    case class GetStore(replyTo : ActorRef[ActorRef[TableStore.TableStoreEvent]]) extends TableStoreSystemEvent

    def create() : ActorSystem[TableStoreSystemEvent] = ActorSystem(TableStoreSystem(), "root")

    def apply() : Behavior[TableStoreSystemEvent] = Behaviors.setup{context =>   
        val tableStore = context.spawn(TableStore(), "tableStore")
        handleMessage(tableStore)
    }

    def handleMessage(store : ActorRef[TableStore.TableStoreEvent]) : Behavior[TableStoreSystemEvent] = Behaviors.receiveMessage{
        case GetStore(replyTo) => 
            replyTo ! store
            Behaviors.same
    }