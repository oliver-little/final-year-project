package org.oliverlittle.clusterprocess.scheduler

import org.oliverlittle.clusterprocess.connector.cassandra.token._

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.LoggerOps
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}

object NodeWorkProducer {
    final case class RequestWork(replyTo : ActorRef[NodeWorkConsumer.ConsumerEvent])
    
    def apply(items : Seq[CassandraTokenRange]) : Behavior[RequestWork] = list(items)

    private def list(items : Seq[CassandraTokenRange]) : Behavior[RequestWork] = Behaviors.receive{(context, message) =>
        items.isEmpty match {
            case true => 
                message.replyTo ! NodeWorkConsumer.NoWork()
                Behaviors.same
            case false => 
                message.replyTo ! NodeWorkConsumer.HasWork(items.head)
                list(items.tail)
        }
    }
}

object NodeWorkConsumer {
    sealed trait ConsumerEvent
    final case class HasWork(tokenRange : CassandraTokenRange) extends ConsumerEvent
    final case class NoWork() extends ConsumerEvent

    def apply() : Behavior[ConsumerEvent] = Behaviors.receive{(context, message) => {
        message match {
            case NoWork() => Behaviors.stopped
            case HasWork(item) => Behaviors.same // Do something with the token range
        }
    }}
}