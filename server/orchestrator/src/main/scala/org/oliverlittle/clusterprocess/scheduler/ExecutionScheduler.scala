package org.oliverlittle.clusterprocess.scheduler

import org.oliverlittle.clusterprocess.connector.grpc.ChannelManager
import org.oliverlittle.clusterprocess.connector.cassandra.token._
import org.oliverlittle.clusterprocess.model.table.{Table, TableResult, LazyTableResult}
import org.oliverlittle.clusterprocess.worker_query

import akka.NotUsed
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.LoggerOps
import akka.actor.typed.{ActorRef, ActorSystem, Behavior, Terminated}

object WorkExecutionScheduler {
    def startFromData(channelTokenRangeMap : Seq[(Seq[ChannelManager], Seq[CassandraTokenRange])], table : Table) : Unit = {
        val dataSourceProtobuf = table.dataSource.getCassandraProtobuf.get
        val tableProtobuf = table.protobuf

        val parsedItems = channelTokenRangeMap.map((channels : Seq[ChannelManager], tokenRanges : Seq[CassandraTokenRange]) => 
            (
                channels.map(manager => worker_query.WorkerComputeServiceGrpc.blockingStub(manager.channel)), 
                tokenRanges.map(tokenRange => worker_query.ComputePartialResultCassandraRequest(table=Some(tableProtobuf), dataSource=Some(dataSourceProtobuf), tokenRange=Some(tokenRange.protobuf)))
            )
        )

        startExecution(parsedItems)
    }

    def startExecution(items : Seq[(Seq[worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceBlockingStub], Seq[worker_query.ComputePartialResultCassandraRequest])]) : Unit = {
        ActorSystem(WorkExecutionScheduler(items), "WorkExecutionScheduler")
    }

    def apply(items : Seq[(Seq[worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceBlockingStub], Seq[worker_query.ComputePartialResultCassandraRequest])]): Behavior[NotUsed] = Behaviors.setup{context =>
        // For each sequence of requests, create a producer (with a unique identifier)
        val mappedProducers = items.zipWithIndex.map((pair, index) => pair._1 -> context.spawn(WorkProducer(pair._2), "producer" + index.toString))
        val producers = mappedProducers.map(_._2)
        val assembler = context.spawn(WorkAssembler(None), "assembler")
        // For each possible stub, and it's matched producer, create a consumer and give it a list of producers to consume from
        val consumers = mappedProducers.map((stubs, producerRef) => stubs.zipWithIndex.map((stub, index) => context.spawn(WorkConsumer(stub, producerRef +: producers.filter(_ == producerRef), assembler), "consumer" + index.toString))).flatten
        // Watch all consumers for termination
        consumers.foreach(context.watch(_))
        val numConsumers = consumers.size
        trackTerminations(0, numConsumers)
    }

    /**
      * Tracks the number of terminations to terminate the actor system when all consumers terminate
      *
      * @param numTerminations The current number of terminations
      * @param expectedTerminations The expected number of terminations
      */
    private def trackTerminations(numTerminations : Int, expectedTerminations : Int) : Behavior[NotUsed] = Behaviors.receiveSignal{
        case (_, Terminated(_)) if numTerminations >= expectedTerminations => Behaviors.stopped
        case (_, Terminated(_)) => trackTerminations(numTerminations + 1, expectedTerminations)
    }
}