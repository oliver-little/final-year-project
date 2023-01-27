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
    sealed trait AssemblerEvent
    final case class SendResult(result : TableResult) extends AssemblerEvent
    final case class GetResult(replyTo : ActorRef[ResultData]) extends AssemblerEvent

    final case class ResultData(result : Option[TableResult])

    

    def startFromData(channelTokenRangeMap : Seq[(Seq[ChannelManager], Seq[CassandraTokenRange])], table : Table, resultCallback : TableResult => Unit) : Unit = {
        val dataSourceProtobuf = table.dataSource.getCassandraProtobuf.get
        val tableProtobuf = table.protobuf

        val parsedItems = channelTokenRangeMap.map((channels : Seq[ChannelManager], tokenRanges : Seq[CassandraTokenRange]) => 
            (
                channels.map(manager => worker_query.WorkerComputeServiceGrpc.blockingStub(manager.channel)), 
                tokenRanges.map(tokenRange => worker_query.ComputePartialResultCassandraRequest(table=Some(tableProtobuf), dataSource=Some(dataSourceProtobuf), tokenRange=Some(tokenRange.protobuf)))
            )
        )

        startExecution(parsedItems, resultCallback : TableResult => Unit)
    }

    def startExecution(items : Seq[(Seq[worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceBlockingStub], Seq[worker_query.ComputePartialResultCassandraRequest])], resultCallback : TableResult => Unit) : Unit = {
        val system = ActorSystem(WorkExecutionScheduler(items, resultCallback), "WorkExecutionScheduler")
    }

    def apply(items : Seq[(Seq[worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceBlockingStub], Seq[worker_query.ComputePartialResultCassandraRequest])], resultCallback : TableResult => Unit): Behavior[AssemblerEvent] = Behaviors.setup{context =>
        // For each sequence of requests, create a producer (with a unique identifier)
        val mappedProducers = items.zipWithIndex.map((pair, index) => pair._1 -> context.spawn(WorkProducer(pair._2), "producer" + index.toString))
        val producers = mappedProducers.map(_._2)
        // For each possible stub, and it's matched producer, create a consumer and give it a list of producers to consume from
        val consumers = mappedProducers.map((stubs, producerRef) => stubs.zipWithIndex.map((stub, index) => context.spawn(WorkConsumer(stub, producerRef +: producers.filter(_ == producerRef), context.self), "consumer" + index.toString))).flatten
        
        assembleData(None, 0, consumers.size, resultCallback)
    }

    def assembleData(currentData : Option[TableResult], numResponses : Int, expectedResponses : Int, resultCallback : TableResult => Unit, assembler : (TableResult, TableResult) => TableResult = (l, r) => l ++ r) : Behavior[AssemblerEvent] = Behaviors.receiveMessage{
        case SendResult(newResult) if numResponses >= expectedResponses => 
            resultCallback(currentData.get)
            Behaviors.stopped
        case SendResult(newResult) if currentData.isEmpty => assembleData(Some(newResult), numResponses + 1, expectedResponses, resultCallback, assembler)
        case SendResult(newResult) => assembleData(Some(assembler(currentData.get, newResult)), numResponses + 1, expectedResponses, resultCallback, assembler)
        case GetResult(replyTo) => 
            replyTo ! ResultData(currentData)
            Behaviors.same
    }
}