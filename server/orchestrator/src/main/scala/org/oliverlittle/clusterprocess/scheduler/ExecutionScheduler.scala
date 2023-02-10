package org.oliverlittle.clusterprocess.scheduler

import org.oliverlittle.clusterprocess.connector.grpc.ChannelManager
import org.oliverlittle.clusterprocess.connector.cassandra.token._
import org.oliverlittle.clusterprocess.model.table.{Table, TableResult, LazyTableResult}
import org.oliverlittle.clusterprocess.worker_query

import akka.actor.typed.scaladsl.{Behaviors, LoggerOps, ActorContext}
import akka.actor.typed.{ActorRef, ActorSystem, Behavior, Terminated}

object WorkExecutionScheduler {
    sealed trait AssemblerEvent
    final case class SendResult(result : TableResult) extends AssemblerEvent

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

        startExecution(parsedItems, table.transformations.last.assemblePartial, resultCallback : TableResult => Unit)
    }

    def startExecution(items : Seq[(Seq[worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceBlockingStub], Seq[worker_query.ComputePartialResultCassandraRequest])], assembler : Iterable[TableResult] => TableResult, resultCallback : TableResult => Unit) : Unit = {
        val system = ActorSystem(WorkExecutionScheduler(items, assembler, resultCallback), "WorkExecutionScheduler")
    }

    def apply(items : Seq[(Seq[worker_query.WorkerComputeServiceGrpc.WorkerComputeServiceBlockingStub], Seq[worker_query.ComputePartialResultCassandraRequest])], assembler : Iterable[TableResult] => TableResult, resultCallback : TableResult => Unit): Behavior[AssemblerEvent] = Behaviors.setup {context =>
        // For each sequence of requests, create a producer (with a unique identifier)
        val mappedProducers = items.zipWithIndex.map((pair, index) => pair._1 -> context.spawn(WorkProducer(pair._2), "producer" + index.toString))
        val producers = mappedProducers.map(_._2)
        // For each possible stub, and it's matched producer, create a consumer and give it a list of producers to consume from
        val consumers = mappedProducers.zipWithIndex.map((pair, mainIndex) => pair._1.zipWithIndex.map((stub, index) => context.spawn(WorkConsumer(stub, pair._2 +: producers.filter(_ == pair._2), context.self), "consumer" + mainIndex.toString + index.toString))).flatten
        new WorkExecutionScheduler(items.map(_._2.size).sum, resultCallback, assembler, context).getFirstData()
    }
}

class WorkExecutionScheduler private (expectedResponses : Int, resultCallback : TableResult => Unit, assembler : Iterable[TableResult] => TableResult, context : ActorContext[WorkExecutionScheduler.AssemblerEvent]) {
    import WorkExecutionScheduler._

    /**
     * Handles receiving the first response from any consumer
     */
    private def getFirstData() : Behavior[WorkExecutionScheduler.AssemblerEvent] = Behaviors.receiveMessage {
        case SendResult(newResult) => 
            if expectedResponses == 1 then 
                onFinished(newResult)
            else assembleData(Seq(newResult), 1)
    }

    /**
     *  Handles receiving all subsequent responses from consumers until expectedResponses is reached
     */
    private def assembleData(currentData : Seq[TableResult], numResponses : Int) : Behavior[WorkExecutionScheduler.AssemblerEvent] = Behaviors.receiveMessage {
        case SendResult(newResult) => 
            if numResponses + 1 == expectedResponses then
                onFinished(assembler(currentData :+ newResult))
            else
                context.log.info("Got " + (numResponses + 1).toString + " responses, need " + expectedResponses.toString + " responses")
                assembleData(currentData :+ newResult, numResponses + 1)
    }

    /**
     * Handles system shutdown - calls the callback, terminates the system, and stops this Actor 
     */
    private def onFinished(result : TableResult) : Behavior[WorkExecutionScheduler.AssemblerEvent] = {
        context.log.info("Received all responses, calling callback function.")
        resultCallback(result)
        context.system.terminate()
        Behaviors.stopped
    }
}