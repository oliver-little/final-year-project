package org.oliverlittle.clusterprocess.query

import org.oliverlittle.clusterprocess.worker_query
import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.model.table._
import org.oliverlittle.clusterprocess.model.table.sources._
import org.oliverlittle.clusterprocess.connector.grpc.{WorkerHandler, ChannelManager}

import akka.actor.typed.scaladsl.{Behaviors, LoggerOps, ActorContext}
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.ActorContext

import scala.collection.mutable.{Queue, ArrayBuffer}
import scala.util.{Success, Failure}
import scala.concurrent.{Promise, Future, ExecutionContext}
import akka.actor.typed.DispatcherSelector

// Messages for Query Scheduler Actor System
sealed trait QueryInstruction
case class InstructionComplete() extends QueryInstruction
case class InstructionCompleteWithTableOutput(partitions : Map[ChannelManager, Seq[PartialTable]]) extends QueryInstruction
case class InstructionCompleteWithDataSourceOutput(partitions : Map[ChannelManager, Seq[PartialDataSource]]) extends QueryInstruction
case class InstructionError(exception : Throwable) extends QueryInstruction

// High level objects representing query plan items
sealed trait QueryPlanItem:
    /**
      * Generate a set of Actors which will be spawned in order, in order to execute this instruction
      *
      * @param workerHandler The list of workers
      * @param onResult An ActorRef to send the result (success or failure) to
      * @param consumerFactory A factory object for consumers
      * @param counterFactory A factory object for counters
      */
    def execute(workerHandler : WorkerHandler, onResult : ActorRef[QueryInstruction])(using context : ActorContext[QueryInstruction])(using ec : ExecutionContext) : Unit

    /**
      * Allows a QueryPlanItem to adjust its functionality based on the last set of available partitions
      *
      * @param partitions
      * @return
      */
    def usePartitions(partitions: Map[ChannelManager, Seq[PartialDataSource]]) : QueryPlanItem = this

// Calculates a Table from a set of partial tables
case class PrepareResult(table : Table) extends QueryPlanItem:
    override def execute(
        workerHandler : WorkerHandler, 
        onResult : ActorRef[QueryInstruction])
    (using context : ActorContext[QueryInstruction])
    (using ec : ExecutionContext) = executeWithFactories(workerHandler, onResult)

    def executeWithFactories(
        workerHandler : WorkerHandler, 
        onResult : ActorRef[QueryInstruction])
    (using context : ActorContext[QueryInstruction])
    (using ec : ExecutionContext)
    (using consumerFactory : PrepareResultConsumerFactory = BasePrepareResultConsumerFactory())
    (using counterFactory : PrepareResultCounterFactory = BasePrepareResultCounterFactory()) : Unit = {
        // Discover actual partitions here, then call usePartitions
        throw new IllegalStateException("No partitions provided, cannot execute PrepareResult")
    }

    override def usePartitions(partitions: Map[ChannelManager, Seq[PartialDataSource]]): QueryPlanItem = PrepareResultWithPartitions(table, partitions)

// Modified form of PrepareResult to execute differently using an existing set of partitions
case class PrepareResultWithPartitions(table : Table, partitions : Map[ChannelManager, Seq[PartialDataSource]]) extends QueryPlanItem:
    lazy val queries = partitions.map((channel, dataSources) => 
            (channel, 
            dataSources.map(ds =>
                (
                    table.withPartialDataSource(ds),
                    worker_query.QueryPlanItem().withPrepareResult(
                        worker_query.PrepareResult(Some(table.withPartialDataSource(ds).protobuf))
                    ) 
                )
            ))
        )
    lazy val partitionsCount = partitions.map(_._2.size).sum

    override def execute(
        workerHandler : WorkerHandler, 
        onResult : ActorRef[QueryInstruction])
    (using context : ActorContext[QueryInstruction])
    (using ec : ExecutionContext) = executeWithFactories(workerHandler, onResult)

    def executeWithFactories(
        workerHandler : WorkerHandler, 
        onResult : ActorRef[QueryInstruction])
    (using context : ActorContext[QueryInstruction])
    (using ec : ExecutionContext) 
    (using consumerFactory : PrepareResultConsumerFactory = BasePrepareResultConsumerFactory())
    (using counterFactory : PrepareResultCounterFactory = BasePrepareResultCounterFactory()) : Unit = {
        val promise = Promise[Map[ChannelManager, Seq[PartialTable]]]()
        val counter = context.spawn(counterFactory.createCounter(partitionsCount, promise), "counter")
        promise.future.onComplete {
            case Success(partitions) => onResult ! InstructionCompleteWithTableOutput(partitions)
            case Failure(e) => onResult ! InstructionError(e)
        }
        
        // Pair._1 is the gRPC channel
        // Pair._2 is the PartialTable/query pair list
        val consumers = queries.zipWithIndex.map((pair, index) => context.spawn(consumerFactory.createConsumer(pair._1, pair._2, counter), "consumer" + index.toString))
    }

// Execute will be removing the item from the tablestore
case class DeleteResult(table : Table, partitions : Option[Map[ChannelManager, Seq[PartialDataSource]]] = None) extends QueryPlanItem:
    override def execute(
        workerHandler : WorkerHandler, 
        onResult : ActorRef[QueryInstruction])
    (using context : ActorContext[QueryInstruction])
    (using ec : ExecutionContext): Unit = {
        sendDeleteResult(workerHandler).onComplete(_ match {
            case Success(r) =>
                partitions match {
                    case Some(p) => onResult ! InstructionCompleteWithDataSourceOutput(p)
                    case None => onResult ! InstructionComplete()
                }
            case Failure(e) => onResult ! InstructionError(e)
        })
    }

    def sendDeleteResult(workerHandler : WorkerHandler)(using ec : ExecutionContext) : Future[Seq[worker_query.ProcessQueryPlanItemResult]] = {
        val query = worker_query.QueryPlanItem().withDeleteResult(worker_query.DeleteResult(Some(table.protobuf)))
        val futures = workerHandler.channels.map(c => c.workerComputeServiceStub.processQueryPlanItem(query))
        return Future.sequence(futures)
    }

    // Take partitions to override them
    override def usePartitions(partitions: Map[ChannelManager, Seq[PartialDataSource]]) : QueryPlanItem = DeleteResult(table, Some(partitions))

// Get partition data from other workers
/*
    This QueryPlanItem creates a subworker that runs 3 steps:
        1: hash dependent tables
        2: run GetPartition for all partitions
        3: delete dependent table hashes
*/
case class GetPartition(dataSource : DataSource) extends QueryPlanItem:
    override def execute(
        workerHandler : WorkerHandler, 
        onResult : ActorRef[QueryInstruction])
    (using context : ActorContext[QueryInstruction])
    (using ec : ExecutionContext) : Unit = executeWithFactories(workerHandler, onResult)

    def executeWithFactories(
        workerHandler : WorkerHandler, 
        onResult : ActorRef[QueryInstruction])
    (using context : ActorContext[QueryInstruction])
    (using ec : ExecutionContext)
    (using producerFactory : GetPartitionProducerFactory = BaseGetPartitionProducerFactory()) 
    (using consumerFactory : GetPartitionConsumerFactory = BaseGetPartitionConsumerFactory())
    (using counterFactory : GetPartitionCounterFactory = BaseGetPartitionCounterFactory()) : Unit = {
        context.spawn(GetPartitionExecutor(dataSource, workerHandler, onResult), "GetPartitionExecutor")
    }


object GetPartitionExecutor {
    sealed trait GetPartitionExecutorEvent
    final case class GetPartitionsFinished(optimalPartitions : Seq[(Seq[ChannelManager], Seq[PartialDataSource])]) extends GetPartitionExecutorEvent
    final case class PrepareHashesFinished(optimalPartitions : Seq[(Seq[ChannelManager], Seq[PartialDataSource])]) extends GetPartitionExecutorEvent
    final case class GetPartitionFinished(partitions : Map[ChannelManager, Seq[PartialDataSource]]) extends GetPartitionExecutorEvent
    final case class DeletePreparedHashesFinished(partitions : Map[ChannelManager, Seq[PartialDataSource]]) extends GetPartitionExecutorEvent
    final case class Error(e : Throwable) extends GetPartitionExecutorEvent

    def apply(
        dataSource : DataSource, 
        workerHandler : WorkerHandler, 
        onResult : ActorRef[QueryInstruction])
        (using producerFactory : GetPartitionProducerFactory) 
        (using consumerFactory : GetPartitionConsumerFactory)
        (using counterFactory : GetPartitionCounterFactory) : Behavior[GetPartitionExecutorEvent] = Behaviors.setup { context =>
        
        implicit val executionContext : ExecutionContext = context.system.dispatchers.lookup(DispatcherSelector.fromConfig("cluster-process-dispatcher"))    

        new GetPartitionExecutor(dataSource, workerHandler, onResult, context).start()  
    }
}

class GetPartitionExecutor private (
    dataSource : DataSource,
    workerHandler : WorkerHandler, 
    onResult : ActorRef[QueryInstruction], 
    context : ActorContext[GetPartitionExecutor.GetPartitionExecutorEvent])
    (using producerFactory : GetPartitionProducerFactory) 
    (using consumerFactory : GetPartitionConsumerFactory)
    (using counterFactory : GetPartitionCounterFactory)
    (using ec : ExecutionContext) {

    import GetPartitionExecutor._

    def start() : Behavior[GetPartitionExecutorEvent] = Behaviors.setup {context => 
        val future = dataSource.getPartitions(workerHandler)

        context.pipeToSelf(future) {
            case Success(partitions) => GetPartitionsFinished(partitions)
            case Failure(e) => Error(e)
        }  

        handleEvent()
    }

    def handleEvent() : Behavior[GetPartitionExecutorEvent] = Behaviors.receive {(context, message) => 
        message match {
            case GetPartitionsFinished(optimalPartitions) =>
                val partitionsCount = optimalPartitions.map(_._2.size).sum
                // If there are dependencies, hash them first. Otherwise, move straight to calculating the PartialDataSources
                if !dataSource.getDependencies.isEmpty then 
                    context.pipeToSelf(sendPreparedHashes(dataSource, partitionsCount, workerHandler, context)) {
                        case Success(_) => PrepareHashesFinished(optimalPartitions)
                        case Failure(e) => Error(e)
                    }
                else
                    context.self ! PrepareHashesFinished(optimalPartitions)   
                Behaviors.same
            case PrepareHashesFinished(optimalPartitions) =>
                val future = sendGetPartitions(optimalPartitions, workerHandler, context)
                context.pipeToSelf(future) {
                    case Success(partitions) => GetPartitionFinished(partitions.asInstanceOf[Map[ChannelManager, Seq[PartialDataSource]]])
                    case Failure(e) => Error(e)
                }
                Behaviors.same
            case GetPartitionFinished(partitions) =>
                // If there are dependencies, clear the stored data. Otherwise, move to the last step
                if !dataSource.getDependencies.isEmpty then
                    context.pipeToSelf(sendDeletePreparedHashes(dataSource, partitions.values.map(_.size).sum, workerHandler)) {
                        case Success(_) => DeletePreparedHashesFinished(partitions)
                        case Failure(e) => Error(e)
                    }
                else 
                    context.self ! DeletePreparedHashesFinished(partitions)
                Behaviors.same
            case DeletePreparedHashesFinished(partitions) => 
                onResult ! InstructionCompleteWithDataSourceOutput(partitions)
                Behaviors.stopped
            case Error(e) => 
                onResult ! InstructionError(e)
                Behaviors.stopped
        }    
    }

    def sendPreparedHashes(dataSource : DataSource, partitionCount : Int,  workerHandler : WorkerHandler, context : ActorContext[GetPartitionExecutorEvent]) : Future[Seq[worker_query.ProcessQueryPlanItemResult]] = {
        val query = worker_query.QueryPlanItem().withPrepareHashes(worker_query.PrepareHashes(Some(dataSource.protobuf), partitionCount))
        val futures = workerHandler.channels.map(c => c.workerComputeServiceStub.processQueryPlanItem(query))
        Future.sequence(futures)
    }

    def sendGetPartitions(
        partitions : Seq[(Seq[ChannelManager], Seq[PartialDataSource])], 
        workerHandler : WorkerHandler, 
        context : ActorContext[GetPartitionExecutorEvent])
        (using producerFactory : GetPartitionProducerFactory) 
        (using consumerFactory : GetPartitionConsumerFactory)
        (using counterFactory : GetPartitionCounterFactory) : Future[Map[ChannelManager, Seq[PartialDataSource]]] = {
        val partitionsCount = partitions.map(_._2.size).sum
        val promise = Promise[Map[ChannelManager, Seq[PartialDataSource]]]()
        val counter = context.spawn(counterFactory.createCounter(partitionsCount, promise), "counter")
        
        // Parse the partitions into queries 
        val queries = partitions.map((channel, dataSources) => 
            (channel, 
            dataSources.map(ds =>
                (
                    ds,
                    worker_query.QueryPlanItem().withGetPartition(
                        worker_query.GetPartition(Some(ds.protobuf), workerHandler.channels.diff(channel).map(c => table_model.InetSocketAddress(c.host, c.port)))
                    )
                )
            ))
        )

        // Input: Pair._1 is the list of channels, Pair._2 is the (partition, request) pairs
        val mappedProducers = queries.zipWithIndex.map((pair, index) => (pair._1, context.spawn(producerFactory.createProducer(pair._2), "producer" + index.toString)))
        val producers = mappedProducers.map(_._2)
        // Input: Pair._1 is the list of channels, Pair._2 is the corresponding producer
        val consumers = mappedProducers.zipWithIndex.map((pair, index) => pair._1.zipWithIndex.map((c, subIndex) => context.spawn(consumerFactory.createConsumer(c, Seq(pair._2) ++ producers.filter(_ == pair._2), counter), "consumer" + index.toString + subIndex.toString))).flatten

        return promise.future
    }

    def sendDeletePreparedHashes(dataSource : DataSource, partitionCount : Int, workerHandler : WorkerHandler)(using ec : ExecutionContext) : Future[Seq[worker_query.ProcessQueryPlanItemResult]] = {
        val query = worker_query.QueryPlanItem().withDeletePreparedHashes(worker_query.DeletePreparedHashes(Some(dataSource.protobuf), partitionCount))
        val futures = workerHandler.channels.map(c => c.workerComputeServiceStub.processQueryPlanItem(query))
        return Future.sequence(futures)
    }
}

// Clear prepared partition data from each worker
case class DeletePartition(dataSource : DataSource) extends QueryPlanItem:
    override def execute(
        workerHandler : WorkerHandler, 
        onResult : ActorRef[QueryInstruction])
    (using context : ActorContext[QueryInstruction])
    (using ec : ExecutionContext): Unit = {
        sendDeletePartition(workerHandler).onComplete(_ match {
            case Success(r) => onResult ! InstructionComplete()
            case Failure(e) => onResult ! InstructionError(e)
        })
    }

    def sendDeletePartition(workerHandler : WorkerHandler)(using ec : ExecutionContext) : Future[Seq[worker_query.ProcessQueryPlanItemResult]] = {
        val query = worker_query.QueryPlanItem().withDeletePartition(worker_query.DeletePartition(Some(dataSource.protobuf)))
        val futures = workerHandler.channels.map(c => c.workerComputeServiceStub.processQueryPlanItem(query))
        return Future.sequence(futures)
    }
