package org.oliverlittle.clusterprocess.query

import org.oliverlittle.clusterprocess.worker_query
import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.model.table._
import org.oliverlittle.clusterprocess.model.table.sources._
import org.oliverlittle.clusterprocess.connector.grpc.{WorkerHandler, ChannelManager}

import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.ActorContext

import scala.collection.mutable.{Queue, ArrayBuffer}
import scala.concurrent.Future
import scala.util.{Success, Failure}
import scala.concurrent.ExecutionContext

// Messages for Query Scheduler Actor System
sealed trait QueryInstruction
case class InstructionComplete(partitions : Option[Seq[(Seq[ChannelManager], Seq[PartialDataSource])]]) extends QueryInstruction
case class InstructionError(exception : Throwable) extends QueryInstruction

object QueryPlanItem:
    /**
      * Generates a set of consumers and producers from ChannelManager/Query mappings
      *
      * @param producerFactory A factory object for producers
      * @param consumerFactory A factory object for consumers
      * @param queries A list of channel/query list pairs for optimal allocation
      * @param allowQueryMixing Whether to allow channels to take work from other channels' query lists.
      * @return A list of Producers and a list of consumers
      */
    def createConsumersAndProducers(queries : Seq[(Seq[ChannelManager], Seq[worker_query.QueryPlanItem])], counter : ActorRef[Counter.CounterEvent], allowQueryMixing : Boolean = false)(using producerFactory : WorkProducerFactory)(using consumerFactory : WorkConsumerFactory)(using context : ActorContext[QueryInstruction]) : (Seq[ActorRef[WorkProducer.ProducerEvent]], Seq[ActorRef[WorkConsumer.ConsumerEvent]]) = {
        // For each sequence of requests, create a producer (with a unique identifier)
        // pair._1 is the gRPC channel
        // pair._2 is the work
        val mappedProducers = queries.zipWithIndex.map((pair, index) => (pair._1, context.spawn(producerFactory.createProducer(pair._2), "producer" + index.toString)))
        val producers = mappedProducers.map(_._2)
        // For each possible stub, and it's matched producer, create a consumer and give it a list of producers to consume from
        val consumers = if allowQueryMixing then getConsumersWithMixing(mappedProducers, counter)
            else getConsumersWithoutMixing(mappedProducers, counter)
    
        if consumers.size == 0 then throw new IllegalArgumentException("Did not create any consumers (likely because no ChannelManagers were provided), cannot continue as this will result in a stuck state.")

        return (producers, consumers)
    }

    def getConsumersWithMixing(mappedProducers : Seq[(Seq[ChannelManager], ActorRef[WorkProducer.ProducerEvent])], counter : ActorRef[Counter.CounterEvent])(using consumerFactory : WorkConsumerFactory)(using context : ActorContext[QueryInstruction]) : Seq[ActorRef[WorkConsumer.ConsumerEvent]] = {
        val producers = mappedProducers.map(_._2)
        val consumers = mappedProducers.zipWithIndex.map((pair, index) => pair._1.zipWithIndex.map((c, subIndex) => context.spawn(consumerFactory.createConsumer(c.workerComputeServiceBlockingStub, Seq(pair._2) ++ producers.filter(_ == pair._2), counter), "consumer" + index.toString + subIndex.toString))).flatten
        return consumers
    }

    def getConsumersWithoutMixing(mappedProducers : Seq[(Seq[ChannelManager], ActorRef[WorkProducer.ProducerEvent])], counter : ActorRef[Counter.CounterEvent])(using consumerFactory : WorkConsumerFactory)(using context : ActorContext[QueryInstruction]) : Seq[ActorRef[WorkConsumer.ConsumerEvent]] = {
        // Get any producers that aren't directly allocated to a Channel - these will have to be mixed anyway, or the work will not get done
        val unallocatedProducers = mappedProducers.filter((channels, producer) => channels.size == 0).map(_._2)
        val consumers = mappedProducers.zipWithIndex.map((pair, index) => pair._1.zipWithIndex.map((c, subIndex) => context.spawn(consumerFactory.createConsumer(c.workerComputeServiceBlockingStub, Seq(pair._2) ++ unallocatedProducers, counter), "consumer" + index.toString + subIndex.toString))).flatten
        return consumers
    }

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
    def execute(workerHandler : WorkerHandler, onResult : ActorRef[QueryInstruction])(using context : ActorContext[QueryInstruction])(using producerFactory : WorkProducerFactory)(using consumerFactory : WorkConsumerFactory)(using counterFactory : CounterFactory)(using ec : ExecutionContext) : Unit

    /**
      * Allows a QueryPlanItem to adjust its functionality based on the last set of available partitions
      *
      * @param partitions
      * @return
      */
    def usePartitions(partitions : Seq[(Seq[ChannelManager], Seq[PartialDataSource])]) : QueryPlanItem = this

// Calculates a Table from a set of partial tables
case class PrepareResult(table : Table) extends QueryPlanItem:
    override def execute(
        workerHandler : WorkerHandler, 
        onResult : ActorRef[QueryInstruction])
    (using context : ActorContext[QueryInstruction])
    (using producerFactory : WorkProducerFactory) 
    (using consumerFactory : WorkConsumerFactory)
    (using counterFactory : CounterFactory)
    (using ec : ExecutionContext): Unit = {
        val partitions = table.getPartialTables(workerHandler)
        val counter = context.spawn(counterFactory.createCounter(partitions.map(_._2.size).sum, () => {onResult ! InstructionComplete(None)}, (t) => {}), "counter")
        val queries = partitions.map((channel, tables) => 
            (channel, 
            tables.map(t =>
                worker_query.QueryPlanItem().withPrepareResult(
                worker_query.PrepareResult(Some(t.protobuf))
            )))
        )
        // Generate producers and consumers
        val (producers, consumers) = QueryPlanItem.createConsumersAndProducers(queries, counter, true)
    }

    override def usePartitions(partitions: Seq[(Seq[ChannelManager], Seq[PartialDataSource])]): QueryPlanItem = PrepareResultWithPartitions(table, partitions)

// Modified form of PrepareResult to execute differently using an existing set of partitions
case class PrepareResultWithPartitions(table : Table, partitions : Seq[(Seq[ChannelManager], Seq[PartialDataSource])]) extends QueryPlanItem:
    lazy val queries = partitions.map((channel, dataSources) => 
            (channel, 
            dataSources.map(ds =>
                worker_query.QueryPlanItem().withPrepareResult(
                    worker_query.PrepareResult(Some(table.withPartialDataSource(ds).protobuf))
                ) 
            ))
        )
    lazy val partitionsCount = partitions.map(_._2.size).sum

    override def execute(
        workerHandler : WorkerHandler, 
        onResult : ActorRef[QueryInstruction])
    (using context : ActorContext[QueryInstruction])
    (using producerFactory : WorkProducerFactory) 
    (using consumerFactory : WorkConsumerFactory)
    (using counterFactory : CounterFactory)
    (using ec : ExecutionContext): Unit = {
        val counter = context.spawn(counterFactory.createCounter(partitionsCount, () => {onResult ! InstructionComplete(None)}, (t) => {}), "counter")
        // Generate producers and consumers
        val (producers, consumers) = QueryPlanItem.createConsumersAndProducers(queries, counter, false)
    }

// Execute will be removing the item from the tablestore
case class DeleteResult(table : Table) extends QueryPlanItem:
    override def execute(
        workerHandler : WorkerHandler, 
        onResult : ActorRef[QueryInstruction])
    (using context : ActorContext[QueryInstruction])
    (using producerFactory : WorkProducerFactory) 
    (using consumerFactory : WorkConsumerFactory)
    (using counterFactory : CounterFactory)
    (using ec : ExecutionContext): Unit = {
        sendDeleteResult(workerHandler).onComplete(_ match {
            case Success(r) => onResult ! InstructionComplete(None)
            case Failure(e) => onResult ! InstructionError(e)
        })
    }

    def sendDeleteResult(workerHandler : WorkerHandler)(using ec : ExecutionContext) : Future[Seq[worker_query.ProcessQueryPlanItemResult]] = {
        val query = worker_query.QueryPlanItem().withDeleteResult(worker_query.DeleteResult(Some(table.protobuf)))
        val futures = workerHandler.channels.map(c => c.workerComputeServiceStub.processQueryPlanItem(query))
        return Future.sequence(futures)
    }

// Get partition data from other workers
case class GetPartition(dataSource : DataSource, partitions : Option[Seq[(Seq[ChannelManager], Seq[PartialDataSource])]] = None) extends QueryPlanItem:
    override def execute(
        workerHandler : WorkerHandler, 
        onResult : ActorRef[QueryInstruction])
    (using context : ActorContext[QueryInstruction])
    (using producerFactory : WorkProducerFactory) 
    (using consumerFactory : WorkConsumerFactory)
    (using counterFactory : CounterFactory)
    (using ec : ExecutionContext): Unit = {
        val usedPartitions = partitions.getOrElse(dataSource.getPartitions(workerHandler))

        startShuffleWorkers(workerHandler, onResult, usedPartitions)
    }

    def startShuffleWorkers(
        workerHandler : WorkerHandler, 
        onResult : ActorRef[QueryInstruction],
        partitions : Seq[(Seq[ChannelManager], Seq[PartialDataSource])]) 
    (using producerFactory : WorkProducerFactory) 
    (using consumerFactory : WorkConsumerFactory)
    (using counterFactory : CounterFactory)
    (using context : ActorContext[QueryInstruction]) : Unit = {
        val partitionsCount = partitions.map(_._2.size).sum
        val counter = context.spawn(counterFactory.createCounter(partitionsCount, () => {onResult ! InstructionComplete(Some(partitions))}, (t) => {}), "counter")
        // Parse the partitions into queries 
        val queries = partitions.map((channel, dataSources) => 
            (channel, 
            dataSources.map(ds =>
                worker_query.QueryPlanItem().withGetPartition(
                    worker_query.GetPartition(Some(ds.protobuf), workerHandler.channels.diff(channel).map(c => table_model.InetSocketAddress(c.host, c.port)))
                )
            ))
        )

        // Generate producers and consumers
        val (producers, consumers) = QueryPlanItem.createConsumersAndProducers(queries, counter, true)

        context.log.info("Generated queries and actors, expecting " + partitionsCount + " results.")
    }

    override def usePartitions(partitions: Seq[(Seq[ChannelManager], Seq[PartialDataSource])]): QueryPlanItem = GetPartition(dataSource, Some(partitions))

// Calculates the hashes of the dependencies of a DependentDataSource
case class PrepareHashes(dataSource : DependentDataSource) extends QueryPlanItem:
    override def execute(
        workerHandler : WorkerHandler, 
        onResult : ActorRef[QueryInstruction])
    (using context : ActorContext[QueryInstruction])
    (using producerFactory : WorkProducerFactory) 
    (using consumerFactory : WorkConsumerFactory)
    (using counterFactory : CounterFactory)
    (using ec : ExecutionContext): Unit = {
        val partitions = dataSource.getPartitions(workerHandler)
        val future = sendPreparedHashes(workerHandler, partitions.map(_._2.size).sum)
        future.onComplete {
            case Success(_) => onResult ! InstructionComplete(Some(partitions))
            case Failure(e) => onResult ! InstructionError(e)
        }
    }

    def sendPreparedHashes(workerHandler : WorkerHandler, partitionCount : Int)(using ec : ExecutionContext) : Future[Seq[worker_query.ProcessQueryPlanItemResult]] = {
        val query = worker_query.QueryPlanItem().withPrepareHashes(worker_query.PrepareHashes(Some(dataSource.protobuf), partitionCount))
        val futures = workerHandler.channels.map(c => c.workerComputeServiceStub.processQueryPlanItem(query))
        return Future.sequence(futures)
    }

// Deletes the hashes of the dependencies of a DependentDataSource
case class DeletePreparedHashes(dataSource : DependentDataSource, partitions : Option[Seq[(Seq[ChannelManager], Seq[PartialDataSource])]] = None) extends QueryPlanItem:
    override def execute(
        workerHandler : WorkerHandler, 
        onResult : ActorRef[QueryInstruction])
    (using context : ActorContext[QueryInstruction])
    (using producerFactory : WorkProducerFactory) 
    (using consumerFactory : WorkConsumerFactory)
    (using counterFactory : CounterFactory)
    (using ec : ExecutionContext): Unit = {
        val usedPartitions = partitions.getOrElse(dataSource.getPartitions(workerHandler))
        val future = sendDeletePreparedHashes(workerHandler, usedPartitions.map(_._2.size).sum)
        future.onComplete {
            case Success(_) => onResult ! InstructionComplete(Some(usedPartitions))
            case Failure(e) => onResult ! InstructionError(e)
        }
    }

    def sendDeletePreparedHashes(workerHandler : WorkerHandler, partitionCount : Int)(using ec : ExecutionContext) : Future[Seq[worker_query.ProcessQueryPlanItemResult]] = {
        val query = worker_query.QueryPlanItem().withDeletePreparedHashes(worker_query.DeletePreparedHashes(Some(dataSource.protobuf), partitionCount))
        val futures = workerHandler.channels.map(c => c.workerComputeServiceStub.processQueryPlanItem(query))
        return Future.sequence(futures)
    }

    override def usePartitions(partitions: Seq[(Seq[ChannelManager], Seq[PartialDataSource])]): QueryPlanItem = DeletePreparedHashes(dataSource, Some(partitions))

// Clear prepared partition data from each worker
case class DeletePartition(dataSource : DataSource) extends QueryPlanItem:
    override def execute(
        workerHandler : WorkerHandler, 
        onResult : ActorRef[QueryInstruction])
    (using context : ActorContext[QueryInstruction])
    (using producerFactory : WorkProducerFactory) 
    (using consumerFactory : WorkConsumerFactory)
    (using counterFactory : CounterFactory)
    (using ec : ExecutionContext): Unit = {
        sendDeletePartition(workerHandler).onComplete(_ match {
            case Success(r) => onResult ! InstructionComplete(None)
            case Failure(e) => onResult ! InstructionError(e)
        })
    }

    def sendDeletePartition(workerHandler : WorkerHandler)(using ec : ExecutionContext) : Future[Seq[worker_query.ProcessQueryPlanItemResult]] = {
        val query = worker_query.QueryPlanItem().withDeletePartition(worker_query.DeletePartition(Some(dataSource.protobuf)))
        val futures = workerHandler.channels.map(c => c.workerComputeServiceStub.processQueryPlanItem(query))
        return Future.sequence(futures)
    }
