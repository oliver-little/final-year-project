package org.oliverlittle.clusterprocess.scheduler

import org.oliverlittle.clusterprocess.worker_query
import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.model.table._
import org.oliverlittle.clusterprocess.model.table.sources._
import org.oliverlittle.clusterprocess.query._
import org.oliverlittle.clusterprocess.connector.grpc.{WorkerHandler, ChannelManager}

import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.ActorContext

import scala.collection.mutable.{Queue, ArrayBuffer}
import scala.concurrent.Future
import scala.util.{Success, Failure}
import scala.concurrent.ExecutionContext

object QueryPlan:
    /**
      * Creates a query plan to construct a given table
      *
      * @param output The table to generate a query plan for
      * @return A pair containing (query items to calculate the table, query items to cleanup after the table has been read)
      */
    def apply(output : Table) : (Seq[QueryPlanItem], Seq[QueryPlanItem]) = {
        // Uses a mutable List and Queue
        val queue : Queue[Seq[QueryableObject]] = Queue(getDependencyTree(output)*)
        val list : ArrayBuffer[QueryPlanItem] = ArrayBuffer()

        var nextPostItems : Seq[QueryPlanItem] = Seq()
        while (!queue.isEmpty) {
            // The current set of dependencies to calculate
            val queryObjects : Seq[QueryableObject] = queue.dequeue
            // The post clean-up items for this set of dependencies
            var postItems : Seq[QueryPlanItem] = Seq()
            
            queryObjects.foreach(qo =>
                // Add on the items for the query plan
                list ++= qo.queryItems
                // Add to the set of clean-up items
                postItems ++= qo.cleanupItems
            )

            // Add the clean-up items to the query plan
            list ++= nextPostItems
            // Store the current clean-up items as the next ones
            nextPostItems = postItems
        }

        // Return as immutable List
        return (list.toList, nextPostItems)
    }

    /**
      * Generates the dependency tree for a given table
      * This uses a breadth-first approach, where all tables at depth n - 1 are calculated and held in memory, then depth n is calculated, then depth n - 1 is deleted from memory.
      * A depth-first approach would likely be more efficient, but at the moment it is not possible for any data source to have more than one dependency, so there is no impact right now.
      *
      * @param table
      * @return A list of list of QueryableObjects, which should be evaluated in order - items in the same sub-Seq are dependent at the same time, and cleanup should be run at the end of the next Seq
      */
    def getDependencyTree(table : Table) : Seq[Seq[QueryableObject]] = {
        val executionOrder : ArrayBuffer[Seq[QueryableObject]] = ArrayBuffer()
        var next : Option[Seq[Table]] = Some(Seq(table))
        while (next.isDefined) {
            val tables = next.get
            // For each table in this set of dependencies, add the table, and its data source as dependencies
            executionOrder += tables.flatMap(t => Seq(QueryableTable(t), QueryableDataSource(t.dataSource)))
            
            // Get all dependencies from all tables at this 
            val deps = tables.flatMap(t => t.dataSource.getDependencies)
            next = if deps.size > 0 then Some(deps) else None
        }

        // Reverse the list before returning, because we've actually found the tree from the goal back to the roots
        return executionOrder.reverse.toList
    }

// Helper classes for generating query plans
trait QueryableObject:
    def queryItems : Seq[QueryPlanItem]
    def cleanupItems : Seq[QueryPlanItem]

    given Conversion[Table, QueryableTable] with
        def apply(t : Table) : QueryableTable = QueryableTable(t)

    given Conversion[DataSource, QueryableDataSource] with
        def apply(d : DataSource) : QueryableDataSource = QueryableDataSource(d)
        
case class QueryableTable(t : Table) extends QueryableObject:
    val queryItems = Seq(PrepareResult(t))
    val cleanupItems = Seq(DeleteResult(t))

case class QueryableDataSource(d : DataSource) extends QueryableObject:
    val queryItems = Seq(GetPartition(d))
    val cleanupItems = Seq(DeletePartition(d))


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
      * @param producerFactory A factory object for producers
      * @param consumerFactory A factory object for consumers
      * @param counterFactory A factory object for counters
      */
    def execute(workerHandler : WorkerHandler, onResult : ActorRef[QueryInstruction])(context : ActorContext[QueryInstruction])(using producerFactory : WorkProducerFactory)(using consumerFactory : WorkConsumerFactory)(using counterFactory : CounterFactory)(using ec : ExecutionContext) : Unit

    /**
      * Allows a QueryPlanItem to adjust its functionality based on the last set of available partitions
      *
      * @param partitions
      * @return
      */
    def usePartitions(partitions : Seq[(Seq[ChannelManager], Seq[PartialDataSource])]) : QueryPlanItem = this

// Execute will be calculating the table from data source and storing it back in the tablestore
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
        val (producers, consumers) = QueryPlanItem.createConsumersAndProducers(queries, counter, true)
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
case class GetPartition(dataSource : DataSource) extends QueryPlanItem:
    override def execute(
        workerHandler : WorkerHandler, 
        onResult : ActorRef[QueryInstruction])
    (using context : ActorContext[QueryInstruction])
    (using producerFactory : WorkProducerFactory) 
    (using consumerFactory : WorkConsumerFactory)
    (using counterFactory : CounterFactory)
    (using ec : ExecutionContext): Unit = {
        val partitions = dataSource.getPartitions(workerHandler)
        val futures = sendPreparePartitions(workerHandler, partitions.map(_._2.size).sum)

        futures.onComplete(_ match {
            case Success(r) => startShuffleWorkers(workerHandler, onResult, partitions)
            case Failure(e) => 
                onResult ! InstructionError(e)
        })
    }

    def sendPreparePartitions(workerHandler : WorkerHandler, partitionCount : Int)(using ec : ExecutionContext) : Future[Seq[worker_query.ProcessQueryPlanItemResult]] = {
        val query = worker_query.QueryPlanItem().withPreparePartition(worker_query.PreparePartition(Some(dataSource.protobuf), partitionCount))
        val futures = workerHandler.channels.map(c => c.workerComputeServiceStub.processQueryPlanItem(query))
        return Future.sequence(futures)
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
    }

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
