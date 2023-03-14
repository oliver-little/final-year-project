package org.oliverlittle.clusterprocess.model.table.sources

import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.worker_query
import org.oliverlittle.clusterprocess.model.table._
import org.oliverlittle.clusterprocess.model.field.expressions._
import org.oliverlittle.clusterprocess.connector.grpc.{WorkerHandler, ChannelManager, StreamedTableResultCompiler}
import org.oliverlittle.clusterprocess.query._

import akka.actor.typed.{ActorRef, ActorSystem}
import akka.actor.typed.scaladsl.AskPattern._
import akka.util.Timeout

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.util.Try
import scala.util.hashing.MurmurHash3
import scala.collection.MapView
import scala.concurrent.{Future, Promise, ExecutionContext}

object GroupByDataSource:
    def fromProtobuf(dataSource : table_model.GroupByDataSource) = GroupByDataSource(Table.fromProtobuf(dataSource.table.get), dataSource.uniqueFields.map(NamedFieldExpression.fromProtobuf(_)), dataSource.aggregateFields.map(AggregateExpression.fromProtobuf(_)))

case class GroupByDataSource(source : Table, uniqueFields : Seq[NamedFieldExpression], aggregates : Seq[AggregateExpression]) extends DependentDataSource:
    private val logger = LoggerFactory.getLogger(classOf[GroupByDataSource].getName) 
    
    // Same as in DataSource definition
    lazy val getHeaders : TableResultHeader = TableResultHeader(uniqueFields.map(_.outputTableField(source.outputHeaders)) ++ aggregates.flatMap(_.outputFinalTableFields(source.outputHeaders)))
    override val getDependencies: Seq[Table] = Seq(source)
    lazy val isValid = source.isValid(false) && Try{uniqueFields.map(_.resolve(source.outputHeaders))}.isSuccess && Try{aggregates.map(_.outputPartialTableFields(source.outputHeaders))}.isSuccess

    def getPartitions(workerHandler : WorkerHandler)(using ec : ExecutionContext) : Future[Seq[(Seq[ChannelManager], Seq[PartialDataSource])]] = 
        // Get the number of partitions
        workerHandler.getNumPartitionsForTable(source).map {totalPartitions =>
            // Calculate all partial data sources
            val partials = (0 until totalPartitions).map(PartialGroupByDataSource(this, _, totalPartitions))
            // Get the numbers of groups by the number of channels
            val numGroups = workerHandler.channels.size
            val groupedPartials = partials.groupBy(_.partitionNum % numGroups)
            val mappings = workerHandler.channels.zipWithIndex.map((channel, index) => (Seq(channel), groupedPartials.getOrElse(index, Seq())))
            logger.info("Generating " + mappings.map(_._2.size).sum + " partitions.")
            mappings
        }
    

    def getQueryPlan : Seq[QueryPlanItem] = source.getQueryPlan ++ Seq(GetPartition(this)) ++ source.getCleanupQueryPlan
    def getCleanupQueryPlan : Seq[QueryPlanItem] = Seq(DeletePartition(this))

    def hashPartitionedData(result : TableResult, numPartitions : Int) : Map[Int, TableResult] = {
        val resolved = uniqueFields.map(_.resolve(result.header))
        // Hash every row by the unique fields, then convert to a result
        return result.rows.groupBy(row => Math.floorMod(MurmurHash3.unorderedHash(resolved.map(_.evaluate(row))), numPartitions)).view.mapValues(LazyTableResult(result.header, _)).toMap
    }

    lazy val groupByProtobuf : table_model.GroupByDataSource = table_model.GroupByDataSource(Some(source.protobuf), uniqueFields.map(_.protobuf), aggregates.map(_.protobuf))
    // Same as in DataSource definition, this will just become a new kind of data source
    lazy val protobuf : table_model.DataSource = table_model.DataSource().withGroupBy(groupByProtobuf)



case class PartialGroupByDataSource(parent : GroupByDataSource, partitionNum : Int, totalPartitions : Int) extends PartialDataSource:
    private val logger = LoggerFactory.getLogger("PartialGroupByDataSource")

    lazy val protobuf : table_model.PartialDataSource = table_model.PartialDataSource().withGroupBy(table_model.PartialGroupByDataSource(
        Some(parent.groupByProtobuf), 
        Some(table_model.PartitionInformation(partitionNum, totalPartitions))
    ))

    def getPartialData(store : ActorRef[TableStore.TableStoreEvent], workerChannels : Seq[ChannelManager])(using t : Timeout)(using system : ActorSystem[_])(using ec : ExecutionContext = system.executionContext) : Future[TableResult] = {
        logger.info("Fetching data for GroupByDataSource partition " + partitionNum.toString + " of " + totalPartitions.toString)
        
        // This version of this script only works for one dependent table, but should be relatively straightforward to loop over
        // all dependencies to get multiple out
        val promises : Seq[Future[Option[TableResult]]]= workerChannels.map(getHashedPartitionData(parent.source, _).future) // Get data from other workers
        val localData : Future[Option[TableResult]] = store.ask[Option[TableResult]](ref => TableStore.GetHash(parent.source, totalPartitions, partitionNum, ref)) // Get data from our

        // Once we've got the data from the workers, we need to actually run the group by       
        return Future.sequence(promises ++ Seq(localData)) // Concatenate all the data we are waiting on into one future
            .map{results => 
                val infoString =
                    "GroupByDataSource partition " + partitionNum.toString + " of " + totalPartitions.toString + ": \n" +
                    results.map(_ match {
                    case Some(value) => value.rows.size.toString + " rows."
                    case None => "no table present."
                }).reduce(_ + "\n" + _)  
                logger.info(infoString)             
                results.flatten.reduceOption(_ ++ _) match {
                    case Some(tableResult) => performGroupBy(tableResult) // If we got some data, perform the group by
                    case None => parent.getHeaders.empty // If there was no data, this partition is empty - return an empty TableResult
                }
            } 
    }

    def getHashedPartitionData(dependency : Table, channel : ChannelManager)(using ec : ExecutionContext) : Promise[Option[TableResult]] = {
        val promise = Promise[Option[TableResult]]()
        channel.workerComputeServiceStub.getHashedPartitionData(worker_query.GetHashedPartitionDataRequest(Some(dependency.protobuf), totalPartitions, partitionNum), StreamedTableResultCompiler(promise))
        return promise
    }

    def performGroupBy(result : TableResult) : TableResult = 
        if parent.aggregates.size == 0 then groupByNoAggregates(result)
        else groupByWithAggregates(result)
        
    
    private def groupByWithAggregates(result : TableResult) : TableResult = {
        val resolvedUniqueFields = parent.uniqueFields.map(_.resolve(result.header))

        val aggregateFinals = parent.aggregates.map(_.resolveToFinal(result.header))

        val rows = result.rows
            .groupBy(row => resolvedUniqueFields.map(_.evaluate(row)))
            .map((uniqueValues, row) => 
                uniqueValues ++ 
                (aggregateFinals.map(_(row)).flatten)
            )
        return LazyTableResult(parent.getHeaders, rows)
    }

    private def groupByNoAggregates(result : TableResult) : TableResult = {
        val groupByHeaderFields = parent.uniqueFields.map(_.outputTableField(result.header))
        val resolvedUniqueFields = parent.uniqueFields.map(_.resolve(result.header))
        val rows = result.rows.groupBy(row => resolvedUniqueFields.map(_.evaluate(row))).keys
        return LazyTableResult(TableResultHeader(groupByHeaderFields), rows)
    }