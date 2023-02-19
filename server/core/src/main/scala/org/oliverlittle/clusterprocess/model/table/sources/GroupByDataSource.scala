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

import scala.util.Try
import scala.util.hashing.MurmurHash3
import scala.collection.MapView
import scala.concurrent.{Future, Promise, ExecutionContext}

object GroupByDataSource:
    def fromProtobuf(dataSource : table_model.GroupByDataSource) = GroupByDataSource(Table.fromProtobuf(dataSource.table.get), dataSource.uniqueFields.map(NamedFieldExpression.fromProtobuf(_)), dataSource.aggregateFields.map(AggregateExpression.fromProtobuf(_)))

case class GroupByDataSource(source : Table, uniqueFields : Seq[NamedFieldExpression], aggregates : Seq[AggregateExpression]) extends DependentDataSource:
    // Same as in DataSource definition
    lazy val getHeaders : TableResultHeader = TableResultHeader(uniqueFields.map(_.outputTableField(source.outputHeaders)) ++ aggregates.flatMap(_.outputTableFields(source.outputHeaders)))
    override val getDependencies: Seq[Table] = Seq(source)
    lazy val isValid = source.isValid && Try{uniqueFields.map(_.resolve(source.outputHeaders))}.isSuccess

    // New function, gives it a list of workers and their channels and requests some partitions/partition data back
    // Partitions will need to be an interface of some kind to handle both Cassandra and internal representations
    // -- Partitions able to calculate themselves? (given the correct dependencies)
    def getPartitions(workerHandler : WorkerHandler) : Seq[(Seq[ChannelManager], Seq[PartialDataSource])]  = Seq((Seq(), Seq(PartialGroupByDataSource(this, 1, 1))))

    def getQueryPlan : Seq[QueryPlanItem] = source.getQueryPlan ++ Seq(GetPartitionWithDependencies(this)) ++ source.getCleanupQueryPlan
    def getCleanupQueryPlan : Seq[QueryPlanItem] = Seq(DeletePartition(this))

    def hashPartitionedData(result : TableResult, numPartitions : Int) : MapView[Int, TableResult] = {
        val header = TableResultHeader(uniqueFields.map(_.outputTableField(result.header)))
        val resolved = uniqueFields.map(_.resolve(result.header))
        // Hash every row by the unique fields, then convert to a result
        return result.rows.groupBy(row => MurmurHash3.unorderedHash(resolved.map(_.evaluate(row))) % numPartitions).mapValues(LazyTableResult(header, _))
    }

    lazy val groupByProtobuf : table_model.GroupByDataSource = table_model.GroupByDataSource(Some(source.protobuf), uniqueFields.map(_.protobuf), aggregates.map(_.protobuf))
    // Same as in DataSource definition, this will just become a new kind of data source
    lazy val protobuf : table_model.DataSource = table_model.DataSource().withGroupBy(groupByProtobuf)



case class PartialGroupByDataSource(parent : GroupByDataSource, partitionNum : Int, totalPartitions : Int) extends PartialDataSource:
    lazy val protobuf : table_model.PartialDataSource = table_model.PartialDataSource().withGroupBy(table_model.PartialGroupByDataSource(
        Some(parent.groupByProtobuf), 
        Some(table_model.PartitionInformation(partitionNum, totalPartitions))
    ))

    def getPartialData(store : ActorRef[TableStore.TableStoreEvent], workerChannels : Seq[ChannelManager])(using t : Timeout)(using system : ActorSystem[_])(using ec : ExecutionContext = system.executionContext) : Future[TableResult] = {
        // This version of this script only works for one dependent table, but should be relatively straightforward to loop over
        // all dependencies to get multiple out
        val promises : Seq[Future[Option[TableResult]]]= workerChannels.map(getHashedPartitionData(parent.source, _).future) // Get data from other workers
        val localData : Future[Option[TableResult]] = store.ask(ref => TableStore.GetHash(parent.source, totalPartitions, partitionNum, ref)) // Get data from our

        // Once we've got the data from the workers, we need to actually run the group by       
        return Future.sequence(promises :+ localData) // Concatenate all the data we are waiting on into one future
            .map(results => performGroupBy(results.flatten.reduce(_ ++ _))) // Reduce the smaller results into one large table, and perform the group by
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
        val groupByHeaderFields = parent.uniqueFields.map(_.outputTableField(result.header))
        val resolvedUniqueFields = parent.uniqueFields.map(_.resolve(result.header))

        val aggregateHeaderFields = parent.aggregates.map(_.outputFinalTableFields(result.header)).flatten
        val aggregateFinals = parent.aggregates.map(_.resolveToFinal(result.header))

        val header = TableResultHeader(groupByHeaderFields ++ aggregateHeaderFields)
        val rows = result.rows
            .groupBy(row => resolvedUniqueFields.map(_.evaluate(row)))
            .map((uniqueValues, row) => 
                uniqueValues ++ 
                (aggregateFinals.map(_(row)).flatten)
            )
        return LazyTableResult(header, rows)
    }

    private def groupByNoAggregates(result : TableResult) : TableResult = {
        val groupByHeaderFields = parent.uniqueFields.map(_.outputTableField(result.header))
        val resolvedUniqueFields = parent.uniqueFields.map(_.resolve(result.header))
        val rows = result.rows.groupBy(row => resolvedUniqueFields.map(_.evaluate(row))).keys
        return LazyTableResult(TableResultHeader(groupByHeaderFields), rows)
    }