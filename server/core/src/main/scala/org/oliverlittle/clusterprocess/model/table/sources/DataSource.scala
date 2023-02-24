package org.oliverlittle.clusterprocess.model.table.sources

import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.model.table._
import org.oliverlittle.clusterprocess.model.table.field.{TableField, TableValue}
import org.oliverlittle.clusterprocess.model.table.sources.cassandra._
import org.oliverlittle.clusterprocess.connector.cassandra.CassandraConnector
import org.oliverlittle.clusterprocess.connector.cassandra.token.CassandraPartition
import org.oliverlittle.clusterprocess.connector.grpc.{WorkerHandler, ChannelManager}
import org.oliverlittle.clusterprocess.query.QueryPlanItem

import akka.actor.typed.{ActorRef, ActorSystem}
import akka.util.Timeout

import scala.collection.MapView
import scala.concurrent.{Future, ExecutionContext}

object DataSource:
	def fromProtobuf(dataSource : table_model.DataSource) = dataSource.source match {
		case table_model.DataSource.Source.Cassandra(source) => CassandraDataSource.inferDataSourceFromCassandra(source.keyspace, source.table)
		case table_model.DataSource.Source.GroupBy(source) => GroupByDataSource.fromProtobuf(source)
		case _ => throw new IllegalArgumentException("Unknown data source")
	} 

trait DataSource:
	/**
	 * Abstract implementation to get the headers of a data source
	 *
	 * @return
	 */
	def getHeaders : TableResultHeader

	/**
	  * Given a list of worker channels, calculates optimal partitions for each worker
	  *
	  * @return
	  */
	def getPartitions(workerHandler : WorkerHandler)(using ec : ExecutionContext) : Future[Seq[(Seq[ChannelManager], Seq[PartialDataSource])]]

	/**
	  * Gets the dependencies that must be calculated before this data source can be calculated
	  * If this object has any dependencies, it must be an instance of DependentDataSource too
	  *
	  * @return
	  */
	def getDependencies : Seq[Table] = Seq()

	/**
	  * Generates the high level query plan to create this DataSource in the TableStore
	  *
	  * @return
	  */
	def getQueryPlan : Seq[QueryPlanItem]

	/**
	  * Generates the high level query plan to cleanup this DataSource in the TableStore
	  *
	  * @return
	  */
	def getCleanupQueryPlan : Seq[QueryPlanItem]

	def isValid : Boolean

	def protobuf : table_model.DataSource

object DependentDataSource:
	def fromProtobuf(dataSource : table_model.DataSource) : DependentDataSource = dataSource.source match {
		case table_model.DataSource.Source.GroupBy(source) => GroupByDataSource.fromProtobuf(source)
		case _ => throw new IllegalArgumentException("Unknown data source")
	} 

trait DependentDataSource extends DataSource:
	def hashPartitionedData(result : TableResult, numPartitions : Int) : MapView[Int, TableResult]

object PartialDataSource:
	def fromProtobuf(dataSource : table_model.PartialDataSource) : PartialDataSource = dataSource.source match {
		case table_model.PartialDataSource.Source.Cassandra(partial) => PartialCassandraDataSource(CassandraDataSource.inferDataSourceFromCassandra(partial.keyspace, partial.table), CassandraPartition.fromProtobuf(partial.tokenRanges))
		case table_model.PartialDataSource.Source.GroupBy(partial) => PartialGroupByDataSource(GroupByDataSource.fromProtobuf(partial.parent.get), partial.partition.get.partitionNumber, partial.partition.get.totalPartitions)
		case x => throw new IllegalArgumentException("Unsupported PartialDataSource: " + x.toString)
	}

trait PartialDataSource extends PartitionElement:
	val parent : DataSource

	/**
	 * Abstract implementation to get the headers of a data source
	 *
	 * @return
	 */
	def getHeaders : TableResultHeader = parent.getHeaders

	/**
	 * Generates the partial dataset for this PartialDataSource
	 * 
	 * @param store A TableStore instance
	 * @param workerChannels A list of other workers in the system
	 * @return An iterator of rows, each row being a map from field name to a table value
	 */
	def getPartialData(store : ActorRef[TableStore.TableStoreEvent], workerChannels : Seq[ChannelManager])(using t : Timeout)(using system : ActorSystem[_])(using ec : ExecutionContext = system.executionContext) : Future[TableResult]

	def protobuf : table_model.PartialDataSource
