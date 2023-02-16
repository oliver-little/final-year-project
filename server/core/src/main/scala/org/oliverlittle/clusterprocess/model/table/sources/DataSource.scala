package org.oliverlittle.clusterprocess.model.table.sources

import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.model.table._
import org.oliverlittle.clusterprocess.model.table.field.{TableField, TableValue}
import org.oliverlittle.clusterprocess.model.table.sources.cassandra._
import org.oliverlittle.clusterprocess.connector.cassandra.CassandraConnector
import org.oliverlittle.clusterprocess.connector.cassandra.token.CassandraPartition
import org.oliverlittle.clusterprocess.connector.grpc.{WorkerHandler, ChannelManager}

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
	def getPartitions(workerHandler : WorkerHandler) : Seq[(Seq[ChannelManager], Seq[PartialDataSource])]

	/**
	  * Gets the dependencies that must be calculated before this data source can be calculated
	  *
	  * @return
	  */
	def getDependencies : Seq[Table] = Seq()

	def isValid : Boolean

	def protobuf : table_model.DataSource

object PartialDataSource:
	def fromProtobuf(dataSource : table_model.PartialDataSource) : PartialDataSource = dataSource.source match {
		case table_model.PartialDataSource.Source.Cassandra(partial) => PartialCassandraDataSource(CassandraDataSource.inferDataSourceFromCassandra(partial.keyspace, partial.table), CassandraPartition.fromProtobuf(partial.tokenRanges))
		case table_model.PartialDataSource.Source.GroupBy(partial) => PartialGroupByDataSource(GroupByDataSource.fromProtobuf(partial.parent.get), partial.partition.get.partitionNumber, partial.partition.get.totalPartitions)
		case x => throw new IllegalArgumentException("Unsupported PartialDataSource: " + x.toString)
	}

trait PartialDataSource:
	val parent : DataSource

	/**
	 * Abstract implementation to get the headers of a data source
	 *
	 * @return
	 */
	def getHeaders : TableResultHeader = parent.getHeaders

	/**
	 * Abstract implementation to get partial data from a data source
	 *
	 * @return An iterator of rows, each row being a map from field name to a table value
	 */
	def getPartialData(workerChannels : Seq[ChannelManager]) : TableResult

	def protobuf : table_model.PartialDataSource
