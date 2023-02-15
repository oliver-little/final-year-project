package org.oliverlittle.clusterprocess.model.table.sources

import org.oliverlittle.clusterprocess.data_source
import org.oliverlittle.clusterprocess.model.table._
import org.oliverlittle.clusterprocess.model.table.field.{TableField, TableValue}
import org.oliverlittle.clusterprocess.model.table.sources.cassandra.CassandraDataSource
import org.oliverlittle.clusterprocess.model.table.sources.cassandra.CassandraField
import org.oliverlittle.clusterprocess.connector.cassandra.CassandraConnector
import org.oliverlittle.clusterprocess.connector.grpc.{WorkerHandler, ChannelManager}

object DataSource:
	def fromProtobuf(connector : CassandraConnector, dataSource : data_source.DataSource) = dataSource.source match {
		case x if x.isCassandra => CassandraDataSource.inferDataSourceFromCassandra(connector, x.cassandra.get.keyspace, x.cassandra.get.table)
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

	def protobuf : data_source.DataSource

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

	def protobuf : data_source.PartialDataSource
