package org.oliverlittle.clusterprocess.model.table.sources

import org.oliverlittle.clusterprocess.data_source
import org.oliverlittle.clusterprocess.model.table._
import org.oliverlittle.clusterprocess.model.table.field.{TableField, TableValue}
import org.oliverlittle.clusterprocess.model.table.sources.cassandra.CassandraDataSource
import org.oliverlittle.clusterprocess.model.table.sources.cassandra.CassandraField
import org.oliverlittle.clusterprocess.connector.cassandra.CassandraConnector

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
	def getPartitions : Seq[PartialDataSource]

	def protobuf : data_source.DataSource

trait PartialDataSource:
	/**
	 * Abstract implementation to get the headers of a data source
	 *
	 * @return
	 */
	def getHeaders : TableResultHeader

	/**
	 * Abstract implementation to get partial data from a data source
	 *
	 * @return An iterator of rows, each row being a map from field name to a table value
	 */
	def getData : TableResult
