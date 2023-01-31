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
	 * Abstract implementation to get data from a data source
	 *
	 * @return An iterator of rows, each row being a map from field name to a table value
	 */
	def getData : TableResult

	def protobuf : data_source.DataSource
	def isCassandra : Boolean = false
	def getCassandraProtobuf : Option[data_source.CassandraDataSource] = None
