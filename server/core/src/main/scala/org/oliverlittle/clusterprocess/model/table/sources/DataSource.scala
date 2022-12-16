package org.oliverlittle.clusterprocess.model.table.sources

import org.oliverlittle.clusterprocess.model.table.field.{TableField, TableValue}
import org.oliverlittle.clusterprocess.model.table.sources.cassandra.CassandraDataSource
import org.oliverlittle.clusterprocess.data_source
import org.oliverlittle.clusterprocess.model.table.sources.cassandra.CassandraField

object DataSource:
	def fromProtobuf(dataSource : data_source.DataSource): DataSource = dataSource.source match {
		case x if x.isCassandra => return CassandraDataSource.inferDataSourceFromCassandra(x.cassandra.get.keyspace, x.cassandra.get.table)
		case _ => throw new IllegalArgumentException("Unknown data source")
	}

trait DataSource:
	/**
	 * Abstract implementation to get the headers of a data source
	 *
	 * @return
	 */
	def getHeaders : Map[String, TableField]
	/**
	 * Abstract implementation to get data from a data source
	 *
	 * @return An iterator of rows, each row being a map from field name to a table value
	 */
	def getData : Iterable[Map[String, TableValue]]
