package org.oliverlittle.clusterprocess.model.table.sources.cassandra


import org.oliverlittle.clusterprocess.connector.cassandra.CassandraConnector
import org.oliverlittle.clusterprocess.connector.cassandra.token._
import org.oliverlittle.clusterprocess.model.field.expressions.F
import org.oliverlittle.clusterprocess.model.table._
import org.oliverlittle.clusterprocess.model.table.sources.DataSource
import org.oliverlittle.clusterprocess.model.table.field._
import org.oliverlittle.clusterprocess.data_source

import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata
import com.datastax.oss.driver.api.core.`type`.{DataTypes, DataType}
import com.datastax.oss.driver.api.core.cql.Row
import scala.jdk.CollectionConverters._
import java.time.Instant
import java.util.logging.Logger

object CassandraDataSource:
    def inferDataSourceFromCassandra(connector : CassandraConnector, keyspace : String, table : String, tokenRange : Option[CassandraTokenRange] = None) : DataSource = {
        val tableMetadata : TableMetadata = connector.getTableMetadata(keyspace, table)
        
        // Map column definitions to (name, data type pairs)
        val fields = tableMetadata.getColumns.asScala.map((k, v) => v.getType match {
            case DataTypes.BIGINT => CassandraIntField(k.asInternal)
            case DataTypes.DOUBLE => CassandraDoubleField(k.asInternal)
            case DataTypes.TEXT => CassandraStringField(k.asInternal)
            case DataTypes.BOOLEAN => CassandraBoolField(k.asInternal)
            case DataTypes.TIMESTAMP => CassandraDateTimeField(k.asInternal)
            case v => throw new UnsupportedOperationException("DataType not supported: " + v)
        }).toSeq
        val partitions = tableMetadata.getPartitionKey.asScala.map(_.getName.asInternal).toSeq
        val primaries = tableMetadata.getPrimaryKey.asScala.map(_.getName.asInternal).toSeq
        return CassandraDataSource(connector, keyspace, table, fields, partitions.toSeq, primaries.toSeq, tokenRange)
    }

/**
  * Creates a Data Source using Cassandra as a backend.
  *
  * @param keyspace The Cassandra keyspace this table is in
  * @param name The name of the table within the keyspace
  * @param fieldDefinitions A list of (column name, column datatype) pairs
  * @param partitionKey A list of strings representing the partition keys for this table
  * @param primaryKey A list of strings representing the primary keys for this table
  */
case class CassandraDataSource(connector : CassandraConnector, keyspace : String, name : String, fields: Seq[CassandraField], partitionKey : Seq[String], primaryKey : Seq[String] = Seq(), tokenRange : Option[CassandraTokenRange] = None) extends DataSource:
    private val logger = Logger.getLogger(classOf[CassandraDataSource].getName)    
    logger.info(name)
    // Validity Checks
    val names : Set[String] = fields.map(_.name).toSet
    if fields.distinct.size != fields.size then throw new IllegalArgumentException("Field list must not contain duplicate names")
    if partitionKey.length == 0 then throw new IllegalArgumentException("Must have at least one partition key")
    if !(partitionKey.forall(names contains _) && primaryKey.forall(names contains _)) then throw new IllegalArgumentException("PartitionKey and PrimaryKey names must match field names")

    lazy val primaryKeyBuilder : String = {
        val partition = if partitionKey.length > 1 then "(" + partitionKey.reduce((l, r) => l + "," + r) + ")" else partitionKey(0)
        val primary = if primaryKey.length > 0 then "," + primaryKey.reduce((l, r) => l + "," + r) else ""
        "(" + partition + primary + ")"
    }

    lazy val getHeaders : TableResultHeader = TableResultHeader(fields)
    lazy val protobuf : data_source.DataSource = data_source.DataSource().withCassandra(getCassandraProtobuf.get)
    override val isCassandra : Boolean = true
    override val getCassandraProtobuf : Option[data_source.CassandraDataSource] = Some(data_source.CassandraDataSource(keyspace=keyspace, table=name))

    lazy val getDataQuery = if tokenRange.isDefined then "SELECT * FROM " + keyspace + "." + name + " WHERE " + tokenRange.get.toQueryString(partitionKey.reduce((l, r) => l + "," + r)) + ";"
        else "SELECT * FROM " + keyspace + "." + name + ";"

    /**
      * Cassandra Implementation of getData - for now this does not restrict the data received, it simply gets an entire table
      *
      * @return An iterator of rows, each row being a map from field name to a table value
      */
    def getData : TableResult = {
        logger.info(getDataQuery)
        return LazyTableResult(getHeaders, connector.getSession.execute(getDataQuery).asScala.map(row => fields.map(_.getTableValue(row))))
    }

    def toCql(ifNotExists : Boolean = true) : String = {
        val ifNotExistsString = if ifNotExists then "IF NOT EXISTS " else "";
        // Need to convert to a prepared statement
        return "CREATE TABLE " + ifNotExistsString + keyspace + "." + name + " (" + fields.map(_.toCql).reduce((l, r) => l + "," + r) + ", PRIMARY KEY " + primaryKeyBuilder + ");"
    }

    /**
      * Creates this table in the current Cassandra database if it doesn't already exist
      */
    def createIfNotExists : Unit = connector.getSession.execute(toCql(true))

    /**
      * Creates this table in the current Cassandra database
      */
    def create : Unit = connector.getSession.execute(toCql(false))
    
// Fields
trait CassandraField extends TableField:
    val fieldType : String
    def toCql : String = name + " " + fieldType
    def getTableValue(rowData : Row) : Option[TableValue]

final case class CassandraIntField(name : String) extends IntField with CassandraField:
    val fieldType = "bigint"
    def getTableValue(rowData : Row) : Option[TableValue] = if !rowData.isNull(name) then Some(IntValue(rowData.getLong(name))) else None

final case class CassandraDoubleField(name : String) extends DoubleField with CassandraField:
    val fieldType = "double"
    def getTableValue(rowData : Row) : Option[TableValue] = if !rowData.isNull(name) then Some(DoubleValue(rowData.getDouble(name))) else None

final case class CassandraStringField(name : String) extends StringField with CassandraField:
    val fieldType = "text"
    def getTableValue(rowData : Row) : Option[TableValue] = if !rowData.isNull(name) then Some(StringValue(rowData.getString(name))) else None

final case class CassandraBoolField(name : String) extends BoolField with CassandraField:
    val fieldType = "boolean"
    def getTableValue(rowData : Row) : Option[TableValue] = if !rowData.isNull(name) then Some(BoolValue(rowData.getBool(name))) else None

final case class CassandraDateTimeField(name : String) extends DateTimeField with CassandraField:
    val fieldType = "timestamp"
    def getTableValue(rowData : Row) : Option[TableValue] = if !rowData.isNull(name) then Some(DateTimeValue(rowData.getInstant(name))) else None
