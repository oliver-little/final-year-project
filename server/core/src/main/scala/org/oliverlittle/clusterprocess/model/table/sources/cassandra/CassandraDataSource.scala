package org.oliverlittle.clusterprocess.model.table.sources.cassandra

import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.connector.grpc.{WorkerHandler, ChannelManager}
import org.oliverlittle.clusterprocess.connector.cassandra.{CassandraConfig, CassandraConnector}
import org.oliverlittle.clusterprocess.connector.cassandra.token._
import org.oliverlittle.clusterprocess.model.field.expressions.F
import org.oliverlittle.clusterprocess.model.table._
import org.oliverlittle.clusterprocess.model.table.sources.{DataSource, PartialDataSource}
import org.oliverlittle.clusterprocess.model.table.field._
import org.oliverlittle.clusterprocess.query._

import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata
import com.datastax.oss.driver.api.core.`type`.{DataTypes, DataType}
import com.datastax.oss.driver.api.core.cql.Row
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.util.Timeout
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._
import java.time.Instant
import scala.concurrent.{Future, ExecutionContext}
import com.datastax.oss.driver.api.core.cql.SimpleStatement

object CassandraDataSource:
    def inferDataSourceFromCassandra(keyspace : String, table : String)(using env : CassandraConfig {val connector : CassandraConnector} = CassandraConfig()) : CassandraDataSource  = {
        val tableMetadata : TableMetadata = env.connector.getTableMetadata(keyspace, table)
        
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
        return CassandraDataSource(env, keyspace, table, fields, partitions.toSeq, primaries.toSeq)
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
case class CassandraDataSource(env: CassandraConfig {val connector : CassandraConnector}, keyspace : String, name : String, fields: Seq[CassandraField], partitionKey : Seq[String], primaryKey : Seq[String] = Seq()) extends DataSource:
    private val logger = LoggerFactory.getLogger(classOf[CassandraDataSource].getName)    
    logger.info(name)
    // Validity Checks
    val names : Set[String] = fields.map(_.name).toSet
    if names.size != fields.size then throw new IllegalArgumentException("Field list must not contain duplicate names")
    if partitionKey.length == 0 then throw new IllegalArgumentException("Must have at least one partition key")
    if !(partitionKey.forall(names contains _) && primaryKey.forall(names contains _)) then throw new IllegalArgumentException("PartitionKey and PrimaryKey names must match field names")
    if !partitionKey.forall(primaryKey contains _) then throw new IllegalArgumentException("All partition keys must be in primary key list")

    val clusterKeys = primaryKey.diff(partitionKey)

    lazy val getHeaders : TableResultHeader = TableResultHeader(fields)
    lazy val protobuf : table_model.DataSource = table_model.DataSource().withCassandra(table_model.CassandraDataSource(keyspace=keyspace, table=name))
    lazy val isValid = true

    def getPartitions(workerHandler : WorkerHandler)(using ec : ExecutionContext) : Future[Seq[(Seq[ChannelManager], Seq[PartialDataSource])]] = {
            // Calculate the optimal allocations based on the workerHandler information
            val mappings = workerHandler.distributeWorkToNodes(env.connector, keyspace, name)
            logger.info("Generating " + mappings.map(_._2.size).sum + " partitions.")
            
            return Future.successful(
                mappings.map {(channels, partitions) => 
                    (channels, 
                    // Convert the partitions into partial data sources
                    partitions.map(PartialCassandraDataSource(this, _)))
                }
            )
        }

    def getQueryPlan : Seq[QueryPlanItem] = Seq(GetPartition(this))
    def getCleanupQueryPlan : Seq[QueryPlanItem] = Seq(DeletePartition(this))

    def toCql(ifNotExists : Boolean = true) : String = {
        val ifNotExistsString = if ifNotExists then "IF NOT EXISTS " else "";
        // Need to convert to a prepared statement
        return "CREATE TABLE " + ifNotExistsString + keyspace + "." + name + " (" + fields.map(_.toCql).reduce((l, r) => l + "," + r) + ", PRIMARY KEY " + primaryKeyBuilder + ");"
    }

    lazy val primaryKeyBuilder : String = {
        val partition = if partitionKey.length > 1 then "(" + partitionKey.reduce((l, r) => l + "," + r) + ")" else partitionKey(0)
        val primary = if clusterKeys.length > 0 then "," + clusterKeys.reduce((l, r) => l + "," + r) else ""
        "(" + partition + primary + ")"
    }

    /**
      * Creates this table in the current Cassandra database if it doesn't already exist
      */
    def createIfNotExists : Unit = env.connector.getSession.execute(toCql(true))

    /**
      * Creates this table in the current Cassandra database
      */
    def create : Unit = env.connector.getSession.execute(toCql(false))

case class PartialCassandraDataSource(parent : CassandraDataSource, tokenRanges : CassandraPartition) extends PartialDataSource:
    lazy val protobuf : table_model.PartialDataSource = table_model.PartialDataSource().withCassandra(table_model.PartialCassandraDataSource(keyspace=parent.keyspace, table=parent.name, tokenRanges=tokenRanges.protobuf))
    lazy val getDataQueries = tokenRanges.ranges.map(_.toQueryString(parent.partitionKey.reduce((l, r) => l + "," + r))).map("SELECT * FROM " + parent.keyspace + "." + parent.name + " WHERE " + _ + ";")

    /**
      * Cassandra Implementation of getData - for now this does not restrict the data received, it simply gets an entire table
      *
      * @return An iterator of rows, each row being a map from field name to a table value
      */
    def getPartialData(store : ActorRef[TableStore.TableStoreEvent], workerChannels : Seq[ChannelManager])(using t : Timeout)(using system : ActorSystem[_])(using ec : ExecutionContext = system.executionContext) : Future[TableResult] = {
        val session = parent.env.connector.getSession
        // Use custom execution profile to increase timeout, accounts for overloaded database in very large queries.
        // NOTE: This doesn't solve the problem where the timeout actually hits, need to implement some kind of exponential backoff algorithm for retries.
        val statements = getDataQueries.map(SimpleStatement.builder(_).setExecutionProfileName("getpartition").build())
        val results = statements.map(session.execute(_).asScala.map(row => parent.fields.map(_.getTableValue(row)))).reduce(_ ++ _)
        Future.successful(LazyTableResult(getHeaders, results))
    }

// Fields
trait CassandraField extends TableField:
    val fieldType : String
    def toCql : String = name + " " + fieldType
    def getTableValue(rowData : Row) : Option[TableValue]

final case class CassandraIntField(name : String) extends IntField with CassandraField:
    val fieldType = "bigint"
    def getTableValue(rowData : Row) : Option[TableValue] = if !rowData.isNull(name) then Some(IntValue(rowData.getLong(name))) else None
    def rename(newName : String) : TableField = copy(name = newName)

final case class CassandraDoubleField(name : String) extends DoubleField with CassandraField:
    val fieldType = "double"
    def getTableValue(rowData : Row) : Option[TableValue] = if !rowData.isNull(name) then Some(DoubleValue(rowData.getDouble(name))) else None
    def rename(newName : String) : TableField = copy(name = newName)

final case class CassandraStringField(name : String) extends StringField with CassandraField:
    val fieldType = "text"
    def getTableValue(rowData : Row) : Option[TableValue] = if !rowData.isNull(name) then Some(StringValue(rowData.getString(name))) else None
    def rename(newName : String) : TableField = copy(name = newName)

final case class CassandraBoolField(name : String) extends BoolField with CassandraField:
    val fieldType = "boolean"
    def getTableValue(rowData : Row) : Option[TableValue] = if !rowData.isNull(name) then Some(BoolValue(rowData.getBoolean(name))) else None
    def rename(newName : String) : TableField = copy(name = newName)

final case class CassandraDateTimeField(name : String) extends DateTimeField with CassandraField:
    val fieldType = "timestamp"
    def getTableValue(rowData : Row) : Option[TableValue] = if !rowData.isNull(name) then Some(DateTimeValue(rowData.getInstant(name))) else None
    def rename(newName : String) : TableField = copy(name = newName)
