package org.oliverlittle.clusterprocess.model.table.sources.cassandra


import org.oliverlittle.clusterprocess.cassandra.CassandraConnector
import org.oliverlittle.clusterprocess.model.field.expressions.F
import org.oliverlittle.clusterprocess.model.table._
import org.oliverlittle.clusterprocess.model.table.sources.DataSource
import org.oliverlittle.clusterprocess.model.table.field._

import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata
import com.datastax.oss.driver.api.core.`type`.{DataTypes, DataType}
import com.datastax.oss.driver.api.core.cql.Row
import scala.jdk.CollectionConverters._
import java.time.Instant

object CassandraDataSource:
    def getTableMetadata(keyspace : String, table : String) : TableMetadata = {
        val ksObj = CassandraConnector.getSession.getMetadata.getKeyspace(keyspace)
        if ksObj.isPresent then
            val tableObj = ksObj.get.getTable(table)
            if tableObj.isPresent then
                return tableObj.get
            throw new IllegalArgumentException("Table " + keyspace + "." + table + " not found.")
        throw new IllegalArgumentException("Keyspace " + keyspace + "not found.")
    }

    def inferTableFromCassandra(keyspace : String, table : String) : Table = {
        val tableMetadata : TableMetadata = CassandraDataSource.getTableMetadata(keyspace, table)
        
        // Map column definitions to (name, data type pairs)
        val fields = tableMetadata.getColumns.asScala.map((k, v) => v.getType match {
            case DataTypes.BIGINT => CassandraIntField(k.asInternal)
            case DataTypes.DOUBLE => CassandraDoubleField(k.asInternal)
            case DataTypes.TEXT => CassandraStringField(k.asInternal)
            case DataTypes.BOOLEAN => CassandraBoolField(k.asInternal)
            case DataTypes.TIMESTAMP => CassandraDateTimeField(k.asInternal)
            case v => throw new UnsupportedOperationException("DataType not supported: " + v)
        }).toSeq
        val partitions = tableMetadata.getPartitionKey.asScala.map(_.getName.asInternal)
        val primaries = tableMetadata.getPrimaryKey.asScala.map(_.getName.asInternal)

        return Table(CassandraDataSource(keyspace, table, fields, partitions.toSeq, primaries.toSeq))
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
class CassandraDataSource(keyspace : String, name : String, fields: Seq[CassandraField], partitionKey : Seq[String], primaryKey : Seq[String] = Seq()) extends DataSource:
    val names : Set[String] = fields.map(_.name).toSet
    // Validity Checks
    if partitionKey.length == 0 then throw new IllegalArgumentException("Must have at least one partition key")
    if !(partitionKey.forall(names contains _) && primaryKey.forall(names contains _)) then throw new IllegalArgumentException("PartitionKey and PrimaryKey names must match field names")

    lazy val primaryKeyBuilder : String = {
        val partition = if partitionKey.length > 1 then "(" + partitionKey.reduce((l, r) => l + "," + r) + ")" else partitionKey(0)
        val primary = if primaryKey.length > 0 then "," + primaryKey.reduce((l, r) => l + "," + r) else ""
        "(" + partition + primary + ")"
    }

    /**
      * Cassandra Implementation of getData - for now this does not restrict the data received, it simply gets an entire table
      *
      * @return An iterator of rows, each row being a map from field name to a table value
      */
    def getData : Iterable[Map[String, TableValue]] = CassandraConnector.getSession.execute("SELECT * FROM " + keyspace + "." + name) // Gets all records from the table in Cassandra
        .asScala.map( // Converts to a Scala Iterable, then for each row
            row => fields.map( // For each field in that row
                f => f.name -> f.getTableValue(row)) // Create a mapping from the field name to the row value
            .toMap // Convert it from a sequence of pairs to a map
        )

    def toCql(ifNotExists : Boolean = true) : String = {
        val ifNotExistsString = if ifNotExists then "IF NOT EXISTS " else "";
        return "CREATE TABLE " + ifNotExistsString + keyspace + "." + name + " (" + fields.map(_.toCql).reduce((l, r) => l + "," + r) + ", PRIMARY KEY " + primaryKeyBuilder + ");"
    }
    
// Fields
trait CassandraField extends TableField:
    val fieldType : String
    def toCql : String = name + " " + fieldType
    def getTableValue(rowData : Row) : TableValue

final case class CassandraIntField(name : String) extends IntField with CassandraField:
    val fieldType = "bigint"
    def getTableValue(rowData : Row) : TableValue = IntValue(name, rowData.getLong(name))

final case class CassandraDoubleField(name : String) extends DoubleField with CassandraField:
    val fieldType = "double"
    def getTableValue(rowData : Row) : TableValue = DoubleValue(name, rowData.getDouble(name))

final case class CassandraStringField(name : String) extends StringField with CassandraField:
    val fieldType = "text"
    def getTableValue(rowData : Row) : TableValue = StringValue(name, rowData.getString(name))

final case class CassandraBoolField(name : String) extends BoolField with CassandraField:
    val fieldType = "boolean"
    def getTableValue(rowData : Row) : TableValue = BoolValue(name, rowData.getBool(name))

final case class CassandraDateTimeField(name : String) extends DateTimeField with CassandraField:
    val fieldType = "timestamp"
    def getTableValue(rowData : Row) : TableValue = DateTimeValue(name, rowData.getInstant(name))

@main def main : Unit = CassandraDataSource.inferTableFromCassandra("test", "test").addTransformation(SelectTransformation(F("a"), F("b")))