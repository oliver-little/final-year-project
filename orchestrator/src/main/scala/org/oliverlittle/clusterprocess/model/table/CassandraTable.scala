package org.oliverlittle.clusterprocess.model.table

import org.oliverlittle.clusterprocess.cassandra.CassandraConnector

import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata
import com.datastax.oss.driver.api.core.`type`.{DataTypes, DataType}
import com.datastax.oss.driver.api.core.cql.Row
import scala.jdk.CollectionConverters._
import java.time.Instant

object CassandraTable:
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
        val tableMetadata : TableMetadata = CassandraTable.getTableMetadata(keyspace, table)
        
        // Map column definitions to (name, data type pairs)
        val fieldDefinitions = tableMetadata.getColumns.asScala.map((k, v) => (k.asInternal, v.getType)).toSeq
        val partitions = tableMetadata.getPartitionKey.asScala.map(_.getName.asInternal)
        val primaries = tableMetadata.getPrimaryKey.asScala.map(_.getName.asInternal)

        return CassandraTable(keyspace, table, fieldDefinitions, partitions.toSeq, primaries.toSeq)
    }

/**
  * Creates an instance of table using Cassandra as a backend.
  *
  * @param keyspace The Cassandra keyspace this table is in
  * @param name The name of the table within the keyspace
  * @param fieldDefinitions A list of (column name, column datatype) pairs
  * @param partitionKey A list of strings representing the partition keys for this table
  * @param primaryKey A list of strings representing the primary keys for this table
  */
class CassandraTable(keyspace : String, name : String, fieldDefinitions : Seq[(String, DataType)], partitionKey : Seq[String], primaryKey : Seq[String] = Seq()) extends Table:
    // Validity Checks
    if partitionKey.length == 0 then throw new IllegalArgumentException("Must have at least one partition key")
    if !(partitionKey.forall(fieldMap contains _) && primaryKey.forall(fieldMap contains _)) then throw new IllegalArgumentException("PartitionKey and PrimaryKey names must match field names")

    var currentData : Iterable[Row] = Iterable()

    val fields : Seq[CassandraField] = fieldDefinitions.map((name, dataType) => dataType match {
            case DataTypes.BIGINT => CassandraIntField(name, this)
            case DataTypes.TEXT => CassandraStringField(name, this)
            case DataTypes.DOUBLE => CassandraDoubleField(name, this)
            case DataTypes.BOOLEAN => CassandraBoolField(name, this)
            case DataTypes.TIMESTAMP => CassandraDateTimeField(name, this)
            case v => throw new UnsupportedOperationException("This column type is unsupported: " + v.asCql(false, false))
    })

    lazy val primaryKeyBuilder : String = {
        val partition = if partitionKey.length > 1 then "(" + partitionKey.reduce((l, r) => l + "," + r) + ")" else partitionKey(0)
        val primary = if primaryKey.length > 0 then "," + primaryKey.reduce((l, r) => l + "," + r) else ""
        "(" + partition + primary + ")"
    }

    // This should eventually get the specific records that the worker will execute on, for now it just gets everything
    def getData : Unit = currentData = CassandraConnector.getSession.execute("SELECT * FROM " + keyspace + "." + name).asScala

    // Theoretically how this will work is that the worker will first call getData to get all the rows to run on. Then it will iterate over all rows in currentData
    // The current row is kept in some "context" of sorts. This context will store name -> TableField mappings for F calls, and the actual table being executed as well?
    // Assuming a select statement, it will then iterate over all rows, executing the field expressions which will call the tablefields through F as necessary.
    // These will then reference back to the table to fetch the actual data from the datastax Row instance.
    // Feels quite circular, perhaps there's a better way of doing this. 
    def getRow : Row // INCOMPLETE

    def toCql(ifNotExists : Boolean = true) : String = {
        val ifNotExistsString = if ifNotExists then "IF NOT EXISTS " else "";
        return "CREATE TABLE " + ifNotExistsString + keyspace + "." + name + " (" + fields.map(_.toCql).reduce((l, r) => l + "," + r) + ", PRIMARY KEY " + primaryKeyBuilder + ");"
    }
    
// Fields
trait CassandraField extends TableField:
    val fieldType : String
    def toCql : String = name + " " + fieldType

final case class CassandraIntField(name : String, table : CassandraTable) extends IntField with CassandraField:
    def getValue(rowNumber : Long) : Long = table.getRow.getLong(name)
    val fieldType = "bigint"

final case class CassandraDoubleField(name : String, table : CassandraTable) extends DoubleField with CassandraField:
    def getValue(rowNumber : Long) : Double = table.getRow.getDouble(name)
    val fieldType = "double"

final case class CassandraStringField(name : String, table : CassandraTable) extends StringField with CassandraField:
    def getValue(rowNumber : Long) : String = table.getRow.getString(name)
    val fieldType = "text"

final case class CassandraBoolField(name : String, table : CassandraTable) extends BoolField with CassandraField:
    def getValue(rowNumber : Long) : Boolean = table.getRow.getBoolean(name)
    val fieldType = "boolean"

final case class CassandraDateTimeField(name : String, table : CassandraTable) extends DateTimeField with CassandraField:
    def getValue(rowNumber : Long) : Instant = table.getRow.getInstant(name)
    val fieldType = "timestamp"
