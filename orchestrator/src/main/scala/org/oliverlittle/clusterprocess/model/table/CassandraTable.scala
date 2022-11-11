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
        val fields = tableMetadata.getColumns.asScala.map(
            (k, v) => v.getType match {
                case DataTypes.BIGINT => IntField(v.getName.asInternal)
                case DataTypes.TEXT => StringField(v.getName.asInternal)
                case DataTypes.DOUBLE => DoubleField(v.getName.asInternal)
                case DataTypes.BOOLEAN => BoolField(v.getName.asInternal)
                case DataTypes.TIMESTAMP => DateTimeField(v.getName.asInternal)
                case v => throw new UnsupportedOperationException("This column type is unsupported: " + v.asCql(false, false))
            }
        )

        val partitions = tableMetadata.getPartitionKey.asScala.map(_.getName.asInternal)
        val primaries = tableMetadata.getPrimaryKey.asScala.map(_.getName.asInternal)

        return CassandraTable(keyspace, table, fields.toSeq, partitions.toSeq, primaries.toSeq)
    }

final case class CassandraTable(keyspace : String, name : String, fields : Seq[TableField], partitionKey : Seq[String], primaryKey : Seq[String] = Seq()) extends Table:
    // Validity Checks
    if partitionKey.length == 0 then throw new IllegalArgumentException("Must have at least one partition key")
    if !(partitionKey.forall(fieldMap contains _) && primaryKey.forall(fieldMap contains _)) then throw new IllegalArgumentException("PartitionKey and PrimaryKey names must match field names")

    lazy val primaryKeyBuilder : String = {
        val partition = if partitionKey.length > 1 then "(" + partitionKey.reduce((l, r) => l + "," + r) + ")" else partitionKey(0)
        val primary = if primaryKey.length > 0 then "," + primaryKey.reduce((l, r) => l + "," + r) else ""
        "(" + partition + primary + ")"
    }

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
