package org.oliverlittle.clusterprocess.model.table

import org.oliverlittle.clusterprocess.cassandra.CassandraConnector

final case class Table(schema : String, name : String, fields : Seq[TableField], partitionKey : Seq[String], primaryKey : Seq[String] = Seq()):
    val names : Set[String] = fields.map(_.name).toSet
    
    if partitionKey.length == 0 then throw new IllegalArgumentException("Must have at least one partition key")
    if !(partitionKey.forall(names.contains(_)) && primaryKey.forall(names.contains(_))) then throw new IllegalArgumentException("PartitionKey and PrimaryKey names must match field names")

    lazy val primaryKeyBuilder : String = {
        val partition = if partitionKey.length > 1 then "(" + partitionKey.reduce((l, r) => l + "," + r) + ")" else partitionKey(0)
        val primary = if primaryKey.length > 0 then "," + primaryKey.reduce((l, r) => l + "," + r) else ""
        "(" + partition + primary + ")"
    }

    def toCql(ifNotExists : Boolean = true) : String = {
        val ifNotExistsString = if ifNotExists then "IF NOT EXISTS " else "";
        return "CREATE TABLE " + ifNotExistsString + schema + "." + name + " (" + fields.map(_.toCql).reduce((l, r) => l + "," + r) + ", PRIMARY KEY " + primaryKeyBuilder + ");"
    }
    
@main def main : Unit = CassandraConnector.getSession.execute(Table("test", "test_table", Seq(IntField("one"), StringField("two"), DateTimeField("three")), Seq("one", "two"), Seq("three")).toCql())