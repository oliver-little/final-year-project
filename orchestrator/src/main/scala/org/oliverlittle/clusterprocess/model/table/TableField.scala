package org.oliverlittle.clusterprocess.model.table

trait TableField:
    val name : String
    val fieldType : String
    // Returns the Cassandra CREATE TABLE representation of this field
    def toCql : String = name + " " + fieldType

final case class IntField(name : String) extends TableField:
    val fieldType = "bigint"

final case class DoubleField(name : String) extends TableField:
    val fieldType = "double"

final case class StringField(name : String) extends TableField:
    val fieldType = "text"

final case class BoolField(name : String) extends TableField:
    val fieldType = "boolean"

final case class DateTimeField(name : String) extends TableField:
    val fieldType = "timestamp"
