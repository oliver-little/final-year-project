package org.oliverlittle.clusterprocess.model.table.field

import java.time.Instant
import scala.reflect.{ClassTag, classTag}

trait TableField:
    val name : String
    val testValue : Any

trait TableValue extends TableField:
    val value : Any

trait IntField extends TableField:
    val testValue : Long = 1

final case class IntValue(name : String, value : Long) extends IntField with TableValue

trait DoubleField extends TableField:
    val testValue : Double = 1

final case class DoubleValue(name : String, value : Double) extends DoubleField with TableValue

trait StringField extends TableField:
    val testValue : String = "a"

final case class StringValue(name : String, value : String) extends StringField with TableValue

trait BoolField extends TableField:
    val testValue : Boolean = true

final case class BoolValue(name : String, value : Boolean) extends BoolField with TableValue

trait DateTimeField extends TableField:
    val testValue : Instant = Instant.EPOCH

final case class DateTimeValue(name : String, value : Instant) extends DateTimeField with TableValue