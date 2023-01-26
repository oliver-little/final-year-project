package org.oliverlittle.clusterprocess.model.table.field

import java.time.Instant
import scala.reflect.{ClassTag, classTag}

object ValueType:
    given intType : IntType = IntTypeInstance()
    given doubleType : DoubleType = DoubleTypeInstance()
    given stringType : StringType = StringTypeInstance()
    given boolType : BoolType = BoolTypeInstance()
    given dateTimeType : DateTimeType = DateTimeTypeInstance()

// Interface for comparing two types
abstract class ValueType:
    type T
    val valueTag : ClassTag[T]
    // Unsure if this is the best solution
    def compareClassTags[U](tag : ClassTag[U]) : Boolean = valueTag.equals(tag)

// Interface for a field definition
sealed trait TableField extends ValueType:
    val name : String

// Interface for a value from a field
object TableValue:
    given fromStringToStringValue : Conversion[String, StringValue] = StringValue(_)
    given fromIntToIntValue : Conversion[Int, IntValue] = (i) => IntValue(i.toLong)
    given fromLongToIntValue : Conversion[Long, IntValue] = IntValue(_)
    given fromFloatToDoubleValue : Conversion[Float, DoubleValue] = (d) => DoubleValue(d.toDouble)
    given fromDoubleToDoubleValue : Conversion[Double, DoubleValue] = DoubleValue(_)
    given fromBoolToBoolValue : Conversion[Boolean, BoolValue] = BoolValue(_)
    given fromInstantToDateTimeValue : Conversion[Instant, DateTimeValue] = DateTimeValue(_)
  
sealed trait TableValue extends ValueType:
    val value : T

trait IntType extends ValueType:
    type T = Long
    val valueTag : ClassTag[Long] = classTag[Long]

final case class IntTypeInstance() extends IntType

trait IntField extends TableField with IntType

case class BaseIntField(name : String) extends IntField
    
case class IntValue(value : Long) extends TableValue with IntType


trait DoubleType extends ValueType:
    type T = Double
    val valueTag : ClassTag[Double] = classTag[Double]

final case class DoubleTypeInstance() extends DoubleType

trait DoubleField extends TableField with DoubleType

case class BaseDoubleField(name : String) extends DoubleField
    
case class DoubleValue(value : Double) extends TableValue with DoubleType


trait StringType extends ValueType:
    type T = String
    val valueTag : ClassTag[String] = classTag[String]

final case class StringTypeInstance() extends StringType

trait StringField extends TableField with StringType

case class BaseStringField(name : String) extends StringField

case class StringValue(value : String) extends TableValue with StringType


trait BoolType extends ValueType:
    type T = Boolean
    val valueTag : ClassTag[Boolean] = classTag[Boolean]

final case class BoolTypeInstance() extends BoolType

trait BoolField extends TableField

case class BaseBoolField(name : String) extends BoolField

case class BoolValue(value : Boolean) extends TableValue with BoolType


trait DateTimeType extends ValueType:
    type T = Instant
    val valueTag : ClassTag[Instant] = classTag[Instant]

final case class DateTimeTypeInstance() extends DateTimeType

trait DateTimeField extends TableField with DateTimeType

final case class BaseDateTimeField(name : String) extends DateTimeField

final case class DateTimeValue(value : Instant) extends TableValue with DateTimeType
