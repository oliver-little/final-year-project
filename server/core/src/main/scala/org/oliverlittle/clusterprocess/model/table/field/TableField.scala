package org.oliverlittle.clusterprocess.model.table.field

import java.time.Instant
import scala.reflect.{ClassTag, classTag}

import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.table_model.DataType.STRING
import org.oliverlittle.clusterprocess.table_model.DataType.DATETIME
import org.oliverlittle.clusterprocess.table_model.DataType.BOOL
import org.oliverlittle.clusterprocess.table_model.DataType.DOUBLE
import org.oliverlittle.clusterprocess.table_model.DataType.INT

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
object TableField:
    def fromProtobuf(field : table_model.TableResultHeader.Header) = field.dataType match {
        case STRING => BaseStringField(field.fieldName)
        case DATETIME => BaseDateTimeField(field.fieldName)
        case BOOL => BaseBoolField(field.fieldName)
        case DOUBLE => BaseDoubleField(field.fieldName)
        case INT => BaseIntField(field.fieldName)
        case x => throw new IllegalArgumentException("Unknown enum type" + x.toString)
    }

trait TableField extends ValueType:
    val name : String
    val protobufDataType : table_model.DataType
    lazy val protobuf : table_model.TableResultHeader.Header = table_model.TableResultHeader.Header(fieldName = name, dataType = protobufDataType)

// Interface for a value from a field
object TableValue:
    def fromProtobuf(value : table_model.Value) : Option[TableValue] = value.value.number match {
        case 2 => Some(StringValue(value.value.string.get))
        case 3 => Some(DateTimeValue(Instant.parse(value.value.datetime.get)))
        case 4 => Some(IntValue(value.value.int.get))
        case 5 => Some(DoubleValue(value.value.double.get))
        case 6 => Some(BoolValue(value.value.bool.get))
        case 7 => None
    }

    def valueToTableValue(value : Any) : TableValue = value match {
        case s : String => StringValue(s)
        case i : Int => IntValue(i.toLong)
        case i : Long => IntValue(i)
        case f : Float => DoubleValue(f.toDouble)
        case d : Double => DoubleValue(d)
        case d : Instant => DateTimeValue(d)
        case b : Boolean => BoolValue(b)
        case x => throw new IllegalArgumentException("Cannot convert " + x + " to TableValue.")
    }

    // Extend Option[TableValue] to add protobuf conversions natively
    extension (optionTableValue : Option[TableValue]) {
        def protobuf : table_model.Value = optionTableValue.map(_.protobuf) match {
            case Some(v) => v
            case None => table_model.Value().withNull(true)
        }
    }

    given stringValueDefault : StringValue = StringValue("")
    given intValueDefault : IntValue = IntValue(0)
    given doubleValueDefault : DoubleValue = DoubleValue(0.0)
    given boolValueDefault : BoolValue = BoolValue(true)
    given dateTimeValueDefault : DateTimeValue = DateTimeValue(Instant.EPOCH)
    given fromStringToStringValue : Conversion[String, StringValue] = StringValue(_)
    given fromIntToIntValue : Conversion[Int, IntValue] = (i) => IntValue(i.toLong)
    given fromLongToIntValue : Conversion[Long, IntValue] = IntValue(_)
    given fromFloatToDoubleValue : Conversion[Float, DoubleValue] = (d) => DoubleValue(d.toDouble)
    given fromDoubleToDoubleValue : Conversion[Double, DoubleValue] = DoubleValue(_)
    given fromBoolToBoolValue : Conversion[Boolean, BoolValue] = BoolValue(_)
    given fromInstantToDateTimeValue : Conversion[Instant, DateTimeValue] = DateTimeValue(_)
  
sealed trait TableValue extends ValueType:
    val value : T
    lazy val innerValueProtobuf : table_model.Value.Value
    lazy val protobuf : table_model.Value = table_model.Value(value = innerValueProtobuf)


trait IntType extends ValueType:
    type T = Long
    val valueTag : ClassTag[Long] = classTag[Long]

final case class IntTypeInstance() extends IntType

trait IntField extends TableField with IntType:
    val protobufDataType: table_model.DataType = table_model.DataType.INT

case class BaseIntField(name : String) extends IntField
    
case class IntValue(value : Long) extends TableValue with IntType:
    lazy val innerValueProtobuf : table_model.Value.Value = table_model.Value.Value.Int(value)


trait DoubleType extends ValueType:
    type T = Double
    val valueTag : ClassTag[Double] = classTag[Double]

final case class DoubleTypeInstance() extends DoubleType

trait DoubleField extends TableField with DoubleType:
    val protobufDataType: table_model.DataType = table_model.DataType.DOUBLE

case class BaseDoubleField(name : String) extends DoubleField
    
case class DoubleValue(value : Double) extends TableValue with DoubleType:
    lazy val innerValueProtobuf : table_model.Value.Value = table_model.Value.Value.Double(value)


trait StringType extends ValueType:
    type T = String
    val valueTag : ClassTag[String] = classTag[String]

final case class StringTypeInstance() extends StringType

trait StringField extends TableField with StringType:
    val protobufDataType: table_model.DataType = table_model.DataType.STRING

case class BaseStringField(name : String) extends StringField

case class StringValue(value : String) extends TableValue with StringType:
    lazy val innerValueProtobuf : table_model.Value.Value = table_model.Value.Value.String(value)


trait BoolType extends ValueType:
    type T = Boolean
    val valueTag : ClassTag[Boolean] = classTag[Boolean]

final case class BoolTypeInstance() extends BoolType

trait BoolField extends TableField with BoolType:
    val protobufDataType: table_model.DataType = table_model.DataType.BOOL

case class BaseBoolField(name : String) extends BoolField

case class BoolValue(value : Boolean) extends TableValue with BoolType:
    lazy val innerValueProtobuf : table_model.Value.Value = table_model.Value.Value.Bool(value)


trait DateTimeType extends ValueType:
    type T = Instant
    val valueTag : ClassTag[Instant] = classTag[Instant]

final case class DateTimeTypeInstance() extends DateTimeType

trait DateTimeField extends TableField with DateTimeType:
    val protobufDataType: table_model.DataType = table_model.DataType.DATETIME

final case class BaseDateTimeField(name : String) extends DateTimeField

final case class DateTimeValue(value : Instant) extends TableValue with DateTimeType:
    lazy val innerValueProtobuf : table_model.Value.Value = table_model.Value.Value.Datetime(value.toString())