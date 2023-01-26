package org.oliverlittle.clusterprocess.model.table.field

import java.time.Instant
import scala.reflect.{ClassTag, classTag}

// Interface for comparing two types
trait TypeComparator:
    // Unsure if this is the best solution
    def compareClassTags[T](tag : ClassTag[T]) : Boolean

// Interface for a field definition
trait TableField extends TypeComparator:
    val name : String

// Interface for a value from a field
trait TableValue extends TypeComparator:
    val value : Any

trait IntTypeComparator extends TypeComparator:
    def compareClassTags[T](tag : ClassTag[T]) = classTag[Long].equals(tag)

trait IntField extends TableField with IntTypeComparator

case class BaseIntField(name : String) extends IntField
    
case class IntValue(value : Long) extends TableValue with IntTypeComparator

trait DoubleTypeComparator extends TypeComparator:
    def compareClassTags[T](tag : ClassTag[T]) = classTag[Double].equals(tag)

trait DoubleField extends TableField with DoubleTypeComparator

case class BaseDoubleField(name : String) extends DoubleField
    
case class DoubleValue(value : Double) extends TableValue with DoubleTypeComparator

trait StringTypeComparator extends TypeComparator:
    def compareClassTags[T](tag : ClassTag[T]) = classTag[String].equals(tag)

trait StringField extends TableField with StringTypeComparator

case class BaseStringField(name : String) extends StringField

case class StringValue(value : String) extends TableValue with StringTypeComparator

trait BoolTypeComparator extends TypeComparator:
    def compareClassTags[T](tag : ClassTag[T]) = classTag[Boolean].equals(tag)

trait BoolField extends TableField with BoolTypeComparator

case class BaseBoolField(name : String) extends BoolField

case class BoolValue(value : Boolean) extends TableValue with BoolTypeComparator

trait DateTimeComparator extends TypeComparator:
    def compareClassTags[T](tag : ClassTag[T]) = classTag[Instant].equals(tag)

trait DateTimeField extends TableField with DateTimeComparator

final case class BaseDateTimeField(name : String) extends DateTimeField

final case class DateTimeValue(value : Instant) extends TableValue with DateTimeComparator