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

case class IntField(name : String) extends TableField with IntTypeComparator
    
case class IntValue(value : Long) extends TableValue with IntTypeComparator

trait DoubleTypeComparator extends TypeComparator:
    def compareClassTags[T](tag : ClassTag[T]) = classTag[Double].equals(tag)

case class DoubleField(name : String) extends TableField with DoubleTypeComparator
    
case class DoubleValue(value : Double) extends TableValue with DoubleTypeComparator

trait StringTypeComparator extends TypeComparator:
    def compareClassTags[T](tag : ClassTag[T]) = classTag[String].equals(tag)

case class StringField(name : String) extends TableField with StringTypeComparator

case class StringValue(value : String) extends TableValue with StringTypeComparator

trait BoolTypeComparator extends TypeComparator:
    def compareClassTags[T](tag : ClassTag[T]) = classTag[Boolean].equals(tag)

case class BoolField(name : String) extends TableField with BoolTypeComparator

case class BoolValue(value : Boolean) extends TableValue with BoolTypeComparator

trait DateTimeComparator extends TypeComparator:
    def compareClassTags[T](tag : ClassTag[T]) = classTag[Instant].equals(tag)

final case class DateTimeField(name : String) extends TableField with DateTimeComparator

final case class DateTimeValue(value : Instant) extends TableValue with DateTimeComparator