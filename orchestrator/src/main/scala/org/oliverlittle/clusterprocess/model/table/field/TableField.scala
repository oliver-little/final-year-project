package org.oliverlittle.clusterprocess.model.table.field

import java.time.Instant
import scala.reflect.{ClassTag, classTag}

trait TableField:
    val name : String
    // This is not a good solution and needs refactoring
    def compareClassTags[T](tag : ClassTag[T]) : Boolean

trait TableValue extends TableField:
    val value : Any

trait IntField extends TableField:
    def compareClassTags[T](tag : ClassTag[T]) = classTag[Long].equals(tag)

final case class IntValue(name : String, value : Long) extends IntField with TableValue

trait DoubleField extends TableField:
    def compareClassTags[T](tag : ClassTag[T]) = classTag[Double].equals(tag)

final case class DoubleValue(name : String, value : Double) extends DoubleField with TableValue

trait StringField extends TableField:
    def compareClassTags[T](tag : ClassTag[T]) = classTag[String].equals(tag)

final case class StringValue(name : String, value : String) extends StringField with TableValue

trait BoolField extends TableField:
    def compareClassTags[T](tag : ClassTag[T]) = classTag[Boolean].equals(tag)

final case class BoolValue(name : String, value : Boolean) extends BoolField with TableValue

trait DateTimeField extends TableField:
    def compareClassTags[T](tag : ClassTag[T]) = classTag[Instant].equals(tag)

final case class DateTimeValue(name : String, value : Instant) extends DateTimeField with TableValue