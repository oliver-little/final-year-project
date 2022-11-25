package org.oliverlittle.clusterprocess.model.table.field

import java.time.Instant
import scala.reflect.{ClassTag, classTag}

trait TableField:
    val name : String
    // This is not a good solution and needs refactoring
    def compareClassTags[T](tag : ClassTag[T]) : Boolean

trait TableValue extends TableField:
    val value : Any

trait BaseIntField extends TableField:
    def compareClassTags[T](tag : ClassTag[T]) = classTag[Long].equals(tag)

final case class IntField(name : String) extends BaseIntField

final case class IntValue(name : String, value : Long) extends BaseIntField with TableValue

trait BaseDoubleField extends TableField:
    def compareClassTags[T](tag : ClassTag[T]) = classTag[Double].equals(tag)

final case class DoubleField(name : String) extends BaseDoubleField

final case class DoubleValue(name : String, value : Double) extends BaseDoubleField with TableValue

trait BaseStringField extends TableField:
    def compareClassTags[T](tag : ClassTag[T]) = classTag[String].equals(tag)

final case class StringField(name : String) extends BaseStringField

final case class StringValue(name : String, value : String) extends BaseStringField with TableValue

trait BaseBoolField extends TableField:
    def compareClassTags[T](tag : ClassTag[T]) = classTag[Boolean].equals(tag)

final case class BoolField(name : String) extends BaseBoolField

final case class BoolValue(name : String, value : Boolean) extends BaseBoolField with TableValue

trait BaseDateTimeField extends TableField:
    def compareClassTags[T](tag : ClassTag[T]) = classTag[Instant].equals(tag)

final case class DateTimeField(name : String) extends BaseDateTimeField

final case class DateTimeValue(name : String, value : Instant) extends BaseDateTimeField with TableValue