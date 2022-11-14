package org.oliverlittle.clusterprocess.model.table

import java.time.Instant
import scala.reflect.{ClassTag, classTag}

trait TableField:
    val name : String
    val testValue : Any
    def getValue() : Any

trait IntField extends TableField:
    val testValue : Long = 1
    def getValue() : Long

trait DoubleField extends TableField:
    val testValue : Double = 1
    def getValue() : Double

trait StringField extends TableField:
    val testValue : String = "a"
    def getValue() : String

trait BoolField extends TableField:
    val testValue : Boolean = true
    def getValue() : Boolean

trait DateTimeField extends TableField:
    val testValue : Instant = Instant.EPOCH
    def getValue() : Instant