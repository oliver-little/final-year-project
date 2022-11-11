package org.oliverlittle.clusterprocess.model.table

import java.time.Instant

trait TableField:
    val name : String
    def getValue(rowNumber : Long) : Any

trait IntField extends TableField:
    def getValue(rowNumber : Long) : Long

trait DoubleField extends TableField:
    def getValue(rowNumber : Long) : Double

trait StringField extends TableField:
    def getValue(rowNumber : Long) : String

trait BoolField extends TableField:
    def getValue(rowNumber : Long) : Boolean

trait DateTimeField extends TableField:
    def getValue(rowNumber : Long) : Instant