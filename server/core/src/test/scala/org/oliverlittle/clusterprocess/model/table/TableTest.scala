package org.oliverlittle.clusterprocess.model.table

import org.oliverlittle.clusterprocess.UnitSpec
import org.oliverlittle.clusterprocess.model.table.sources.DataSource
import org.oliverlittle.clusterprocess.model.table.field.{TableField, TableValue, IntField, IntValue}
import org.oliverlittle.clusterprocess.model.field.expressions.{F, V}
import org.oliverlittle.clusterprocess.model.field.expressions.FieldOperations.AddInt

class MockDataSource extends DataSource {
    def getHeaders = Map("a" -> IntField("a"))
    def getData = Seq(Map("a" -> IntValue("a", 1)))
}

class TableSpec extends UnitSpec {
    "A table" should "add a transformation correctly" in {
        val table = Table(MockDataSource())
        table.transformations should have length 0
        val newTable = table.addTransformation(SelectTransformation())
        newTable.transformations should have length 1
    }

    it should "compute a single transformation correctly" in {
        val table = Table(MockDataSource(), Seq(SelectTransformation(AddInt(F("a"), V(1)).as("a"))))
        table.compute should be (Seq(Map("a" -> IntValue("a", 2))))
    }

    // TODO: test multiple transformations sequentially
}