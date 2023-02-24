package org.oliverlittle.clusterprocess.model.table

import org.oliverlittle.clusterprocess.UnitSpec
import org.oliverlittle.clusterprocess.model.table.sources.{MockDataSource, MockPartialDataSource}
import org.oliverlittle.clusterprocess.model.table.field._
import org.oliverlittle.clusterprocess.model.field.expressions.{F, V}
import org.oliverlittle.clusterprocess.model.field.expressions.FieldOperations.AddInt
import org.oliverlittle.clusterprocess.worker_query

class TableSpec extends UnitSpec {
    "A table" should "add a transformation correctly" in {
        val table = Table(MockDataSource())
        table.transformations should have length 0
        val newTable = table.addTransformation(SelectTransformation())
        newTable.transformations should have length 1
    }

    it should "convert to protobuf correctly" in {
        fail()
    }

    it should "calculate the output headers correctly" in {
        fail()
    }

    it should "be valid if the DataSource and all transformations are valid" in {
        fail()
    }

    it should "be invalid if any dependency is invalid" in {
        fail()
    }

    it should "convert to a PartialTable when provided with a PartialDataSource" in {
        fail()
    }

    it should "fail to convert to a PartialTable if the PartialDataSource is the incorrect type" in {
        fail()
    }

    it should "assemble partial results based on the final transformation's assembler" in {
        fail()
    }
}

class PartialTableSpec extends UnitSpec {
    "A PartialTable" should "convert from protobuf correctly" in {
        fail()
    }

    it should "convert to protobuf correctly" in {
        fail()
    }

    it should "compute a partial result correctly" in {
        fail()
    }

    it should "fail to compute if the provided table result has incorrect headers" in {
        fail()
    }
}