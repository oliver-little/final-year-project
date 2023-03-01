package org.oliverlittle.clusterprocess.model.table

import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.UnitSpec
import org.oliverlittle.clusterprocess.model.table.sources.{MockDataSource, MockPartialDataSource}
import org.oliverlittle.clusterprocess.model.table.field._
import org.oliverlittle.clusterprocess.model.field.expressions.{F, V}
import org.oliverlittle.clusterprocess.model.field.expressions.Max

class TableTest extends UnitSpec {
    "A table" should "add a transformation correctly" in {
        val table = Table(MockDataSource())
        table.transformations should have length 0
        val newTable = table.addTransformation(SelectTransformation())
        newTable.transformations should have length 1
    }

    it should "convert to protobuf correctly" in {
        val ds = MockDataSource()
        val transformations = Seq(SelectTransformation(V(1) as "Col1"))
        val table = Table(ds, transformations)
        table.protobuf should be (table_model.Table(Some(ds.protobuf), transformations.map(_.protobuf)))
    }

    it should "calculate the output headers correctly" in {
        val ds = MockDataSource()
        val table = Table(ds)

        table.outputHeaders should be (ds.getHeaders)

        val newTable = table.addTransformation(SelectTransformation(V(1) as "Col1", V(2) as "Col2"))
        newTable.outputHeaders should be (TableResultHeader(Seq(BaseIntField("Col1"), BaseIntField("Col2"))))
    }

    it should "be valid if the DataSource and all transformations are valid" in {
        val ds = MockDataSource()
        val table = Table(ds, Seq(SelectTransformation(F("a"))))

        table.isValid should be (true)
    }

    it should "be invalid and throw an exception if any dependency is invalid" in {
        val ds = MockDataSource()
        assertThrows[IllegalArgumentException] {
            val table = Table(ds, Seq(SelectTransformation(F("b"))))
            table.isValid
        }
    }

    it should "convert to a PartialTable when provided with a PartialDataSource" in {
        val ds = MockDataSource()
        val partialDS = MockPartialDataSource(ds)
        val table = Table(ds, Seq(SelectTransformation(V(1) as "Col1")))

        table.withPartialDataSource(partialDS) should be (PartialTable(partialDS, Seq(SelectTransformation(V(1) as "Col1"))))
    }

    it should "fail to convert to a PartialTable if the PartialDataSource is the incorrect type" in {
        val ds = MockDataSource()
        val partialDS = MockPartialDataSource()
        val table = Table(ds, Seq(SelectTransformation(V(1) as "Col1")))

        assertThrows[IllegalArgumentException] {
            table.withPartialDataSource(partialDS) should be (PartialTable(partialDS, Seq(SelectTransformation(V(1) as "Col1"))))
        }
    }

    it should "assemble partial results based on the final transformation's assembler" in {
        val header = TableResultHeader(Seq(BaseIntField("Max_a")))
        val rows = Seq(Seq(Some(IntValue(1))), Seq(Some(IntValue(2))))
        val one = EvaluatedTableResult(header, rows)

        val ds = MockDataSource()
        val tableOne = Table(ds, Seq(AggregateTransformation(Max(F("a")))))
        tableOne.assembler.assemblePartial(Seq(one, one)) should be (LazyTableResult(header, Seq(Seq(Some(IntValue(2))))))

        val tableTwo = Table(ds, Seq(SelectTransformation(F("a") as "Max_a"), SelectTransformation(F("Max_a"))))
        tableTwo.assembler.assemblePartial(Seq(one, one)) should be (EvaluatedTableResult(header, rows ++ rows))
    }
}

class PartialTableTest extends UnitSpec {
    "A PartialTable" should "convert to protobuf correctly" in {
        val ds = MockPartialDataSource()
        val transformations = Seq(SelectTransformation(V(1) as "Col1"))
        val table = PartialTable(ds, transformations)
        table.protobuf should be (table_model.PartialTable(Some(ds.protobuf), transformations.map(_.protobuf)))
    }

    it should "compute a partial result correctly" in {
        val ds = MockPartialDataSource()
        val transformations = Seq(SelectTransformation(F("a") as "b"), SelectTransformation(F("b") as "c"))
        val input = EvaluatedTableResult(ds.getHeaders, Seq(Seq(Some(IntValue(1)))))
        val table = PartialTable(ds, transformations).compute(input) should be (LazyTableResult(TableResultHeader(Seq(BaseIntField("c"))), Seq(Seq(Some(IntValue(1))))))
    }

    it should "fail to compute if the provided table result has incorrect headers" in {
        val ds = MockPartialDataSource()
        val transformations = Seq(SelectTransformation(F("a") as "b"), SelectTransformation(F("b") as "c"))
        val input = EvaluatedTableResult(TableResultHeader(Seq(BaseStringField("a"))), Seq(Seq(Some(IntValue(1)))))
        assertThrows[IllegalArgumentException] {
            PartialTable(ds, transformations).compute(input) 
        }
    }
}