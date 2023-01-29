package org.oliverlittle.clusterprocess.model.table

import org.oliverlittle.clusterprocess.UnitSpec
import org.oliverlittle.clusterprocess.model.table.sources.DataSource
import org.oliverlittle.clusterprocess.model.table.field._
import org.oliverlittle.clusterprocess.model.field.expressions.{F, V}
import org.oliverlittle.clusterprocess.model.field.expressions.FieldOperations.AddInt
import org.oliverlittle.clusterprocess.data_source

class MockDataSource extends DataSource {
    def getHeaders = TableResultHeader(Seq(BaseIntField("a")))
    def getData = EvaluatedTableResult(getHeaders, Seq(Seq(Some(IntValue(2)))))
    def protobuf = data_source.DataSource().withCassandra(data_source.CassandraDataSource(keyspace="test", table="test"))
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
        table.compute should be (LazyTableResult(TableResultHeader(Seq(BaseIntField("a"))), Seq(Seq(Some(IntValue(3))))))
    }

    it should "compute sequential transformations correctly" in {
        val table = Table(MockDataSource(), Seq(SelectTransformation(AddInt(F("a"), V(1)).as("a")), SelectTransformation(AddInt(F("a"), V(-3)).as("b"))))
        table.compute should be (LazyTableResult(TableResultHeader(Seq(BaseIntField("b"))), Seq(Seq(Some(IntValue(0))))))
    }
}