package org.oliverlittle.clusterprocess.model.table

import org.oliverlittle.clusterprocess.UnitSpec
import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.model.table.field._

import org.scalatest.time.Span
import org.scalatest.time.SpanSugar._
import org.scalatest.concurrent.TimeLimits._

class LazyTableResultTest extends UnitSpec {
    "A LazyTableResult" should "concatenate tables with the same header" in {
        val one = LazyTableResult(TableResultHeader(Seq(BaseIntField("one"))), Seq(Seq(Some(IntValue(1L)))))
        val two = LazyTableResult(TableResultHeader(Seq(BaseIntField("one"))), Seq(Seq(Some(IntValue(2L)))))

        val comb = (one ++ two)
        comb.header should be (TableResultHeader(Seq(BaseIntField("one"))))
        comb.rows.toSeq should be (Seq(Seq(Some(IntValue(1L))), Seq(Some(IntValue(2L)))))
    }

    it should "throw an error if the headers are different" in {
        val one = LazyTableResult(TableResultHeader(Seq(BaseStringField("one"))), Seq(Seq(Some(StringValue("a")))))
        val two = LazyTableResult(TableResultHeader(Seq(BaseIntField("one"))), Seq(Seq(Some(IntValue(2L)))))

        assertThrows[IllegalArgumentException] {
            one ++ two
        }
    }

    it should "avoid evaluating the rows" in {
        val seq : Iterable[Seq[Option[TableValue]]]  = Seq(Seq(Some(IntValue(1L))))

        failAfter(50.millis) {
            val seqView = seq.view.map {v => 
                Thread.sleep(100)
                v    
            }
            LazyTableResult(TableResultHeader(Seq(BaseStringField("a"))), seqView)
        }
    }

    it should "convert to protobuf" in {
        val one = LazyTableResult(TableResultHeader(Seq(BaseStringField("one"))), Seq(Seq(Some(StringValue("a")))))
        one.protobuf should be (table_model.TableResult(headers=Some(table_model.TableResultHeader(fields=Seq(table_model.TableResultHeader.Header("one", dataType = table_model.DataType.STRING)))), rows=Seq(table_model.TableResultRow(values=Seq(table_model.Value().withString("a"))))))
    }
}

class EvaluatedTableResultTest extends UnitSpec {
    "An EvaluatedTableResult" should "get a specific cell by index" in {
        val one = EvaluatedTableResult(TableResultHeader(Seq(BaseStringField("one"))), Seq(Seq(Some(StringValue("a"))), Seq(Some(StringValue("b")))))
        one.getCell(0, 0) should be (Some(StringValue("a")))
        one.getCell(1, 0) should be (Some(StringValue("b")))
    }

    it should "get a specific cell by name" in {
        val one = EvaluatedTableResult(TableResultHeader(Seq(BaseStringField("one"))), Seq(Seq(Some(StringValue("a"))), Seq(Some(StringValue("b")))))
        one.getCell(0, "one") should be (Some(StringValue("a")))
        one.getCell(1, "one") should be (Some(StringValue("b")))
    }

    it should "get a row by index" in {
        val one = EvaluatedTableResult(TableResultHeader(Seq(BaseStringField("one"))), Seq(Seq(Some(StringValue("a"))), Seq(Some(StringValue("b")))))
        one.getRow(0) should be (Some(Seq(Some(StringValue("a")))))
        one.getRow(1) should be (Some(Seq(Some(StringValue("b")))))
    }
}

class TableResultHeaderTest extends UnitSpec {
    "A TableResultHeader" should "calculate an index mapping" in {
        val header = TableResultHeader(Seq(BaseIntField("one"), BaseStringField("two")))
        header.headerIndex should be (Map("one" -> 0, "two" -> 1))
    }

    it should "calculate a name -> field mapping" in {
        val header = TableResultHeader(Seq(BaseIntField("one"), BaseStringField("two")))
        header.headerMap should be (Map("one" -> BaseIntField("one"), "two" -> BaseStringField("two")))
    }

    it should "convert to a protobuf" in {
        val header = TableResultHeader(Seq(BaseIntField("one"), BaseStringField("two")))
        header.protobuf should be (table_model.TableResultHeader(fields=Seq(table_model.TableResultHeader.Header("one", dataType = table_model.DataType.INT), table_model.TableResultHeader.Header("two", dataType = table_model.DataType.STRING))))
    }
}