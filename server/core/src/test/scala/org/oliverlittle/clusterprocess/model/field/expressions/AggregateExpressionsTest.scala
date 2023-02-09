package org.oliverlittle.clusterprocess.model.field.expressions

import org.oliverlittle.clusterprocess.UnitSpec

import org.oliverlittle.clusterprocess.model.table._
import org.oliverlittle.clusterprocess.model.table.field._
import org.oliverlittle.clusterprocess.table_model

import java.time.Instant

class AggregateExpressionsTest extends UnitSpec {
    "An AggregateExpression" should "convert from protobuf correctly" in {
        val inputResults = Map(
            table_model.AggregateExpression.AggregateType.MAX -> Min(F("name")),
            table_model.AggregateExpression.AggregateType.MAX -> Max(F("name")),
            table_model.AggregateExpression.AggregateType.SUM -> Sum(F("name")),
            table_model.AggregateExpression.AggregateType.AVG -> Avg(F("name")),
            table_model.AggregateExpression.AggregateType.COUNT -> Count(F("name")),
            table_model.AggregateExpression.AggregateType.COUNT_DISTINCT -> DistinctCount(F("name")),
            table_model.AggregateExpression.AggregateType.STRING_CONCAT -> StringConcat(F("name"), ","),
            table_model.AggregateExpression.AggregateType.STRING_CONCAT_DISTINCT -> DistinctStringConcat(F("name"), ",")
        )

        val namedExpression = table_model.NamedExpression("name", Some(table_model.Expression().withValue(table_model.Value().withField("name"))))
        inputResults.foreach((i, o) => AggregateExpression.fromProtobuf(table_model.AggregateExpression(i, Some(namedExpression), Some(","))) should be (o))
    }
}

class MaxTest extends UnitSpec {
    "A MaxExpression" should "convert to protobuf correctly" in {
        val namedExpression = table_model.NamedExpression("name", Some(table_model.Expression().withValue(table_model.Value().withField("name"))))
        Max(F("name")).protobuf should be (table_model.AggregateExpression(table_model.AggregateExpression.AggregateType.MAX, Some(namedExpression)))
    }

    it should "output a correct partial table field based on the input type" in {
        val inputResults = Map(
            Seq(BaseStringField("name")) -> Seq(BaseStringField("Max_name")),
            Seq(BaseIntField("name")) -> Seq(BaseIntField("Max_name")),
            Seq(BaseDoubleField("name")) -> Seq(BaseDoubleField("Max_name")),
            Seq(BaseDateTimeField("name")) -> Seq(BaseDateTimeField("Max_name")),
        )

        inputResults.foreach((i, o) => Max(F("name")).outputPartialTableFields(TableResultHeader(i)) should be (o))
    }

    it should "output a correct final table field based on the input type" in {
        val inputResults = Map(
            Seq(BaseStringField("Max_name")) -> Seq(BaseStringField("Max_name")),
            Seq(BaseIntField("Max_name")) -> Seq(BaseIntField("Max_name")),
            Seq(BaseDoubleField("Max_name")) -> Seq(BaseDoubleField("Max_name")),
            Seq(BaseDateTimeField("Max_name")) -> Seq(BaseDateTimeField("Max_name")),
        )

        inputResults.foreach((i, o) => Max(F("name")).outputTableFields(TableResultHeader(i)) should be (o))
    }

    it should "produce a partial result" in {
        val inputResults : Seq[(TableResultHeader, Seq[Seq[Option[TableValue]]], Seq[Option[TableValue]])] = Seq(
            (TableResultHeader(Seq(BaseStringField("name"))), Seq(Seq(Some(StringValue("a"))), Seq(Some(StringValue("b"))), Seq(Some(StringValue("a")))), Seq(Some(StringValue("b")))),
            (TableResultHeader(Seq(BaseIntField("name"))), Seq(Seq(Some(IntValue(1))), Seq(Some(IntValue(3))), Seq(Some(IntValue(1)))), Seq(Some(IntValue(3)))),
            (TableResultHeader(Seq(BaseDoubleField("name"))), Seq(Seq(Some(DoubleValue(1.0))), Seq(Some(DoubleValue(3.0))), Seq(Some(DoubleValue(1.0)))), Seq(Some(DoubleValue(3.0)))),
            (TableResultHeader(Seq(BaseDateTimeField("name"))), Seq(Seq(Some(DateTimeValue(Instant.ofEpochSecond(1)))), Seq(Some(DateTimeValue(Instant.ofEpochSecond(3)))), Seq(Some(DateTimeValue(Instant.ofEpochSecond(1))))), Seq(Some(DateTimeValue(Instant.ofEpochSecond(3)))))
        )

        inputResults.foreach((header, items, output) => Max(F("name")).resolve(header)(items) should be (output))
    }

    it should "produce None as a partial result if all elements are None" in {
        val inputResults : Seq[(TableResultHeader, Seq[Seq[Option[TableValue]]], Seq[Option[TableValue]])] = Seq(
            (TableResultHeader(Seq(BaseStringField("name"))), Seq(Seq(None), Seq(None), Seq(None)), Seq(None)),
            (TableResultHeader(Seq(BaseIntField("name"))), Seq(Seq(None), Seq(None), Seq(None)), Seq(None)),
            (TableResultHeader(Seq(BaseDoubleField("name"))), Seq(Seq(None), Seq(None), Seq(None)), Seq(None)),
            (TableResultHeader(Seq(BaseDateTimeField("name"))), Seq(Seq(None), Seq(None), Seq(None)), Seq(None))
        )

        inputResults.foreach((header, items, output) => Max(F("name")).resolve(header)(items) should be (output))
    }

    it should "produce a final result" in {
        val inputResults : Seq[(TableResultHeader, Seq[Seq[Option[TableValue]]], Seq[Option[TableValue]])] = Seq(
            (TableResultHeader(Seq(BaseStringField("Max_name"))), Seq(Seq(Some(StringValue("a"))), Seq(Some(StringValue("b"))), Seq(Some(StringValue("a")))), Seq(Some(StringValue("b")))),
            (TableResultHeader(Seq(BaseIntField("Max_name"))), Seq(Seq(Some(IntValue(1))), Seq(Some(IntValue(3))), Seq(Some(IntValue(1)))), Seq(Some(IntValue(3)))),
            (TableResultHeader(Seq(BaseDoubleField("Max_name"))), Seq(Seq(Some(DoubleValue(1.0))), Seq(Some(DoubleValue(3.0))), Seq(Some(DoubleValue(1.0)))), Seq(Some(DoubleValue(3.0)))),
            (TableResultHeader(Seq(BaseDateTimeField("Max_name"))), Seq(Seq(Some(DateTimeValue(Instant.ofEpochSecond(1)))), Seq(Some(DateTimeValue(Instant.ofEpochSecond(3)))), Seq(Some(DateTimeValue(Instant.ofEpochSecond(1))))), Seq(Some(DateTimeValue(Instant.ofEpochSecond(3)))))
        )

        inputResults.foreach((header, items, output) => Max(F("name")).assemble(header)(items) should be (output))
    }

    it should "produce None as a final result if all partial elements are None" in {
        val inputResults : Seq[(TableResultHeader, Seq[Seq[Option[TableValue]]], Seq[Option[TableValue]])] = Seq(
            (TableResultHeader(Seq(BaseStringField("Max_name"))), Seq(Seq(None), Seq(None), Seq(None)), Seq(None)),
            (TableResultHeader(Seq(BaseIntField("Max_name"))), Seq(Seq(None), Seq(None), Seq(None)), Seq(None)),
            (TableResultHeader(Seq(BaseDoubleField("Max_name"))), Seq(Seq(None), Seq(None), Seq(None)), Seq(None)),
            (TableResultHeader(Seq(BaseDateTimeField("Max_name"))), Seq(Seq(None), Seq(None), Seq(None)), Seq(None))
        )

        inputResults.foreach((header, items, output) => Max(F("name")).assemble(header)(items) should be (output))
    }

    it should "throw an IllegalArgumentExpression if called on an invalid type" in {
        assertThrows[IllegalArgumentException] {
            Max(F("name")).outputPartialTableFields(TableResultHeader(Seq(BaseBoolField("name"))))
        }

        assertThrows[IllegalArgumentException] {
            Max(F("name")).resolve(TableResultHeader(Seq(BaseBoolField("name"))))(Seq(Seq()))
        }

         assertThrows[IllegalArgumentException] {
            Max(F("name")).assemble(TableResultHeader(Seq(BaseBoolField("name"))))(Seq(Seq()))
        }
    }
}

class MinTest extends UnitSpec {
    "A MinExpression" should "convert to protobuf correctly" in {
        val namedExpression = table_model.NamedExpression("name", Some(table_model.Expression().withValue(table_model.Value().withField("name"))))
        Min(F("name")).protobuf should be (table_model.AggregateExpression(table_model.AggregateExpression.AggregateType.MIN, Some(namedExpression)))
    }

    it should "output a correct partial table field based on the input type" in {
        val inputResults = Map(
            Seq(BaseStringField("name")) -> Seq(BaseStringField("Min_name")),
            Seq(BaseIntField("name")) -> Seq(BaseIntField("Min_name")),
            Seq(BaseDoubleField("name")) -> Seq(BaseDoubleField("Min_name")),
            Seq(BaseDateTimeField("name")) -> Seq(BaseDateTimeField("Min_name")),
        )

        inputResults.foreach((i, o) => Min(F("name")).outputPartialTableFields(TableResultHeader(i)) should be (o))
    }

    it should "output a correct final table field based on the input type" in {
        val inputResults = Map(
            Seq(BaseStringField("Min_name")) -> Seq(BaseStringField("Min_name")),
            Seq(BaseIntField("Min_name")) -> Seq(BaseIntField("Min_name")),
            Seq(BaseDoubleField("Min_name")) -> Seq(BaseDoubleField("Min_name")),
            Seq(BaseDateTimeField("Min_name")) -> Seq(BaseDateTimeField("Min_name")),
        )

        inputResults.foreach((i, o) => Min(F("name")).outputTableFields(TableResultHeader(i)) should be (o))
    }

    it should "produce a partial result" in {
        val inputResults : Seq[(TableResultHeader, Seq[Seq[Option[TableValue]]], Seq[Option[TableValue]])] = Seq(
            (TableResultHeader(Seq(BaseStringField("name"))), Seq(Seq(Some(StringValue("a"))), Seq(Some(StringValue("b"))), Seq(Some(StringValue("a")))), Seq(Some(StringValue("a")))),
            (TableResultHeader(Seq(BaseIntField("name"))), Seq(Seq(Some(IntValue(1))), Seq(Some(IntValue(3))), Seq(Some(IntValue(1)))), Seq(Some(IntValue(1)))),
            (TableResultHeader(Seq(BaseDoubleField("name"))), Seq(Seq(Some(DoubleValue(1.0))), Seq(Some(DoubleValue(3.0))), Seq(Some(DoubleValue(1.0)))), Seq(Some(DoubleValue(1.0)))),
            (TableResultHeader(Seq(BaseDateTimeField("name"))), Seq(Seq(Some(DateTimeValue(Instant.ofEpochSecond(1)))), Seq(Some(DateTimeValue(Instant.ofEpochSecond(3)))), Seq(Some(DateTimeValue(Instant.ofEpochSecond(1))))), Seq(Some(DateTimeValue(Instant.ofEpochSecond(1)))))
        )

        inputResults.foreach((header, items, output) => Min(F("name")).resolve(header)(items) should be (output))
    }

    it should "produce None as a partial result if all elements are None" in {
        val inputResults : Seq[(TableResultHeader, Seq[Seq[Option[TableValue]]], Seq[Option[TableValue]])] = Seq(
            (TableResultHeader(Seq(BaseStringField("name"))), Seq(Seq(None), Seq(None), Seq(None)), Seq(None)),
            (TableResultHeader(Seq(BaseIntField("name"))), Seq(Seq(None), Seq(None), Seq(None)), Seq(None)),
            (TableResultHeader(Seq(BaseDoubleField("name"))), Seq(Seq(None), Seq(None), Seq(None)), Seq(None)),
            (TableResultHeader(Seq(BaseDateTimeField("name"))), Seq(Seq(None), Seq(None), Seq(None)), Seq(None))
        )

        inputResults.foreach((header, items, output) => Min(F("name")).resolve(header)(items) should be (output))
    }

    it should "produce a final result" in {
        val inputResults : Seq[(TableResultHeader, Seq[Seq[Option[TableValue]]], Seq[Option[TableValue]])] = Seq(
            (TableResultHeader(Seq(BaseStringField("Min_name"))), Seq(Seq(Some(StringValue("a"))), Seq(Some(StringValue("b"))), Seq(Some(StringValue("a")))), Seq(Some(StringValue("a")))),
            (TableResultHeader(Seq(BaseIntField("Min_name"))), Seq(Seq(Some(IntValue(1))), Seq(Some(IntValue(3))), Seq(Some(IntValue(1)))), Seq(Some(IntValue(1)))),
            (TableResultHeader(Seq(BaseDoubleField("Min_name"))), Seq(Seq(Some(DoubleValue(1.0))), Seq(Some(DoubleValue(3.0))), Seq(Some(DoubleValue(1.0)))), Seq(Some(DoubleValue(1.0)))),
            (TableResultHeader(Seq(BaseDateTimeField("Min_name"))), Seq(Seq(Some(DateTimeValue(Instant.ofEpochSecond(1)))), Seq(Some(DateTimeValue(Instant.ofEpochSecond(3)))), Seq(Some(DateTimeValue(Instant.ofEpochSecond(1))))), Seq(Some(DateTimeValue(Instant.ofEpochSecond(1)))))
        )

        inputResults.foreach((header, items, output) => Min(F("name")).assemble(header)(items) should be (output))
    }

    it should "produce None as a final result if all partial elements are None" in {
        val inputResults : Seq[(TableResultHeader, Seq[Seq[Option[TableValue]]], Seq[Option[TableValue]])] = Seq(
            (TableResultHeader(Seq(BaseStringField("Min_name"))), Seq(Seq(None), Seq(None), Seq(None)), Seq(None)),
            (TableResultHeader(Seq(BaseIntField("Min_name"))), Seq(Seq(None), Seq(None), Seq(None)), Seq(None)),
            (TableResultHeader(Seq(BaseDoubleField("Min_name"))), Seq(Seq(None), Seq(None), Seq(None)), Seq(None)),
            (TableResultHeader(Seq(BaseDateTimeField("Min_name"))), Seq(Seq(None), Seq(None), Seq(None)), Seq(None))
        )

        inputResults.foreach((header, items, output) => Min(F("name")).assemble(header)(items) should be (output))
    }

    it should "throw an IllegalArgumentExpression if called on an invalid type" in {
        assertThrows[IllegalArgumentException] {
            Min(F("name")).outputPartialTableFields(TableResultHeader(Seq(BaseBoolField("name"))))
        }

        assertThrows[IllegalArgumentException] {
            Min(F("name")).resolve(TableResultHeader(Seq(BaseBoolField("name"))))(Seq(Seq()))
        }

         assertThrows[IllegalArgumentException] {
            Min(F("name")).assemble(TableResultHeader(Seq(BaseBoolField("name"))))(Seq(Seq()))
        }
    }
}

class SumTest extends UnitSpec {
    "A SumExpression" should "convert to protobuf correctly" in {
        val namedExpression = table_model.NamedExpression("name", Some(table_model.Expression().withValue(table_model.Value().withField("name"))))
        Sum(F("name")).protobuf should be (table_model.AggregateExpression(table_model.AggregateExpression.AggregateType.SUM, Some(namedExpression)))
    }

    it should "output a correct partial table field based on the input type" in {
        val inputResults = Map(
            Seq(BaseIntField("name")) -> Seq(BaseIntField("Sum_name")),
            Seq(BaseDoubleField("name")) -> Seq(BaseDoubleField("Sum_name"))
        )

        inputResults.foreach((i, o) => Sum(F("name")).outputPartialTableFields(TableResultHeader(i)) should be (o))
    }

    it should "output a correct final table field based on the input type" in {
        val inputResults = Map(
            Seq(BaseIntField("Sum_name")) -> Seq(BaseIntField("Sum_name")),
            Seq(BaseDoubleField("Sum_name")) -> Seq(BaseDoubleField("Sum_name"))
        )

        inputResults.foreach((i, o) => Sum(F("name")).outputTableFields(TableResultHeader(i)) should be (o))
    }

    it should "produce a partial result" in {
        val inputResults : Seq[(TableResultHeader, Seq[Seq[Option[TableValue]]], Seq[Option[TableValue]])] = Seq(
            (TableResultHeader(Seq(BaseIntField("name"))), Seq(Seq(Some(IntValue(1))), Seq(Some(IntValue(3))), Seq(Some(IntValue(1)))), Seq(Some(IntValue(5)))),
            (TableResultHeader(Seq(BaseDoubleField("name"))), Seq(Seq(Some(DoubleValue(1.0))), Seq(Some(DoubleValue(3.0))), Seq(Some(DoubleValue(1.0)))), Seq(Some(DoubleValue(5.0))))
        )

        inputResults.foreach((header, items, output) => Sum(F("name")).resolve(header)(items) should be (output))
    }

    it should "produce 0 as a partial result if all elements are None" in {
        val inputResults : Seq[(TableResultHeader, Seq[Seq[Option[TableValue]]], Seq[Option[TableValue]])] = Seq(
            (TableResultHeader(Seq(BaseIntField("name"))), Seq(Seq(None), Seq(None), Seq(None)), Seq(Some(IntValue(0)))),
            (TableResultHeader(Seq(BaseDoubleField("name"))), Seq(Seq(None), Seq(None), Seq(None)), Seq(Some(DoubleValue(0))))
        )

        inputResults.foreach((header, items, output) => Sum(F("name")).resolve(header)(items) should be (output))
    }

    it should "produce a final result" in {
        val inputResults : Seq[(TableResultHeader, Seq[Seq[Option[TableValue]]], Seq[Option[TableValue]])] = Seq(
            (TableResultHeader(Seq(BaseIntField("Sum_name"))), Seq(Seq(Some(IntValue(1))), Seq(Some(IntValue(3))), Seq(Some(IntValue(1)))), Seq(Some(IntValue(5)))),
            (TableResultHeader(Seq(BaseDoubleField("Sum_name"))), Seq(Seq(Some(DoubleValue(1.0))), Seq(Some(DoubleValue(3.0))), Seq(Some(DoubleValue(1.0)))), Seq(Some(DoubleValue(5.0))))
        )

        inputResults.foreach((header, items, output) => Sum(F("name")).assemble(header)(items) should be (output))
    }

    it should "produce None as a final result if all partial elements are None" in {
        val inputResults : Seq[(TableResultHeader, Seq[Seq[Option[TableValue]]], Seq[Option[TableValue]])] = Seq(
            (TableResultHeader(Seq(BaseIntField("Sum_name"))), Seq(Seq(None), Seq(None), Seq(None)), Seq(Some(IntValue(0)))),
            (TableResultHeader(Seq(BaseDoubleField("Sum_name"))), Seq(Seq(None), Seq(None), Seq(None)), Seq(Some(DoubleValue(0))))
        )

        inputResults.foreach((header, items, output) => Sum(F("name")).assemble(header)(items) should be (output))
    }

    it should "throw an IllegalArgumentExpression if called on an invalid type" in {
        assertThrows[IllegalArgumentException] {
            Sum(F("name")).outputPartialTableFields(TableResultHeader(Seq(BaseBoolField("name"))))
        }

        assertThrows[IllegalArgumentException] {
            Sum(F("name")).resolve(TableResultHeader(Seq(BaseBoolField("name"))))(Seq(Seq()))
        }

         assertThrows[IllegalArgumentException] {
            Sum(F("name")).assemble(TableResultHeader(Seq(BaseBoolField("name"))))(Seq(Seq()))
        }
    }
}

class AvgTest extends UnitSpec {
    "An AvgExpression" should "convert to protobuf correctly" in {
        val namedExpression = table_model.NamedExpression("name", Some(table_model.Expression().withValue(table_model.Value().withField("name"))))
        Avg(F("name")).protobuf should be (table_model.AggregateExpression(table_model.AggregateExpression.AggregateType.AVG, Some(namedExpression)))
    }

    it should "output a correct partial table field based on the input type" in {
        val inputResults = Map(
            Seq(BaseIntField("name")) -> Seq(BaseIntField("AvgSum_name"), BaseIntField("AvgCount_name")),
            Seq(BaseDoubleField("name")) -> Seq(BaseDoubleField("AvgSum_name"), BaseIntField("AvgCount_name")),
            Seq(BaseDateTimeField("name")) -> Seq(BaseDateTimeField("AvgSum_name"), BaseIntField("AvgCount_name")),
        )

        inputResults.foreach((i, o) => Avg(F("name")).outputPartialTableFields(TableResultHeader(i)) should be (o))
    }

    it should "output a correct final table field based on the input type" in {
        val inputResults = Map(
            Seq(BaseIntField("AvgSum_name"), BaseIntField("AvgCount_name")) -> Seq(BaseIntField("Avg_name")),
            Seq(BaseDoubleField("AvgSum_name"), BaseDoubleField("AvgCount_name")) -> Seq(BaseDoubleField("Avg_name")),
            Seq(BaseDateTimeField("AvgSum_name"), BaseIntField("AvgCount_name")) -> Seq(BaseDateTimeField("Avg_name")),
        )

        inputResults.foreach((i, o) => Avg(F("name")).outputTableFields(TableResultHeader(i)) should be (o))
    }

    it should "produce a partial result" in {
        val inputResults : Seq[(TableResultHeader, Seq[Seq[Option[TableValue]]], Seq[Option[TableValue]])] = Seq(
            (TableResultHeader(Seq(BaseIntField("name"))), Seq(Seq(Some(IntValue(1))), Seq(Some(IntValue(3))), Seq(Some(IntValue(2)))), Seq(Some(IntValue(6)), Some(IntValue(3)))),
            (TableResultHeader(Seq(BaseDoubleField("name"))), Seq(Seq(Some(DoubleValue(1.0))), Seq(Some(DoubleValue(3.0))), Seq(Some(DoubleValue(2.0)))), Seq(Some(DoubleValue(6.0)), Some(IntValue(3)))),
            (TableResultHeader(Seq(BaseDateTimeField("name"))), Seq(Seq(Some(DateTimeValue(Instant.ofEpochSecond(1)))), Seq(Some(DateTimeValue(Instant.ofEpochSecond(3))))), Seq(Some(StringValue("4000")), Some(IntValue(2))))
        )

        inputResults.foreach((header, items, output) => Avg(F("name")).resolve(header)(items) should be (output))
    }

    it should "produce a correct partial result if all elements are None" in {
        val inputResults : Seq[(TableResultHeader, Seq[Seq[Option[TableValue]]], Seq[Option[TableValue]])] = Seq(
            (TableResultHeader(Seq(BaseIntField("name"))), Seq(Seq(None), Seq(None), Seq(None)), Seq(Some(IntValue(0)), Some(IntValue(0)))),
            (TableResultHeader(Seq(BaseDoubleField("name"))), Seq(Seq(None), Seq(None), Seq(None)), Seq(Some(DoubleValue(0)), Some(IntValue(0)))),
            (TableResultHeader(Seq(BaseDateTimeField("name"))), Seq(Seq(None), Seq(None), Seq(None)), Seq(Some(StringValue("0")), Some(IntValue(0))))
        )

        inputResults.foreach((header, items, output) => Avg(F("name")).resolve(header)(items) should be (output))
    }

    it should "produce a final result" in {
        val inputResults : Seq[(TableResultHeader, Seq[Seq[Option[TableValue]]], Seq[Option[TableValue]])] = Seq(
            (TableResultHeader(Seq(BaseIntField("AvgSum_name"), BaseIntField("AvgCount_name"))), Seq(Seq(Some(IntValue(6)), Some(IntValue(3))), Seq(Some(IntValue(6)), Some(IntValue(3)))), Seq(Some(DoubleValue(2)))),
            (TableResultHeader(Seq(BaseDoubleField("AvgSum_name"), BaseIntField("AvgCount_name"))), Seq(Seq(Some(DoubleValue(6.0)), Some(IntValue(3))), Seq(Some(DoubleValue(6.0)), Some(IntValue(3)))), Seq(Some(DoubleValue(2.0)))),
            (TableResultHeader(Seq(BaseDateTimeField("AvgSum_name"), BaseIntField("AvgCount_name"))), Seq(Seq(Some(StringValue("4000")), Some(IntValue(2))), Seq(Some(StringValue("4000")), Some(IntValue(2)))), Seq(Some(DateTimeValue(Instant.ofEpochSecond(2)))))
        )

        inputResults.foreach((header, items, output) => Avg(F("name")).assemble(header)(items) should be (output))
    }

    it should "throw an IllegalArgumentExpression if called on an invalid type" in {
        assertThrows[IllegalArgumentException] {
            Avg(F("name")).outputPartialTableFields(TableResultHeader(Seq(BaseBoolField("name"))))
        }

        assertThrows[IllegalArgumentException] {
            Avg(F("name")).resolve(TableResultHeader(Seq(BaseBoolField("name"))))(Seq(Seq()))
        }
    }
}

class CountTest extends UnitSpec {
    "A CountExpression" should "convert to protobuf correctly" in {
        val namedExpression = table_model.NamedExpression("name", Some(table_model.Expression().withValue(table_model.Value().withField("name"))))
        Count(F("name")).protobuf should be (table_model.AggregateExpression(table_model.AggregateExpression.AggregateType.COUNT, Some(namedExpression)))
    }

    it should "output a correct partial table field based on the input type" in {
        val inputResults = Map(
            Seq(BaseStringField("name")) -> Seq(BaseIntField("Count_name")),
            Seq(BaseIntField("name")) -> Seq(BaseIntField("Count_name")),
            Seq(BaseDoubleField("name")) -> Seq(BaseIntField("Count_name")),
            Seq(BaseDateTimeField("name")) -> Seq(BaseIntField("Count_name")),
            Seq(BaseBoolField("name")) -> Seq(BaseIntField("Count_name")),
        )

        inputResults.foreach((i, o) => Count(F("name")).outputPartialTableFields(TableResultHeader(i)) should be (o))
    }

    it should "output a correct final table field based on the input type" in {
        val inputResults = Map(
            Seq(BaseIntField("Count_name")) -> Seq(BaseIntField("Count_name"))
        )

        inputResults.foreach((i, o) => Count(F("name")).outputTableFields(TableResultHeader(i)) should be (o))
    }

    it should "produce a partial result" in {
        val inputResults : Seq[(TableResultHeader, Seq[Seq[Option[TableValue]]], Seq[Option[TableValue]])] = Seq(
            (TableResultHeader(Seq(BaseStringField("name"))), Seq(Seq(Some(StringValue("a"))), Seq(Some(StringValue("b"))), Seq(Some(StringValue("a")))), Seq(Some(IntValue(3))))
        )

        inputResults.foreach((header, items, output) => Count(F("name")).resolve(header)(items) should be (output))
    }

    it should "exclude None from counts" in {
        val inputResults : Seq[(TableResultHeader, Seq[Seq[Option[TableValue]]], Seq[Option[TableValue]])] = Seq(
            (TableResultHeader(Seq(BaseStringField("name"))), Seq(Seq(None), Seq(None), Seq(None)), Seq(Some(IntValue(0)))),

        )

        inputResults.foreach((header, items, output) => Count(F("name")).resolve(header)(items) should be (output))
    }

    it should "produce a final result" in {
        val inputResults : Seq[(TableResultHeader, Seq[Seq[Option[TableValue]]], Seq[Option[TableValue]])] = Seq(
            (TableResultHeader(Seq(BaseIntField("Count_name"))), Seq(Seq(Some(IntValue(3))), Seq(Some(IntValue(3)))), Seq(Some(IntValue(6)))),
        )

        inputResults.foreach((header, items, output) => Count(F("name")).assemble(header)(items) should be (output))
    }
}

class DistinctCountTest extends UnitSpec {
    "A CountExpression" should "convert to protobuf correctly" in {
        val namedExpression = table_model.NamedExpression("name", Some(table_model.Expression().withValue(table_model.Value().withField("name"))))
        DistinctCount(F("name")).protobuf should be (table_model.AggregateExpression(table_model.AggregateExpression.AggregateType.COUNT_DISTINCT, Some(namedExpression)))
    }

    it should "output a correct partial table field based on the input type" in {
        val inputResults = Map(
            Seq(BaseStringField("name")) -> Seq(BaseStringField("CountDistinct_name")),
            Seq(BaseIntField("name")) -> Seq(BaseStringField("CountDistinct_name")),
            Seq(BaseDoubleField("name")) -> Seq(BaseStringField("CountDistinct_name")),
            Seq(BaseDateTimeField("name")) -> Seq(BaseStringField("CountDistinct_name")),
            Seq(BaseBoolField("name")) -> Seq(BaseStringField("CountDistinct_name")),
        )

        inputResults.foreach((i, o) => DistinctCount(F("name")).outputPartialTableFields(TableResultHeader(i)) should be (o))
    }

    it should "output a correct final table field based on the input type" in {
        val inputResults = Map(
            Seq(BaseStringField("CountDistinct_name")) -> Seq(BaseIntField("CountDistinct_name"))
        )

        inputResults.foreach((i, o) => DistinctCount(F("name")).outputTableFields(TableResultHeader(i)) should be (o))
    }

    it should "produce a partial result" in {
        val inputResults : Seq[(TableResultHeader, Seq[Seq[Option[TableValue]]], Seq[Option[TableValue]])] = Seq(
            (TableResultHeader(Seq(BaseStringField("name"))), Seq(Seq(Some(StringValue("a"))), Seq(Some(StringValue("b"))), Seq(Some(StringValue("a")))), Seq(Some(StringValue("a>^<b"))))
        )

        inputResults.foreach((header, items, output) => DistinctCount(F("name")).resolve(header)(items) should be (output))
    }

    it should "exclude None from concatenations" in {
        val inputResults : Seq[(TableResultHeader, Seq[Seq[Option[TableValue]]], Seq[Option[TableValue]])] = Seq(
            (TableResultHeader(Seq(BaseStringField("name"))), Seq(Seq(None), Seq(None), Seq(None)), Seq(Some(StringValue("")))),

        )

        inputResults.foreach((header, items, output) => DistinctCount(F("name")).resolve(header)(items) should be (output))
    }

    it should "produce a final result" in {
        val inputResults : Seq[(TableResultHeader, Seq[Seq[Option[TableValue]]], Seq[Option[TableValue]])] = Seq(
            (TableResultHeader(Seq(BaseIntField("CountDistinct_name"))), Seq(Seq(Some(StringValue("a>^<b"))), Seq(Some(StringValue("c>^<d>^<e")))), Seq(Some(IntValue(5)))),
        )

        inputResults.foreach((header, items, output) => DistinctCount(F("name")).assemble(header)(items) should be (output))
    }
}

class StringConcatTest extends UnitSpec {
    "A CountExpression" should "convert to protobuf correctly" in {
        val namedExpression = table_model.NamedExpression("name", Some(table_model.Expression().withValue(table_model.Value().withField("name"))))
        StringConcat(F("name"), ",").protobuf should be (table_model.AggregateExpression(table_model.AggregateExpression.AggregateType.STRING_CONCAT, Some(namedExpression), Some(",")))
    }

    it should "output a correct partial table field based on the input type" in {
        val inputResults = Map(
            Seq(BaseStringField("name")) -> Seq(BaseStringField("StringConcat_name"))
        )

        inputResults.foreach((i, o) => StringConcat(F("name"), ",").outputPartialTableFields(TableResultHeader(i)) should be (o))
    }

    it should "output a correct final table field based on the input type" in {
        val inputResults = Map(
            Seq(BaseStringField("StringConcat_name")) -> Seq(BaseStringField("StringConcat_name"))
        )

        inputResults.foreach((i, o) => StringConcat(F("name"), ",").outputTableFields(TableResultHeader(i)) should be (o))
    }

    it should "produce a partial result" in {
        val inputResults : Seq[(TableResultHeader, Seq[Seq[Option[TableValue]]], Seq[Option[TableValue]])] = Seq(
            (TableResultHeader(Seq(BaseStringField("name"))), Seq(Seq(Some(StringValue("a"))), Seq(Some(StringValue("b"))), Seq(Some(StringValue("a")))), Seq(Some(StringValue("a,b,a"))))
        )

        inputResults.foreach((header, items, output) => StringConcat(F("name"), ",").resolve(header)(items) should be (output))
    }

    it should "return None if all elements are None" in {
        val inputResults : Seq[(TableResultHeader, Seq[Seq[Option[TableValue]]], Seq[Option[TableValue]])] = Seq(
            (TableResultHeader(Seq(BaseStringField("name"))), Seq(Seq(None), Seq(None), Seq(None)), Seq(None))

        )

        inputResults.foreach((header, items, output) => StringConcat(F("name"), ",").resolve(header)(items) should be (output))
    }

    it should "produce a final result" in {
        val inputResults : Seq[(TableResultHeader, Seq[Seq[Option[TableValue]]], Seq[Option[TableValue]])] = Seq(
            (TableResultHeader(Seq(BaseIntField("StringConcat_name"))), Seq(Seq(Some(StringValue("a,b,a"))), Seq(Some(StringValue("c,e")))), Seq(Some(StringValue("a,b,a,c,e"))))
        )

        inputResults.foreach((header, items, output) => StringConcat(F("name"), ",").assemble(header)(items) should be (output))
    }
}

class DistinctStringConcatTest extends UnitSpec {
    "A CountExpression" should "convert to protobuf correctly" in {
        val namedExpression = table_model.NamedExpression("name", Some(table_model.Expression().withValue(table_model.Value().withField("name"))))
        DistinctStringConcat(F("name"), ",").protobuf should be (table_model.AggregateExpression(table_model.AggregateExpression.AggregateType.STRING_CONCAT_DISTINCT, Some(namedExpression), Some(",")))
    }

    it should "output a correct partial table field based on the input type" in {
        val inputResults = Map(
            Seq(BaseStringField("name")) -> Seq(BaseStringField("StringConcatDistinct_name"))
        )

        inputResults.foreach((i, o) => DistinctStringConcat(F("name"), ",").outputPartialTableFields(TableResultHeader(i)) should be (o))
    }

    it should "output a correct final table field based on the input type" in {
        val inputResults = Map(
            Seq(BaseStringField("StringConcatDistinct_name")) -> Seq(BaseStringField("StringConcatDistinct_name"))
        )

        inputResults.foreach((i, o) => DistinctStringConcat(F("name"), ",").outputTableFields(TableResultHeader(i)) should be (o))
    }

    it should "produce a partial result" in {
        val inputResults : Seq[(TableResultHeader, Seq[Seq[Option[TableValue]]], Seq[Option[TableValue]])] = Seq(
            (TableResultHeader(Seq(BaseStringField("name"))), Seq(Seq(Some(StringValue("a"))), Seq(Some(StringValue("b"))), Seq(Some(StringValue("a")))), Seq(Some(StringValue("a,b"))))
        )

        inputResults.foreach((header, items, output) => DistinctStringConcat(F("name"), ",").resolve(header)(items) should be (output))
    }

    it should "return None if all elements are None" in {
        val inputResults : Seq[(TableResultHeader, Seq[Seq[Option[TableValue]]], Seq[Option[TableValue]])] = Seq(
            (TableResultHeader(Seq(BaseStringField("name"))), Seq(Seq(None), Seq(None), Seq(None)), Seq(None))

        )

        inputResults.foreach((header, items, output) => DistinctStringConcat(F("name"), ",").resolve(header)(items) should be (output))
    }

    it should "produce a final result" in {
        val inputResults : Seq[(TableResultHeader, Seq[Seq[Option[TableValue]]], Seq[Option[TableValue]])] = Seq(
            (TableResultHeader(Seq(BaseIntField("StringConcatDistinct_name"))), Seq(Seq(Some(StringValue("a,b"))), Seq(Some(StringValue("a,e")))), Seq(Some(StringValue("a,b,e"))))
        )

        inputResults.foreach((header, items, output) => DistinctStringConcat(F("name"), ",").assemble(header)(items) should be (output))
    }
}