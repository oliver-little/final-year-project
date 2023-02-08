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
            Seq(BaseIntField("name")) -> Seq(BaseIntField("Max_name")),
            Seq(BaseDoubleField("name")) -> Seq(BaseDoubleField("Max_name")),
            Seq(BaseDateTimeField("name")) -> Seq(BaseDateTimeField("Max_name")),
        )

        inputResults.foreach((i, o) => Max(F("name")).outputPartialTableFields(TableResultHeader(i)) should be (o))
    }

    it should "output a correct final table field based on the input type" in {
        val inputResults = Map(
            Seq(BaseIntField("Max_name")) -> Seq(BaseIntField("Max_name")),
            Seq(BaseDoubleField("Max_name")) -> Seq(BaseDoubleField("Max_name")),
            Seq(BaseDateTimeField("Max_name")) -> Seq(BaseDateTimeField("Max_name")),
        )

        inputResults.foreach((i, o) => Max(F("name")).outputTableFields(TableResultHeader(i)) should be (o))
    }

    it should "produce a partial result" in {
        val inputResults : Seq[(TableResultHeader, Seq[Seq[Option[TableValue]]], Seq[Option[TableValue]])] = Seq(
            (TableResultHeader(Seq(BaseIntField("name"))), Seq(Seq(Some(IntValue(1))), Seq(Some(IntValue(3))), Seq(Some(IntValue(1)))), Seq(Some(IntValue(3)))),
            (TableResultHeader(Seq(BaseDoubleField("name"))), Seq(Seq(Some(DoubleValue(1.0))), Seq(Some(DoubleValue(3.0))), Seq(Some(DoubleValue(1.0)))), Seq(Some(DoubleValue(3.0)))),
            (TableResultHeader(Seq(BaseDateTimeField("name"))), Seq(Seq(Some(DateTimeValue(Instant.ofEpochSecond(1)))), Seq(Some(DateTimeValue(Instant.ofEpochSecond(3)))), Seq(Some(DateTimeValue(Instant.ofEpochSecond(1))))), Seq(Some(DateTimeValue(Instant.ofEpochSecond(3)))))
        )

        inputResults.foreach((header, items, output) => Max(F("name")).resolve(header)(items) should be (output))
    }

    it should "produce None as a partial result if all elements are None" in {
        val inputResults : Seq[(TableResultHeader, Seq[Seq[Option[TableValue]]], Seq[Option[TableValue]])] = Seq(
            (TableResultHeader(Seq(BaseIntField("name"))), Seq(Seq(None), Seq(None), Seq(None)), Seq(None)),
            (TableResultHeader(Seq(BaseDoubleField("name"))), Seq(Seq(None), Seq(None), Seq(None)), Seq(None)),
            (TableResultHeader(Seq(BaseDateTimeField("name"))), Seq(Seq(None), Seq(None), Seq(None)), Seq(None))
        )

        inputResults.foreach((header, items, output) => Max(F("name")).resolve(header)(items) should be (output))
    }

    it should "produce a final result" in {
        val inputResults : Seq[(TableResultHeader, Seq[Seq[Option[TableValue]]], Seq[Option[TableValue]])] = Seq(
            (TableResultHeader(Seq(BaseIntField("Max_name"))), Seq(Seq(Some(IntValue(1))), Seq(Some(IntValue(3))), Seq(Some(IntValue(1)))), Seq(Some(IntValue(3)))),
            (TableResultHeader(Seq(BaseDoubleField("Max_name"))), Seq(Seq(Some(DoubleValue(1.0))), Seq(Some(DoubleValue(3.0))), Seq(Some(DoubleValue(1.0)))), Seq(Some(DoubleValue(3.0)))),
            (TableResultHeader(Seq(BaseDateTimeField("Max_name"))), Seq(Seq(Some(DateTimeValue(Instant.ofEpochSecond(1)))), Seq(Some(DateTimeValue(Instant.ofEpochSecond(3)))), Seq(Some(DateTimeValue(Instant.ofEpochSecond(1))))), Seq(Some(DateTimeValue(Instant.ofEpochSecond(3)))))
        )

        inputResults.foreach((header, items, output) => Max(F("name")).assemble(header)(items) should be (output))
    }

    it should "produce None as a final result if all partial elements are None" in {
        val inputResults : Seq[(TableResultHeader, Seq[Seq[Option[TableValue]]], Seq[Option[TableValue]])] = Seq(
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
    "A MaxExpression" should "convert to protobuf correctly" in {
        val namedExpression = table_model.NamedExpression("name", Some(table_model.Expression().withValue(table_model.Value().withField("name"))))
        Min(F("name")).protobuf should be (table_model.AggregateExpression(table_model.AggregateExpression.AggregateType.MIN, Some(namedExpression)))
    }

    it should "output a correct partial table field based on the input type" in {
        val inputResults = Map(
            Seq(BaseIntField("name")) -> Seq(BaseIntField("Max_name")),
            Seq(BaseDoubleField("name")) -> Seq(BaseDoubleField("Max_name")),
            Seq(BaseDateTimeField("name")) -> Seq(BaseDateTimeField("Max_name")),
        )

        inputResults.foreach((i, o) => Min(F("name")).outputPartialTableFields(TableResultHeader(i)) should be (o))
    }

    it should "output a correct final table field based on the input type" in {
        val inputResults = Map(
            Seq(BaseIntField("Max_name")) -> Seq(BaseIntField("Max_name")),
            Seq(BaseDoubleField("Max_name")) -> Seq(BaseDoubleField("Max_name")),
            Seq(BaseDateTimeField("Max_name")) -> Seq(BaseDateTimeField("Max_name")),
        )

        inputResults.foreach((i, o) => Min(F("name")).outputTableFields(TableResultHeader(i)) should be (o))
    }

    it should "produce a partial result" in {
        val inputResults : Seq[(TableResultHeader, Seq[Seq[Option[TableValue]]], Seq[Option[TableValue]])] = Seq(
            (TableResultHeader(Seq(BaseIntField("name"))), Seq(Seq(Some(IntValue(1))), Seq(Some(IntValue(3))), Seq(Some(IntValue(1)))), Seq(Some(IntValue(1)))),
            (TableResultHeader(Seq(BaseDoubleField("name"))), Seq(Seq(Some(DoubleValue(1.0))), Seq(Some(DoubleValue(3.0))), Seq(Some(DoubleValue(1.0)))), Seq(Some(DoubleValue(1.0)))),
            (TableResultHeader(Seq(BaseDateTimeField("name"))), Seq(Seq(Some(DateTimeValue(Instant.ofEpochSecond(1)))), Seq(Some(DateTimeValue(Instant.ofEpochSecond(3)))), Seq(Some(DateTimeValue(Instant.ofEpochSecond(1))))), Seq(Some(DateTimeValue(Instant.ofEpochSecond(1)))))
        )

        inputResults.foreach((header, items, output) => Min(F("name")).resolve(header)(items) should be (output))
    }

    it should "produce None as a partial result if all elements are None" in {
        val inputResults : Seq[(TableResultHeader, Seq[Seq[Option[TableValue]]], Seq[Option[TableValue]])] = Seq(
            (TableResultHeader(Seq(BaseIntField("name"))), Seq(Seq(None), Seq(None), Seq(None)), Seq(None)),
            (TableResultHeader(Seq(BaseDoubleField("name"))), Seq(Seq(None), Seq(None), Seq(None)), Seq(None)),
            (TableResultHeader(Seq(BaseDateTimeField("name"))), Seq(Seq(None), Seq(None), Seq(None)), Seq(None))
        )

        inputResults.foreach((header, items, output) => Min(F("name")).resolve(header)(items) should be (output))
    }

    it should "produce a final result" in {
        val inputResults : Seq[(TableResultHeader, Seq[Seq[Option[TableValue]]], Seq[Option[TableValue]])] = Seq(
            (TableResultHeader(Seq(BaseIntField("Max_name"))), Seq(Seq(Some(IntValue(1))), Seq(Some(IntValue(3))), Seq(Some(IntValue(1)))), Seq(Some(IntValue(1)))),
            (TableResultHeader(Seq(BaseDoubleField("Max_name"))), Seq(Seq(Some(DoubleValue(1.0))), Seq(Some(DoubleValue(3.0))), Seq(Some(DoubleValue(1.0)))), Seq(Some(DoubleValue(1.0)))),
            (TableResultHeader(Seq(BaseDateTimeField("Max_name"))), Seq(Seq(Some(DateTimeValue(Instant.ofEpochSecond(1)))), Seq(Some(DateTimeValue(Instant.ofEpochSecond(3)))), Seq(Some(DateTimeValue(Instant.ofEpochSecond(1))))), Seq(Some(DateTimeValue(Instant.ofEpochSecond(1)))))
        )

        inputResults.foreach((header, items, output) => Min(F("name")).assemble(header)(items) should be (output))
    }

    it should "produce None as a final result if all partial elements are None" in {
        val inputResults : Seq[(TableResultHeader, Seq[Seq[Option[TableValue]]], Seq[Option[TableValue]])] = Seq(
            (TableResultHeader(Seq(BaseIntField("Max_name"))), Seq(Seq(None), Seq(None), Seq(None)), Seq(None)),
            (TableResultHeader(Seq(BaseDoubleField("Max_name"))), Seq(Seq(None), Seq(None), Seq(None)), Seq(None)),
            (TableResultHeader(Seq(BaseDateTimeField("Max_name"))), Seq(Seq(None), Seq(None), Seq(None)), Seq(None))
        )

        inputResults.foreach((header, items, output) => Min(F("name")).assemble(header)(items) should be (output))
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