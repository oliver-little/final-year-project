package org.oliverlittle.clusterprocess.model.table

import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.model.field.expressions._
import org.oliverlittle.clusterprocess.model.field.comparisons._
import org.oliverlittle.clusterprocess.model.table.field._
import org.oliverlittle.clusterprocess.UnitSpec

class SelectTransformationTest extends UnitSpec {
    "A SelectTransformation" should "be valid when all expressions are valid" in {
        SelectTransformation(F("name"), FieldOperations.AddInt(V(1), V(2)).as("sum")).isValid(TableResultHeader(Seq(BaseIntField("name")))) should be (true)
    }

    it should "be invalid when any expression is invalid" in {
        SelectTransformation(F("name"), FieldOperations.AddInt(V(1), V("a")).as("sum")).isValid(TableResultHeader(Seq(BaseIntField("name")))) should be (false)
    }

    it should "output correct partial and final headers" in {
        SelectTransformation(F("name"), FieldOperations.AddInt(V(1), V("a")).as("sum")).outputHeaders(TableResultHeader(Seq(BaseStringField("name")))) should be (TableResultHeader(Seq(BaseStringField("name"), BaseIntField("sum"))))
        SelectTransformation(F("name"), FieldOperations.AddInt(V(1), V("a")).as("sum")).outputPartialHeaders(TableResultHeader(Seq(BaseStringField("name")))) should be (TableResultHeader(Seq(BaseStringField("name"), BaseIntField("sum"))))
    }

    it should "convert to protobuf" in {
        SelectTransformation(F("name"), FieldOperations.AddInt(V(1), V(1)).as("sum")).protobuf should be (table_model.Table.TableTransformation().withSelect(table_model.Select(Seq(F.fToNamed(F("name")).protobuf, FieldOperations.AddInt(V(1), V(1)).as("sum").protobuf))))
    }

    it should "evaluate to a TableResult" in {
        val input = LazyTableResult(
            TableResultHeader(Seq(BaseStringField("name"))),
            Seq(
                Seq(Some(StringValue("a"))),
                Seq(Some(StringValue("b")))
            )
        )

        val output = LazyTableResult(
            TableResultHeader(Seq(BaseStringField("name"), BaseIntField("sum"))),
            Seq(
                Seq(Some(StringValue("a")), Some(IntValue(2))),
                Seq(Some(StringValue("b")), Some(IntValue(2)))
            )
        )

        SelectTransformation(F("name"), FieldOperations.AddInt(V(1), V(1)).as("sum")).evaluate(input) should be (output)
    }

    it should "combine partial results" in {
        val inputs = Seq(
            LazyTableResult(
                TableResultHeader(Seq(BaseStringField("name"))),
                Seq(
                    Seq(Some(StringValue("a"))),
                    Seq(Some(StringValue("b")))
                )
            ),
            LazyTableResult(
                TableResultHeader(Seq(BaseStringField("name"))),
                Seq(
                    Seq(Some(StringValue("c"))),
                    Seq(Some(StringValue("d")))
                )
            )
        )

        val output = LazyTableResult(
            TableResultHeader(Seq(BaseStringField("name"))),
            Seq(
                Seq(Some(StringValue("a"))),
                Seq(Some(StringValue("b"))),
                Seq(Some(StringValue("c"))),
                Seq(Some(StringValue("d")))
            )
        )

        SelectTransformation(F("name")).assemblePartial(inputs) should be (output)
    }
}

class FilterTransformationTest extends UnitSpec {
    "A FilterTransformation" should "be valid when all filters are valid" in {
        FilterTransformation(EqualityFieldComparison(F("name"), EqualsComparator.EQ,  V(1))).isValid(TableResultHeader(Seq(BaseIntField("name")))) should be (true)
    }

    it should "be invalid when any part of the filter is invalid" in {
        FilterTransformation(CombinedFieldComparison(EqualityFieldComparison(F("name"), EqualsComparator.EQ,   V(1)), BooleanOperator.AND, EqualityFieldComparison(F("name1"), EqualsComparator.EQ,  V(1)))).isValid(TableResultHeader(Seq(BaseIntField("name1")))) should be (false)
    }

    it should "output correct partial and final headers" in {
        val header = TableResultHeader(Seq(BaseIntField("name")))
        FilterTransformation(EqualityFieldComparison(F("name"), EqualsComparator.EQ,  V(1))).outputHeaders(header) should be (header)
        FilterTransformation(EqualityFieldComparison(F("name"), EqualsComparator.EQ,  V(1))).outputPartialHeaders(header) should be (header)
    }

    it should "convert to protobuf" in {
        val filter = EqualityFieldComparison(F("name"), EqualsComparator.EQ,  V(1))
        
        FilterTransformation(filter).protobuf should be (table_model.Table.TableTransformation().withFilter(filter.protobuf))
    }

    it should "evaluate to a TableResult" in {
        val header = TableResultHeader(Seq(BaseIntField("name")))
        val filter = EqualityFieldComparison(F("name"), EqualsComparator.NE,  V(1))

        val input = LazyTableResult(
            header,
            Seq(
                Seq(Some(IntValue(1))),
                Seq(Some(IntValue(2)))
            )
        )

        val output = LazyTableResult(
            header,
            Seq(
                Seq(Some(IntValue(2)))
            )
        )

        FilterTransformation(filter).evaluate(input) should be (output)
    }

    it should "combine partial results" in {
        val header = TableResultHeader(Seq(BaseIntField("name")))
        val filter = EqualityFieldComparison(F("name"), EqualsComparator.NE,  V(1))

        val inputs = Seq(
            LazyTableResult(
                header,
                Seq(
                    Seq(Some(IntValue(2)))
                )
            ),
            LazyTableResult(
                header,
                Seq(
                    Seq(Some(IntValue(3))),
                    Seq(Some(IntValue(4)))
                )
            )
        )

        val output = LazyTableResult(
            header,
            Seq(
                Seq(Some(IntValue(2))),
                Seq(Some(IntValue(3))),
                Seq(Some(IntValue(4)))
            )
        )

        FilterTransformation(filter).assemblePartial(inputs) should be (output)
    }
}

class AggregateTransformationTest extends UnitSpec {
    "An AggregateExpression" should "be valid when all aggregates are valid" in {
        val header = TableResultHeader(Seq(BaseIntField("name")))
        AggregateTransformation(Max(F("name")), Min(FieldOperations.AddInt(V(1), V(2)).as("sum"))).isValid(header) should be (true)
    }

    it should "be invalid when any aggregate is invalid" in {
        val header = TableResultHeader(Seq(BaseIntField("name")))
        AggregateTransformation(Max(F("name1")), Min(FieldOperations.AddInt(V(1), V(2)).as("sum"))).isValid(header) should be (true)
    }

    it should "output correct partial headers" in {
        val header = TableResultHeader(Seq(BaseIntField("name")))
        AggregateTransformation(Avg(F("name"))).outputPartialHeaders(header) should be (TableResultHeader(Seq(BaseIntField("AvgSum_name"), BaseIntField("AvgCount_name"))))
    }

    it should "output correct final headers" in {
        val header = TableResultHeader(Seq(BaseIntField("AvgSum_name"), BaseIntField("AvgCount_name")))
        AggregateTransformation(Avg(F("name"))).outputHeaders(header) should be (TableResultHeader(Seq(BaseDoubleField("Avg_name"))))
    }

    it should "convert to protobuf" in {
        AggregateTransformation(Avg(F("name"))).protobuf should be (table_model.Table.TableTransformation().withAggregate(table_model.Aggregate(Seq(Avg(F("name")).protobuf))))
    }

    it should "evaluate to a partial TableResult" in {
        val inHeader = TableResultHeader(Seq(BaseIntField("name")))
        val outHeader = TableResultHeader(Seq(BaseIntField("AvgSum_name"), BaseIntField("AvgCount_name")))

        val input = LazyTableResult(
            inHeader,
            Seq(
                Seq(Some(IntValue(2))),
                Seq(Some(IntValue(4)))
            )
        )

        val output = LazyTableResult(
            outHeader,
            Seq(
                Seq(Some(IntValue(6)), Some(IntValue(2)))
            )
        )

        AggregateTransformation(Avg(F("name"))).evaluate(input) should be (output)
    }

    it should "combine partial results" in {
        val inHeader = TableResultHeader(Seq(BaseIntField("AvgSum_name"), BaseIntField("AvgCount_name")))
        val outHeader = TableResultHeader(Seq(BaseDoubleField("Avg_name")))

         val input = Seq(
            LazyTableResult(
                inHeader,
                Seq(
                    Seq(Some(IntValue(6)), Some(IntValue(2)))

                )
            ),
            LazyTableResult(
                inHeader,
                Seq(
                    Seq(Some(IntValue(12)), Some(IntValue(4)))

                )
            )
         )

        val output = LazyTableResult(
            outHeader,
            Seq(
                Seq(Some(DoubleValue(3.0)))
            )
        )

        AggregateTransformation(Avg(F("name"))).assemblePartial(input) should be (output)
    }
}