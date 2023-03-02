package org.oliverlittle.clusterprocess.model.field.comparisons

import java.time.{Instant, LocalDateTime}

import org.oliverlittle.clusterprocess.model.table._
import org.oliverlittle.clusterprocess.model.table.field._
import org.oliverlittle.clusterprocess.model.field.expressions._
import org.oliverlittle.clusterprocess.UnitSpec
import java.time.ZoneOffset

class UnaryFieldComparisonSpec extends UnitSpec {
    // NOTE: currently nulls are not handled correctly (they will cause a not well-typed error, as intended)
    // Need to find some way of implementing them (likely Some/None syntax) so they are able to propagate through the system
    "A UnaryFieldComparison" should "evaluate null comparators correctly" in {
        val inputs : Seq[(FieldExpression, UnaryComparator, Seq[Option[TableValue]], Boolean)] = Seq(
            (F("test"), UnaryComparator.IS_NULL, Seq(None), true),
            (F("test"), UnaryComparator.NULL, Seq(None), true),
            (V(1), UnaryComparator.IS_NULL, Seq(None), false),
            (V(1), UnaryComparator.NULL, Seq(None), false),
        )

        inputs.foreach((l, e, v, result) => {
            UnaryFieldComparison(l, e).resolve(TableResultHeader(Seq(BaseIntField("test")))).evaluate(v) should be (result)
        })
    }

    it should "evaluate not null comparators correctly" in {
        val inputs : Seq[(FieldExpression, UnaryComparator, Seq[Option[TableValue]], Boolean)] = Seq(
            (F("test"), UnaryComparator.IS_NOT_NULL, Seq(None), false),
            (F("test"), UnaryComparator.NOT_NULL, Seq(None), false),
            (V(1), UnaryComparator.IS_NOT_NULL, Seq(None), true),
            (V(1), UnaryComparator.NOT_NULL, Seq(None), true),
        )

        inputs.foreach((l, e, v, result) => {
            UnaryFieldComparison(l, e).resolve(TableResultHeader(Seq(BaseIntField("test")))).evaluate(v) should be (result)
        })
    }
}

class EqualityFieldComparisonSpec extends UnitSpec {
    "An EqualityFieldComparison" should "compare strings correctly" in {
        val inputs = Seq(
            (V("s"), EqualsComparator.EQ, V("s"), true),
            (V("s"), EqualsComparator.EQ, V("r"), false),
            (V("s"), EqualsComparator.NE, V("s"), false),
            (V("s"), EqualsComparator.NE, V("r"), true),
        )

        inputs.foreach((l, e, r, result) => {
            EqualityFieldComparison(l, e, r).resolve(TableResultHeader(Seq())).evaluate(Seq()) should be (result)
        })
    }

    it should "compare Longs correctly" in {
        val inputs = Seq(
            (V(1), EqualsComparator.EQ, V(1), true),
            (V(1), EqualsComparator.EQ, V(2), false),
            (V(1), EqualsComparator.NE, V(1), false),
            (V(1), EqualsComparator.NE, V(2), true),
        )

        inputs.foreach((l, e, r, result) => {
            EqualityFieldComparison(l, e, r).resolve(TableResultHeader(Seq())).evaluate(Seq()) should be (result)
        })
    }

    it should "compare Doubles correctly" in {
        val inputs = Seq(
            (V(1.01), EqualsComparator.EQ, V(1.01), true),
            (V(1.01), EqualsComparator.EQ, V(1.02), false),
            (V(1.01), EqualsComparator.NE, V(1.01), false),
            (V(1.01), EqualsComparator.NE, V(1.02), true),
        )

        inputs.foreach((l, e, r, result) => {
            EqualityFieldComparison(l, e, r).resolve(TableResultHeader(Seq())).evaluate(Seq()) should be (result)
        })
    }

    it should "compare DateTimes correctly" in {
        val inputs = Seq(
            (V(LocalDateTime.of(2000, 1, 1, 1, 0, 0, 0).atOffset(ZoneOffset.UTC).toInstant), EqualsComparator.EQ, V(LocalDateTime.of(2000, 1, 1, 1, 0, 0, 0).atOffset(ZoneOffset.UTC).toInstant), true),
            (V(LocalDateTime.of(2000, 1, 1, 1, 0, 0, 0).atOffset(ZoneOffset.UTC).toInstant), EqualsComparator.EQ, V(LocalDateTime.of(2000, 1, 1, 1, 1, 0, 0).atOffset(ZoneOffset.UTC).toInstant), false),
            (V(LocalDateTime.of(2000, 1, 1, 1, 0, 0, 0).atOffset(ZoneOffset.UTC).toInstant), EqualsComparator.NE, V(LocalDateTime.of(2000, 1, 1, 1, 0, 0, 0).atOffset(ZoneOffset.UTC).toInstant), false),
            (V(LocalDateTime.of(2000, 1, 1, 1, 0, 0, 0).atOffset(ZoneOffset.UTC).toInstant), EqualsComparator.NE, V(LocalDateTime.of(2000, 1, 1, 1, 1, 0, 0).atOffset(ZoneOffset.UTC).toInstant), true),
        )

        inputs.foreach((l, e, r, result) => {
            EqualityFieldComparison(l, e, r).resolve(TableResultHeader(Seq())).evaluate(Seq()) should be (result)
        })
    }

    it should "compare Booleans correctly" in {
        val inputs = Seq(
            (V(1.01), EqualsComparator.EQ, V(1.01), true),
            (V(1.01), EqualsComparator.EQ, V(1.02), false),
            (V(1.01), EqualsComparator.NE, V(1.01), false),
            (V(1.01), EqualsComparator.NE, V(1.02), true),
        )

        inputs.foreach((l, e, r, result) => {
            EqualityFieldComparison(l, e, r).resolve(TableResultHeader(Seq())).evaluate(Seq()) should be (result)
        })
    }
}

class OrderedFieldComparisonSpec extends UnitSpec {
    "An OrderedFieldComparison" should "compare less than correctly" in {
        val inputs = Seq(
            (V(1 : Long), OrderedComparator.LT, V(2 : Long), true),
            (V(2 : Long), OrderedComparator.LT, V(2 : Long), false),
            (V(2 : Long), OrderedComparator.LT, V(1 : Long), false),
            (V(1 : Long), OrderedComparator.LESS_THAN, V(2 : Long), true),
            (V(2 : Long), OrderedComparator.LESS_THAN, V(2 : Long), false),
            (V(2 : Long), OrderedComparator.LESS_THAN, V(1 : Long), false),
        )

        inputs.foreach((l, e, r, result) => {
            OrderedFieldComparison(l, e, r).resolve(TableResultHeader(Seq())).evaluate(Seq()) should be (result)
        })
    }

    it should "compare less than equals correctly" in {
        val inputs = Seq(
            (V(1 : Long), OrderedComparator.LTE, V(2 : Long), true),
            (V(2 : Long), OrderedComparator.LTE, V(2 : Long), true),
            (V(2 : Long), OrderedComparator.LTE, V(1 : Long), false),
            (V(1 : Long), OrderedComparator.LESS_THAN_EQUAL, V(2 : Long), true),
            (V(2 : Long), OrderedComparator.LESS_THAN_EQUAL, V(2 : Long), true),
            (V(2 : Long), OrderedComparator.LESS_THAN_EQUAL, V(1 : Long), false),
        )

        inputs.foreach((l, e, r, result) => {
            OrderedFieldComparison(l, e, r).resolve(TableResultHeader(Seq())).evaluate(Seq()) should be (result)
        })
    }

    it should "compare greater than  correctly" in {
        val inputs = Seq(
            (V(1 : Long), OrderedComparator.GT, V(2 : Long), false),
            (V(2 : Long), OrderedComparator.GT, V(2 : Long), false),
            (V(2 : Long), OrderedComparator.GT, V(1 : Long), true),
            (V(1 : Long), OrderedComparator.GREATER_THAN, V(2 : Long), false),
            (V(2 : Long), OrderedComparator.GREATER_THAN, V(2 : Long), false),
            (V(2 : Long), OrderedComparator.GREATER_THAN, V(1 : Long), true),
        )

        inputs.foreach((l, e, r, result) => {
            OrderedFieldComparison(l, e, r).resolve(TableResultHeader(Seq())).evaluate(Seq()) should be (result)
        })
    }

    it should "compare greater than equals correctly" in {
        val inputs = Seq(
            (V(1 : Long), OrderedComparator.GTE, V(2 : Long), false),
            (V(2 : Long), OrderedComparator.GTE, V(2 : Long), true),
            (V(2 : Long), OrderedComparator.GTE, V(1 : Long), true),
            (V(1 : Long), OrderedComparator.GREATER_THAN_EQUAL, V(2 : Long), false),
            (V(2 : Long), OrderedComparator.GREATER_THAN_EQUAL, V(2 : Long), true),
            (V(2 : Long), OrderedComparator.GREATER_THAN_EQUAL, V(1 : Long), true),
        )

        inputs.foreach((l, e, r, result) => {
            OrderedFieldComparison(l, e, r).resolve(TableResultHeader(Seq())).evaluate(Seq()) should be (result)
        })
    }

    it should "compare strings correctly" in {
        val inputs = Seq(
            (V("a"), OrderedComparator.LT, V("b"), true),
            (V("b"), OrderedComparator.LT, V("b"), false),
            (V("b"), OrderedComparator.LTE, V("b"), true),
            (V("b"), OrderedComparator.GTE, V("b"), true),
            (V("b"), OrderedComparator.GT, V("b"), false),
            (V("b"), OrderedComparator.GT, V("a"), true),
        )

        inputs.foreach((l, e, r, result) => {
            OrderedFieldComparison(l, e, r).resolve(TableResultHeader(Seq())).evaluate(Seq()) should be (result)
        })
    }

    it should "compare dates correctly" in {
        val date1 = LocalDateTime.of(2000, 1, 1, 1, 0, 0, 0).atOffset(ZoneOffset.UTC).toInstant
        val date2 = LocalDateTime.of(2000, 1, 1, 2, 0, 0, 0).atOffset(ZoneOffset.UTC).toInstant

        val inputs = Seq(
            (V(date1), OrderedComparator.LT, V(date2), true),
            (V(date2), OrderedComparator.LT, V(date2), false),
            (V(date2), OrderedComparator.LTE, V(date2), true),
            (V(date2), OrderedComparator.GTE, V(date2), true),
            (V(date2), OrderedComparator.GT, V(date2), false),
            (V(date2), OrderedComparator.GT, V(date1), true),
        )

        inputs.foreach((l, e, r, result) => {
            OrderedFieldComparison(l, e, r).resolve(TableResultHeader(Seq())).evaluate(Seq()) should be (result)
        })
    }
}

class StringFieldComparisonTest extends UnitSpec {
    "A StringFieldComparison" should "calculate contains correctly" in {
        StringFieldComparison(V("hello"), StringComparator.CONTAINS, V("ell")).resolve(TableResultHeader(Seq())).evaluate(Seq()) should be (true)
        StringFieldComparison(V("hello"), StringComparator.CONTAINS, V("elL")).resolve(TableResultHeader(Seq())).evaluate(Seq()) should be (false)
        StringFieldComparison(V("hello"), StringComparator.CONTAINS, V("elll")).resolve(TableResultHeader(Seq())).evaluate(Seq()) should be (false)
    }

    it should "calculate case insensitive contains correctly" in {
        StringFieldComparison(V("hello"), StringComparator.ICONTAINS, V("ell")).resolve(TableResultHeader(Seq())).evaluate(Seq()) should be (true)
        StringFieldComparison(V("hello"), StringComparator.ICONTAINS, V("elL")).resolve(TableResultHeader(Seq())).evaluate(Seq()) should be (true)
        StringFieldComparison(V("hello"), StringComparator.ICONTAINS, V("elll")).resolve(TableResultHeader(Seq())).evaluate(Seq()) should be (false)
    }

    it should "calculate starts with correctly" in {
        StringFieldComparison(V("hello"), StringComparator.STARTS_WITH, V("hel")).resolve(TableResultHeader(Seq())).evaluate(Seq()) should be (true)
        StringFieldComparison(V("hello"), StringComparator.STARTS_WITH, V("Hel")).resolve(TableResultHeader(Seq())).evaluate(Seq()) should be (false)
        StringFieldComparison(V("hello"), StringComparator.STARTS_WITH, V("ell")).resolve(TableResultHeader(Seq())).evaluate(Seq()) should be (false)
    }

    it should "calculate case insensitive starts with correctly" in {
        StringFieldComparison(V("hello"), StringComparator.ISTARTS_WITH, V("hel")).resolve(TableResultHeader(Seq())).evaluate(Seq()) should be (true)
        StringFieldComparison(V("hello"), StringComparator.ISTARTS_WITH, V("Hel")).resolve(TableResultHeader(Seq())).evaluate(Seq()) should be (true)
        StringFieldComparison(V("hello"), StringComparator.ISTARTS_WITH, V("ell")).resolve(TableResultHeader(Seq())).evaluate(Seq()) should be (false)
    }

    it should "calculate ends with correctly" in {
        StringFieldComparison(V("hello"), StringComparator.ENDS_WITH, V("llo")).resolve(TableResultHeader(Seq())).evaluate(Seq()) should be (true)
        StringFieldComparison(V("hello"), StringComparator.ENDS_WITH, V("Llo")).resolve(TableResultHeader(Seq())).evaluate(Seq()) should be (false)
        StringFieldComparison(V("hello"), StringComparator.ENDS_WITH, V("lllo")).resolve(TableResultHeader(Seq())).evaluate(Seq()) should be (false)
    }

    it should "calculate case insensitive ends with correctly" in {
        StringFieldComparison(V("hello"), StringComparator.IENDS_WITH, V("llo")).resolve(TableResultHeader(Seq())).evaluate(Seq()) should be (true)
        StringFieldComparison(V("hello"), StringComparator.IENDS_WITH, V("Llo")).resolve(TableResultHeader(Seq())).evaluate(Seq()) should be (true)
        StringFieldComparison(V("hello"), StringComparator.IENDS_WITH, V("lllo")).resolve(TableResultHeader(Seq())).evaluate(Seq()) should be (false)
    }
}