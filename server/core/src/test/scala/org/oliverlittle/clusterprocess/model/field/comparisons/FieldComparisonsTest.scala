package org.oliverlittle.clusterprocess.model.field.comparisons

import java.time.{Instant, LocalDateTime}

import org.oliverlittle.clusterprocess.model.field.expressions._
import org.oliverlittle.clusterprocess.UnitSpec
import java.time.ZoneOffset

class UnaryFieldComparisonSpec extends UnitSpec {
    // NOTE: currently nulls are not handled correctly (they will cause a not well-typed error, as intended)
    // Need to find some way of implementing them (likely Some/None syntax) so they are able to propagate through the system
    "A UnaryFieldComparison" should "evaluate null comparators correctly" in {
        val inputs = Seq(
            (V(null), UnaryComparator.IS_NULL, true),
            (V(null), UnaryComparator.NULL, true),
            (V(1), UnaryComparator.IS_NULL, false),
            (V(1), UnaryComparator.NULL, false),
        )

        inputs.foreach((l, e, result) => {
            assertThrows[IllegalArgumentException] {
                UnaryFieldComparison(l, e).resolve(Map()).evaluate(Map()) should be (result)
            }
        })
    }

    it should "evaluate not null comparators correctly" in {
        assertThrows[IllegalArgumentException] {
            UnaryFieldComparison(V(1), UnaryComparator.IS_NOT_NULL).resolve(Map()).evaluate(Map()) should be (true)
            UnaryFieldComparison(V(1), UnaryComparator.NOT_NULL).resolve(Map()).evaluate(Map()) should be (true)
            UnaryFieldComparison(V(null), UnaryComparator.IS_NOT_NULL).resolve(Map()).evaluate(Map()) should be (false)
            UnaryFieldComparison(V(null), UnaryComparator.NOT_NULL).resolve(Map()).evaluate(Map()) should be (false)
        }
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
            EqualityFieldComparison(l, e, r).resolve(Map()).evaluate(Seq()) should be (result)
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
            EqualityFieldComparison(l, e, r).resolve(Map()).evaluate(Seq()) should be (result)
        })
    }

    it should "compare Doubles correctly" in {
        EqualityFieldComparison(V(1.01), EqualsComparator.EQ, V(1.01)).resolve(Map()).evaluate(Map()) should be (true)
        EqualityFieldComparison(V(1.01), EqualsComparator.EQ, V(1.02)).resolve(Map()).evaluate(Map()) should be (false)
        EqualityFieldComparison(V(1.01), EqualsComparator.NE, V(1.01)).resolve(Map()).evaluate(Map()) should be (false)
        EqualityFieldComparison(V(1.01), EqualsComparator.NE, V(1.02)).resolve(Map()).evaluate(Map()) should be (true)

        val inputs = Seq(
            (V(1.01), EqualsComparator.EQ, V(1.01), true),
            (V(1.01), EqualsComparator.EQ, V(1.02), false),
            (V(1.01), EqualsComparator.NE, V(1.01), false),
            (V(1.01), EqualsComparator.NE, V(1.02), true),
        )

        inputs.foreach((l, e, r, result) => {
            EqualityFieldComparison(l, e, r).resolve(Map()).evaluate(Seq()) should be (result)
        })
    }

    it should "compare DateTimes correctly" in {
        EqualityFieldComparison(V(LocalDateTime.of(2000, 1, 1, 1, 0, 0, 0).atOffset(ZoneOffset.UTC).toInstant), EqualsComparator.EQ, V(LocalDateTime.of(2000, 1, 1, 1, 0, 0, 0).atOffset(ZoneOffset.UTC).toInstant)).resolve(Map()).evaluate(Map()) should be (true)
        EqualityFieldComparison(V(LocalDateTime.of(2000, 1, 1, 1, 0, 0, 0).atOffset(ZoneOffset.UTC).toInstant), EqualsComparator.EQ, V(LocalDateTime.of(2000, 1, 1, 1, 0, 1, 0).atOffset(ZoneOffset.UTC).toInstant)).resolve(Map()).evaluate(Map()) should be (false)
        EqualityFieldComparison(V(LocalDateTime.of(2000, 1, 1, 1, 0, 0, 0).atOffset(ZoneOffset.UTC).toInstant), EqualsComparator.NE, V(LocalDateTime.of(2000, 1, 1, 1, 0, 0, 0).atOffset(ZoneOffset.UTC).toInstant)).resolve(Map()).evaluate(Map()) should be (false)
        EqualityFieldComparison(V(LocalDateTime.of(2000, 1, 1, 1, 0, 0, 0).atOffset(ZoneOffset.UTC).toInstant), EqualsComparator.NE, V(LocalDateTime.of(2000, 1, 1, 1, 0, 1, 0).atOffset(ZoneOffset.UTC).toInstant)).resolve(Map()).evaluate(Map()) should be (true)
    }

    it should "compare Booleans correctly" in {
        EqualityFieldComparison(V(true), EqualsComparator.EQ, V(true)).resolve(Map()).evaluate(Map()) should be (true)
        EqualityFieldComparison(V(true), EqualsComparator.EQ, V(false)).resolve(Map()).evaluate(Map()) should be (false)
        EqualityFieldComparison(V(true), EqualsComparator.NE, V(true)).resolve(Map()).evaluate(Map()) should be (false)
        EqualityFieldComparison(V(true), EqualsComparator.NE, V(false)).resolve(Map()).evaluate(Map()) should be (true)
    }
}

class OrderedFieldComparisonSpec extends UnitSpec {
    "An OrderedFieldComparison" should "compare less than correctly" in {
        OrderedFieldComparison(V(1 : Long), OrderedComparator.LT, V(2 : Long)).resolve(Map()).evaluate(Map()) should be (true)
        OrderedFieldComparison(V(2: Long), OrderedComparator.LT, V(2 : Long)).resolve(Map()).evaluate(Map()) should be (false)
        OrderedFieldComparison(V(2 : Long), OrderedComparator.LT, V(1 : Long)).resolve(Map()).evaluate(Map()) should be (false)
        OrderedFieldComparison(V(1 : Long), OrderedComparator.LESS_THAN, V(2 : Long)).resolve(Map()).evaluate(Map()) should be (true)
        OrderedFieldComparison(V(2: Long), OrderedComparator.LESS_THAN, V(2 : Long)).resolve(Map()).evaluate(Map()) should be (false)
        OrderedFieldComparison(V(2 : Long), OrderedComparator.LESS_THAN, V(1 : Long)).resolve(Map()).evaluate(Map()) should be (false)
    }

    it should "compare less than equals correctly" in {
        OrderedFieldComparison(V(1 : Long), OrderedComparator.LTE, V(2 : Long)).resolve(Map()).evaluate(Map()) should be (true)
        OrderedFieldComparison(V(2 : Long), OrderedComparator.LTE, V(2 : Long)).resolve(Map()).evaluate(Map()) should be (true)
        OrderedFieldComparison(V(2 : Long), OrderedComparator.LTE, V(1 : Long)).resolve(Map()).evaluate(Map()) should be (false)
        OrderedFieldComparison(V(1 : Long), OrderedComparator.LESS_THAN_EQUAL, V(2 : Long)).resolve(Map()).evaluate(Map()) should be (true)
        OrderedFieldComparison(V(2 : Long), OrderedComparator.LESS_THAN_EQUAL, V(2 : Long)).resolve(Map()).evaluate(Map()) should be (true)
        OrderedFieldComparison(V(2 : Long), OrderedComparator.LESS_THAN_EQUAL, V(1 : Long)).resolve(Map()).evaluate(Map()) should be (false)
    }

    it should "compare greater than  correctly" in {
        OrderedFieldComparison(V(1 : Long), OrderedComparator.GT, V(2 : Long)).resolve(Map()).evaluate(Map()) should be (false)
        OrderedFieldComparison(V(2 : Long), OrderedComparator.GT, V(2 : Long)).resolve(Map()).evaluate(Map()) should be (false)
        OrderedFieldComparison(V(2 : Long), OrderedComparator.GT, V(1 : Long)).resolve(Map()).evaluate(Map()) should be (true)
        OrderedFieldComparison(V(1 : Long), OrderedComparator.GREATER_THAN, V(2 : Long)).resolve(Map()).evaluate(Map()) should be (false)
        OrderedFieldComparison(V(2 : Long), OrderedComparator.GREATER_THAN, V(2 : Long)).resolve(Map()).evaluate(Map()) should be (false)
        OrderedFieldComparison(V(2 : Long), OrderedComparator.GREATER_THAN, V(1 : Long)).resolve(Map()).evaluate(Map()) should be (true)
    }

    it should "compare greater than equals correctly" in {
        OrderedFieldComparison(V(1 : Long), OrderedComparator.GTE, V(2 : Long)).resolve(Map()).evaluate(Map()) should be (false)
        OrderedFieldComparison(V(2 : Long), OrderedComparator.GTE, V(2 : Long)).resolve(Map()).evaluate(Map()) should be (true)
        OrderedFieldComparison(V(2 : Long), OrderedComparator.GTE, V(1 : Long)).resolve(Map()).evaluate(Map()) should be (true)
        OrderedFieldComparison(V(1 : Long), OrderedComparator.GREATER_THAN_EQUAL, V(2 : Long)).resolve(Map()).evaluate(Map()) should be (false)
        OrderedFieldComparison(V(2 : Long), OrderedComparator.GREATER_THAN_EQUAL, V(2 : Long)).resolve(Map()).evaluate(Map()) should be (true)
        OrderedFieldComparison(V(2 : Long), OrderedComparator.GREATER_THAN_EQUAL, V(1 : Long)).resolve(Map()).evaluate(Map()) should be (true)
    }

    it should "compare strings correctly" in {
        OrderedFieldComparison(V("a"), OrderedComparator.LT, V("b")).resolve(Map()).evaluate(Map()) should be (true)
        OrderedFieldComparison(V("b"), OrderedComparator.LT, V("b")).resolve(Map()).evaluate(Map()) should be (false)
        OrderedFieldComparison(V("b"), OrderedComparator.LTE, V("b")).resolve(Map()).evaluate(Map()) should be (true)
        OrderedFieldComparison(V("b"), OrderedComparator.GTE, V("b")).resolve(Map()).evaluate(Map()) should be (true)
        OrderedFieldComparison(V("b"), OrderedComparator.GT, V("b")).resolve(Map()).evaluate(Map()) should be (false)
        OrderedFieldComparison(V("b"), OrderedComparator.GT, V("a")).resolve(Map()).evaluate(Map()) should be (true)
    }

    it should "compare dates correctly" in {
        val date1 = LocalDateTime.of(2000, 1, 1, 1, 0, 0, 0).atOffset(ZoneOffset.UTC).toInstant()
        val date2 = LocalDateTime.of(2000, 1, 1, 2, 0, 0, 0).atOffset(ZoneOffset.UTC).toInstant()
        OrderedFieldComparison(V(date1), OrderedComparator.LT, V(date2)).resolve(Map()).evaluate(Map()) should be (true)
        OrderedFieldComparison(V(date2), OrderedComparator.LT, V(date2)).resolve(Map()).evaluate(Map()) should be (false)
        OrderedFieldComparison(V(date2), OrderedComparator.LTE, V(date2)).resolve(Map()).evaluate(Map()) should be (true)
        OrderedFieldComparison(V(date2), OrderedComparator.GTE, V(date2)).resolve(Map()).evaluate(Map()) should be (true)
        OrderedFieldComparison(V(date2), OrderedComparator.GT, V(date2)).resolve(Map()).evaluate(Map()) should be (false)
        OrderedFieldComparison(V(date2), OrderedComparator.GT, V(date1)).resolve(Map()).evaluate(Map()) should be (true)
    }
}

class StringFieldComparisonSpec extends UnitSpec {
    "A StringFieldComparison" should "calculate contains correctly" in {
        StringFieldComparison(V("hello"), StringComparator.CONTAINS, V("ell")).resolve(Map()).evaluate(Map()) should be (true)
        StringFieldComparison(V("hello"), StringComparator.CONTAINS, V("elL")).resolve(Map()).evaluate(Map()) should be (false)
        StringFieldComparison(V("hello"), StringComparator.CONTAINS, V("elll")).resolve(Map()).evaluate(Map()) should be (false)
    }

    it should "calculate case insensitive contains correctly" in {
        StringFieldComparison(V("hello"), StringComparator.ICONTAINS, V("ell")).resolve(Map()).evaluate(Map()) should be (true)
        StringFieldComparison(V("hello"), StringComparator.ICONTAINS, V("elL")).resolve(Map()).evaluate(Map()) should be (true)
        StringFieldComparison(V("hello"), StringComparator.ICONTAINS, V("elll")).resolve(Map()).evaluate(Map()) should be (false)
    }

    it should "calculate starts with correctly" in {
        StringFieldComparison(V("hello"), StringComparator.STARTS_WITH, V("hel")).resolve(Map()).evaluate(Map()) should be (true)
        StringFieldComparison(V("hello"), StringComparator.STARTS_WITH, V("Hel")).resolve(Map()).evaluate(Map()) should be (false)
        StringFieldComparison(V("hello"), StringComparator.STARTS_WITH, V("ell")).resolve(Map()).evaluate(Map()) should be (false)
    }

    it should "calculate case insensitive starts with correctly" in {
        StringFieldComparison(V("hello"), StringComparator.ISTARTS_WITH, V("hel")).resolve(Map()).evaluate(Map()) should be (true)
        StringFieldComparison(V("hello"), StringComparator.ISTARTS_WITH, V("Hel")).resolve(Map()).evaluate(Map()) should be (true)
        StringFieldComparison(V("hello"), StringComparator.ISTARTS_WITH, V("ell")).resolve(Map()).evaluate(Map()) should be (false)
    }

    it should "calculate ends with correctly" in {
        StringFieldComparison(V("hello"), StringComparator.ENDS_WITH, V("llo")).resolve(Map()).evaluate(Map()) should be (true)
        StringFieldComparison(V("hello"), StringComparator.ENDS_WITH, V("Llo")).resolve(Map()).evaluate(Map()) should be (false)
        StringFieldComparison(V("hello"), StringComparator.ENDS_WITH, V("lllo")).resolve(Map()).evaluate(Map()) should be (false)
    }

    it should "calculate case insensitive ends with correctly" in {
        StringFieldComparison(V("hello"), StringComparator.IENDS_WITH, V("llo")).resolve(Map()).evaluate(Map()) should be (true)
        StringFieldComparison(V("hello"), StringComparator.IENDS_WITH, V("Llo")).resolve(Map()).evaluate(Map()) should be (true)
        StringFieldComparison(V("hello"), StringComparator.IENDS_WITH, V("lllo")).resolve(Map()).evaluate(Map()) should be (false)
    }
}