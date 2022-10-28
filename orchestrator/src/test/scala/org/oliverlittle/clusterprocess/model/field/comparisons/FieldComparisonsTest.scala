package org.oliverlittle.clusterprocess.model.field.comparisons

import org.oliverlittle.clusterprocess.model.field.expressions._
import org.oliverlittle.clusterprocess.UnitSpec
import java.time.OffsetDateTime
import java.time.ZoneOffset

class UnaryFieldComparisonSpec extends UnitSpec {
    // NOTE: currently nulls are not handled correctly (they will cause a not well-typed error, as intended)
    // Need to find some way of implementing them (likely Some/None syntax) so they are able to propagate through the system
    "A UnaryFieldComparison" should "evaluate null comparators correctly" in {
        UnaryFieldComparison(V(null), UnaryComparator.IS_NULL).evaluate should be (true)
        UnaryFieldComparison(V(null), UnaryComparator.NULL).evaluate should be (true)
        UnaryFieldComparison(V(1), UnaryComparator.IS_NULL).evaluate should be (false)
        UnaryFieldComparison(V(1), UnaryComparator.NULL).evaluate should be (false)
    }

    it should "evaluate not null comparators correctly" in {
        UnaryFieldComparison(V(1), UnaryComparator.IS_NOT_NULL).evaluate should be (true)
        UnaryFieldComparison(V(1), UnaryComparator.NOT_NULL).evaluate should be (true)
        UnaryFieldComparison(V(null), UnaryComparator.IS_NOT_NULL).evaluate should be (false)
        UnaryFieldComparison(V(null), UnaryComparator.NOT_NULL).evaluate should be (false)
    }
}

class OrderedFieldComparisonSpec extends UnitSpec {
    "An OrderedFieldComparison" should "compare less than correctly" in {
        OrderedFieldComparison[Long](V(1 : Long), OrderedComparator.LT, V(2 : Long)).evaluate should be (true)
        OrderedFieldComparison[Long](V(2: Long), OrderedComparator.LT, V(2 : Long)).evaluate should be (false)
        OrderedFieldComparison[Long](V(2 : Long), OrderedComparator.LT, V(1 : Long)).evaluate should be (false)
        OrderedFieldComparison[Long](V(1 : Long), OrderedComparator.LESS_THAN, V(2 : Long)).evaluate should be (true)
        OrderedFieldComparison[Long](V(2: Long), OrderedComparator.LESS_THAN, V(2 : Long)).evaluate should be (false)
        OrderedFieldComparison[Long](V(2 : Long), OrderedComparator.LESS_THAN, V(1 : Long)).evaluate should be (false)
    }

    it should "compare less than equals correctly" in {
        OrderedFieldComparison[Long](V(1 : Long), OrderedComparator.LTE, V(2 : Long)).evaluate should be (true)
        OrderedFieldComparison[Long](V(2 : Long), OrderedComparator.LTE, V(2 : Long)).evaluate should be (true)
        OrderedFieldComparison[Long](V(2 : Long), OrderedComparator.LTE, V(1 : Long)).evaluate should be (false)
        OrderedFieldComparison[Long](V(1 : Long), OrderedComparator.LESS_THAN_EQUAL, V(2 : Long)).evaluate should be (true)
        OrderedFieldComparison[Long](V(2 : Long), OrderedComparator.LESS_THAN_EQUAL, V(2 : Long)).evaluate should be (true)
        OrderedFieldComparison[Long](V(2 : Long), OrderedComparator.LESS_THAN_EQUAL, V(1 : Long)).evaluate should be (false)
    }

    it should "compare greater than  correctly" in {
        OrderedFieldComparison[Long](V(1 : Long), OrderedComparator.GT, V(2 : Long)).evaluate should be (false)
        OrderedFieldComparison[Long](V(2 : Long), OrderedComparator.GT, V(2 : Long)).evaluate should be (false)
        OrderedFieldComparison[Long](V(2 : Long), OrderedComparator.GT, V(1 : Long)).evaluate should be (true)
        OrderedFieldComparison[Long](V(1 : Long), OrderedComparator.GREATER_THAN, V(2 : Long)).evaluate should be (false)
        OrderedFieldComparison[Long](V(2 : Long), OrderedComparator.GREATER_THAN, V(2 : Long)).evaluate should be (false)
        OrderedFieldComparison[Long](V(2 : Long), OrderedComparator.GREATER_THAN, V(1 : Long)).evaluate should be (true)
    }

    it should "compare greater than equals correctly" in {
        OrderedFieldComparison[Long](V(1 : Long), OrderedComparator.GTE, V(2 : Long)).evaluate should be (false)
        OrderedFieldComparison[Long](V(2 : Long), OrderedComparator.GTE, V(2 : Long)).evaluate should be (true)
        OrderedFieldComparison[Long](V(2 : Long), OrderedComparator.GTE, V(1 : Long)).evaluate should be (true)
        OrderedFieldComparison[Long](V(1 : Long), OrderedComparator.GREATER_THAN_EQUAL, V(2 : Long)).evaluate should be (false)
        OrderedFieldComparison[Long](V(2 : Long), OrderedComparator.GREATER_THAN_EQUAL, V(2 : Long)).evaluate should be (true)
        OrderedFieldComparison[Long](V(2 : Long), OrderedComparator.GREATER_THAN_EQUAL, V(1 : Long)).evaluate should be (true)
    }

    it should "compare strings correctly" in {
        OrderedFieldComparison[String](V("a"), OrderedComparator.LT, V("b")).evaluate should be (true)
        OrderedFieldComparison[String](V("b"), OrderedComparator.LT, V("b")).evaluate should be (false)
        OrderedFieldComparison[String](V("b"), OrderedComparator.LTE, V("b")).evaluate should be (true)
        OrderedFieldComparison[String](V("b"), OrderedComparator.GTE, V("b")).evaluate should be (true)
        OrderedFieldComparison[String](V("b"), OrderedComparator.GT, V("b")).evaluate should be (false)
        OrderedFieldComparison[String](V("b"), OrderedComparator.GT, V("a")).evaluate should be (true)
    }

    it should "compare dates correctly" in {
        val date1 = OffsetDateTime.of(1, 1, 1, 1, 0, 0, 0, ZoneOffset.UTC)
        val date2 = OffsetDateTime.of(1, 1, 1, 2, 0, 0, 0, ZoneOffset.UTC)
        OrderedFieldComparison[OffsetDateTime](V(date1), OrderedComparator.LT, V(date2)).evaluate should be (true)
        OrderedFieldComparison[OffsetDateTime](V(date2), OrderedComparator.LT, V(date2)).evaluate should be (false)
        OrderedFieldComparison[OffsetDateTime](V(date2), OrderedComparator.LTE, V(date2)).evaluate should be (true)
        OrderedFieldComparison[OffsetDateTime](V(date2), OrderedComparator.GTE, V(date2)).evaluate should be (true)
        OrderedFieldComparison[OffsetDateTime](V(date2), OrderedComparator.GT, V(date2)).evaluate should be (false)
        OrderedFieldComparison[OffsetDateTime](V(date2), OrderedComparator.GT, V(date1)).evaluate should be (true)
    }
}