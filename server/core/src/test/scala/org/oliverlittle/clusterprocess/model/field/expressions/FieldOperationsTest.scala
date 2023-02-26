package org.oliverlittle.clusterprocess.model.field.expressions

import org.oliverlittle.clusterprocess.UnitSpec
import org.oliverlittle.clusterprocess.model.table.field._
import org.oliverlittle.clusterprocess.model.table._

class FieldOperationsTest extends UnitSpec {
    "A FieldOperation" should "concatenate strings" in {
        FieldOperations.Concat(V("1"), V("2")).resolve(TableResultHeader(Seq())).evaluate(Seq()) should be (Some(StringValue("12")))
    }

    it should "add integers" in {
        FieldOperations.AddInt(V(1), V(2)).resolve(TableResultHeader(Seq())).evaluate(Seq()) should be (Some(IntValue(3)))
    }
    
    it should "add doubles" in {
        FieldOperations.AddDouble(V(1.01d), V(1.01d)).resolve(TableResultHeader(Seq())).evaluate(Seq()) should be (Some(DoubleValue(2.02d)))
    }

    it should "calculate exponents" in {
        FieldOperations.Pow(V(10d), V(2d)).resolve(TableResultHeader(Seq())).evaluate(Seq()) should be (Some(DoubleValue(100d)))
    }

    it should "calculate substrings" in {
        FieldOperations.Substring(V("hello"), V(1), V(4)).resolve(TableResultHeader(Seq())).evaluate(Seq()) should be (Some(StringValue("ell")))
    }

    it should "calculate left substrings" in {
        FieldOperations.Left(V("hello"), V(2)).resolve(TableResultHeader(Seq())).evaluate(Seq()) should be (Some(StringValue("he")))
    }

    it should "calculate right substrings" in {
        FieldOperations.Right(V("hello"), V(2)).resolve(TableResultHeader(Seq())).evaluate(Seq()) should be (Some(StringValue("lo")))
    }

    it should "calculate polymorphic additions" in {
        FieldOperations.Add(V("a"), V("b")).resolve(TableResultHeader(Seq())).evaluate(Seq()) should be (Some(StringValue("ab")))
        FieldOperations.Add(V(1), V(2)).resolve(TableResultHeader(Seq())).evaluate(Seq()) should be (Some(IntValue(3)))
        FieldOperations.Add(V(1.01d), V(1.01d)).resolve(TableResultHeader(Seq())).evaluate(Seq()) should be (Some(DoubleValue(2.02d)))
    }
}