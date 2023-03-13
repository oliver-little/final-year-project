package org.oliverlittle.clusterprocess.model.field.expressions

import org.oliverlittle.clusterprocess.UnitSpec
import org.oliverlittle.clusterprocess.model.table.field._
import org.oliverlittle.clusterprocess.model.table._

import java.text.DecimalFormat
import java.time.{LocalDateTime, Instant}
import java.time.ZoneOffset

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

    it should "divide doubles" in {
        FieldOperations.Div(V(6d), V(2d)).resolve(TableResultHeader(Seq())).evaluate(Seq()) should be (Some(DoubleValue(3d)))
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

    it should "calculate polymorphic multiplications" in {
        FieldOperations.Mul(V(2), V(2)).resolve(TableResultHeader(Seq())).evaluate(Seq()) should be (Some(IntValue(4)))
        FieldOperations.Mul(V(2.5d), V(3d)).resolve(TableResultHeader(Seq())).evaluate(Seq()) should be (Some(DoubleValue(7.5d)))
    }

    it should "calculate polymorphic subtractions" in {
        FieldOperations.Sub(V(2), V(2)).resolve(TableResultHeader(Seq())).evaluate(Seq()) should be (Some(IntValue(0)))
        FieldOperations.Sub(V(2.5d), V(3d)).resolve(TableResultHeader(Seq())).evaluate(Seq()) should be (Some(DoubleValue(-0.5d)))
    }

    it should "calculate polymorphic modulo" in {
        FieldOperations.Mod(V(4), V(3)).resolve(TableResultHeader(Seq())).evaluate(Seq()) should be (Some(IntValue(1)))
        FieldOperations.Mod(V(4.5d), V(3.5d)).resolve(TableResultHeader(Seq())).evaluate(Seq()) should be (Some(DoubleValue(1d)))
        FieldOperations.Mod(V(4), V(2.5d)).resolve(TableResultHeader(Seq())).evaluate(Seq()) should be (Some(DoubleValue(1.5d)))
        FieldOperations.Mod(V(4.5d), V(3)).resolve(TableResultHeader(Seq())).evaluate(Seq()) should be (Some(DoubleValue(1.5d)))
    }
}


class CastToStringSpec extends UnitSpec {
    "A CastToString" should "convert Strings" in {
        val result = CastToString(V("a")).resolve(TableResultHeader(Seq())).evaluate(Seq()).get.value should be ("a")
    }

    it should "convert Ints" in {
        CastToString(V(1 : Int)).resolve(TableResultHeader(Seq())).evaluate(Seq()).get.value should be ("1")
    }

    it should "convert Longs" in {
        CastToString(V(1 : Long)).resolve(TableResultHeader(Seq())).evaluate(Seq()).get.value should be ("1")
    }

    it should "convert Floats" in {
        CastToString(V(1.01 : Float)).resolve(TableResultHeader(Seq())).evaluate(Seq()).get.value shouldBe a [String]
    }

    it should "convert Doubles" in {
        CastToString(V(1.01 : Double)).resolve(TableResultHeader(Seq())).evaluate(Seq()).get.value shouldBe a [String]
    }

    it should "convert Instants" in {
        CastToString(V(LocalDateTime.of(2000, 1, 1, 1, 0, 0).atOffset(ZoneOffset.UTC).toInstant)).resolve(TableResultHeader(Seq())).evaluate(Seq()).get.value should be (LocalDateTime.of(2000, 1, 1, 1, 0, 0).atOffset(ZoneOffset.UTC).toInstant.toString)
    }

    it should "convert Booleans" in {
        CastToString(V(true)).resolve(TableResultHeader(Seq())).evaluate(Seq()).get.value should be ("true")
    }
}

class DoubleToStringSpec extends UnitSpec {
    "A DoubleToString cast" should "convert Floats" in {
        DoubleToString(V(1.01 : Float), DecimalFormat("#.##")).resolve(TableResultHeader(Seq())).evaluate(Seq()).get.value should be ("1.01")
    }

    "A DoubleToString cast" should "convert Doubles" in {
        DoubleToString(V(1.01 : Double), DecimalFormat("#.##")).resolve(TableResultHeader(Seq())).evaluate(Seq()).get.value should be ("1.01")
    }
}

class CastToIntSpec extends UnitSpec {
    "A CastToInt" should "convert Strings" in {
        CastToInt(V("1")).resolve(TableResultHeader(Seq())).evaluate(Seq()).get.value should be (1)
    }

    it should "convert Ints" in {
        CastToInt(V(1 : Int)).resolve(TableResultHeader(Seq())).evaluate(Seq()).get.value should be (1)
    }

    it should "convert Longs" in {
        CastToInt(V(1 : Long)).resolve(TableResultHeader(Seq())).evaluate(Seq()).get.value should be (1)
    }

    it should "convert Floats" in {
        CastToInt(V(1.01 : Float)).resolve(TableResultHeader(Seq())).evaluate(Seq()).get.value should be (1)
    }

    it should "convert Doubles" in {
        CastToInt(V(1.01 : Double)).resolve(TableResultHeader(Seq())).evaluate(Seq()).get.value should be (1)
    }

    it should "fail to convert Instants" in {
        assertThrows[IllegalArgumentException] {
            CastToInt(V(LocalDateTime.of(2000, 1, 1, 1, 0, 0).atOffset(ZoneOffset.UTC).toInstant)).resolve(TableResultHeader(Seq())).evaluate(Seq())
        }
    }

    it should "fail to convert Booleans" in {
        assertThrows[IllegalArgumentException] {
            CastToInt(V(true)).resolve(TableResultHeader(Seq())).evaluate(Seq())
        }
    }
}

class CastToDoubleSpec extends UnitSpec {
    "A CastToDouble" should "convert Strings" in {
        CastToDouble(V("1")).resolve(TableResultHeader(Seq())).evaluate(Seq()).get.value should be (1)
    }

    it should "convert Ints" in {
        val result = CastToDouble(V(1 : Int)).resolve(TableResultHeader(Seq())).evaluate(Seq()).get.asInstanceOf[DoubleValue]
        result.value should be (1)
        result.value shouldBe a [Double]
    }

    it should "convert Longs" in {
        val result = CastToDouble(V(1 : Long)).resolve(TableResultHeader(Seq())).evaluate(Seq()).get.asInstanceOf[DoubleValue]
        result.value should be (1)
        result.value shouldBe a [Double]
    }

    it should "convert Floats" in {
        val result = CastToDouble(V(1.01 : Float)).resolve(TableResultHeader(Seq())).evaluate(Seq()).get.asInstanceOf[DoubleValue]
        result.value should be (1.01 +- 0.01)
        result.value shouldBe a [Double] 
    }

    it should "convert Doubles" in {
        val result = CastToDouble(V(1.01 : Double)).resolve(TableResultHeader(Seq())).evaluate(Seq()).get.asInstanceOf[DoubleValue]
        result.value should be (1.01 +- 0.01)
        result.value shouldBe a [Double]
    }

    it should "fail to convert LocalDateTimes" in {
        assertThrows[IllegalArgumentException] {
            CastToDouble(V(LocalDateTime.of(2000, 1, 1, 1, 0, 0).atOffset(ZoneOffset.UTC).toInstant)).resolve(TableResultHeader(Seq())).evaluate(Seq())
        }
    }

    it should "fail to convert OffsetDateTimes" in {
        assertThrows[IllegalArgumentException] {
            CastToDouble(V(LocalDateTime.of(2000, 1, 1, 1, 0, 0).atOffset(ZoneOffset.UTC).toInstant)).resolve(TableResultHeader(Seq())).evaluate(Seq())
        }
    }

    it should "fail to convert Booleans" in {
        assertThrows[IllegalArgumentException] {
            CastToDouble(V(true)).resolve(TableResultHeader(Seq())).evaluate(Seq())
        }
    }
}