package org.oliverlittle.clusterprocess.model.field.expressions

import java.time.{LocalDateTime, Instant}
import scala.reflect.{ClassTag, classTag}
import java.text.DecimalFormat


import org.oliverlittle.clusterprocess.table_model.{Expression, Value}
import org.oliverlittle.clusterprocess.model.field.expressions.FieldOperations.AddInt
import org.oliverlittle.clusterprocess.UnitSpec
import java.time.ZoneOffset
import org.oliverlittle.clusterprocess.model.table.field.TableField
import org.oliverlittle.clusterprocess.model.table.field.TableValue

class FieldExpressionSpec extends UnitSpec {
    "A FieldExpression" should "evaluate Values correctly" in {
        V(1).evaluate[Long](Map()) should be (1)
    }

    it should "evaluate FunctionCalls correctly" in {
        AddInt(V(1), V(2)).evaluate[Long](Map()) should be (3)
    }

    // TODO: Fields once implemented

    it should "evaluate nested FieldExpressions correctly" in {
        AddInt(V(1), AddInt(V(2), V(5))).evaluate[Long](Map()) should be (8)
    }

    it should "throw IllegalArgumentException if a type is invalid within the statement" in {
        assertThrows[IllegalArgumentException] {
            AddInt(V(1), V("a")).evaluate[Long](Map())
        }
    }

    it should "throw IllegalArgumentException if the requested type is invalid" in {
        assertThrows[IllegalArgumentException] {
            AddInt(V(1), V(1)).evaluate[String](Map())
        }
    }
}

class ValueSpec extends UnitSpec {
    // Instantiation
    "A Value" should "return the same value it is given" in {
        val value = V("a")
        assert(value.evaluateAny(Map()) == "a")
    }

    // Protobufs
    it should "convert Strings to Protobuf expressions" in {
        val literal = "a"
        val value = V(literal)
        val expression : Expression = value.protobuf
        
        inside(expression) { case Expression(expr, unknownFields) => 
            inside (expr) { case Expression.Expr.Value(value) => 
                inside (value) { case Value(v, unknownFields) => 
                    v.string.value should be (literal)
                }
            }
        }
    }

    it should "convert Longs to Protobuf expressions" in {
        val literal : Long = 1
        val value = V(literal)
        val expression : Expression = value.protobuf
        
        inside(expression) { case Expression(expr, unknownFields) => 
            inside (expr) { case Expression.Expr.Value(value) => 
                inside (value) { case Value(v, unknownFields) => 
                    v.int.value should be (literal)
                }
            }
        }
    }

    it should "convert Ints to Protobuf expressions" in {
        val literal : Int = 1
        val value = V(literal)
        val expression : Expression = value.protobuf
        
        inside(expression) { case Expression(expr, unknownFields) => 
            inside (expr) { case Expression.Expr.Value(value) => 
                inside (value) { case Value(v, unknownFields) => 
                    v.int.value should be (1L)
                }
            }
        }
    }

    it should "convert Doubles to Protobuf expressions" in {
        val literal : Double = 1.01
        val value = V(literal)
        val expression : Expression = value.protobuf
        
        inside(expression) { case Expression(expr, unknownFields) => 
            inside (expr) { case Expression.Expr.Value(value) => 
                inside (value) { case Value(v, unknownFields) => 
                    v.double.value should be (literal +- 0.01)
                }
            }
        }
    }

    it should "convert Floats to Protobuf expressions" in {
        val literal : Float = 1.01
        val value = V(literal)
        val expression : Expression = value.protobuf
        
        inside(expression) { case Expression(expr, unknownFields) => 
            inside (expr) { case Expression.Expr.Value(value) => 
                inside (value) { case Value(v, unknownFields) => 
                    v.double.value should be (1.01D +- 0.01)
                }
            }
        }
    }

    it should "convert Booleans to Protobuf expressions" in {
        val literal = true
        val value = V(literal)
        val expression : Expression = value.protobuf
        
        inside(expression) { case Expression(expr, unknownFields) => 
            inside (expr) { case Expression.Expr.Value(value) => 
                inside (value) { case Value(v, unknownFields) => 
                    v.bool.value should be (literal)
                }
            }
        }
    }

    it should "convert Instants to Protobuf strings" in {
        val literal = LocalDateTime.of(2000, 1, 1, 1, 0, 0).atOffset(ZoneOffset.UTC).toInstant
        val value = V(literal)
        val expression : Expression = value.protobuf
        
        inside(expression) { case Expression(expr, unknownFields) => 
            inside (expr) { case Expression.Expr.Value(value) => 
                inside (value) { case Value(v, unknownFields) => 
                    v.datetime.value should be ("2000-01-01T01:00:00Z")
                }
            }
        }
    }

    it should "throw IllegalArgumentException when converting invalid types to Protobuf expressions" in {
        Seq(Seq(1, 2, 3)) foreach {literal => 
            val value = V(literal)
            assertThrows[IllegalArgumentException] {
                value.protobuf
            }   
        }
    }

    // Well Typed check
    it should "be well-typed for valid types" in {
        Seq("a", 1L, 1, 1.01D, 1.01, true, LocalDateTime.of(2000, 1, 1, 1, 0, 0).atOffset(ZoneOffset.UTC).toInstant) foreach {literal =>
            val value = V(literal)
            
            value.isWellTyped(Map()) should be (true) withClue (", when literal is: " + literal.toString)
        }
    }

    it should "not be well-typed for invalid types" in {
        Seq(Seq(1, 2, 3)) foreach { literal =>
            val value = V(literal)
            
            value.isWellTyped(Map()) should be (false)
        }
    }

    // Conversions
    it should "automatically convert Ints to Longs" in {
        val int : Int = 1
        val value = V(int)
        value.evaluateAny(Map()) shouldBe a [Long]
        value.evaluateAny(Map()) should be (1)
        value.getValueAsType[Long] shouldBe a [Long]
        value.getLong shouldBe a [Long]
    }

    it should "automatically convert Floats to Doubles" in {
        val float : Float = 1.01
        val value = V(float)
        value.evaluateAny(Map()) shouldBe a [Double]
        value.evaluateAny(Map()) should be (float)
        value.getValueAsType[Double] shouldBe a [Double]
        value.getDouble shouldBe a [Double]
    }

    it should "throw IllegalArgumentException when a conversion fails" in {
        val value = V(Seq(1, 2, 3))

        assertThrows[IllegalArgumentException] {
            value.getValueAsType[String]
        }
    }

    it should "throw IllegalArgumentException when failing to convert to a Long" in {
        val value = V("1")

        assertThrows[IllegalArgumentException] {
            value.getLong
        }
    }

    it should "throw IllegalArgumentException when failing to convert to a Double" in {
        val value = V("1")

        assertThrows[IllegalArgumentException] {
            value.getDouble
        }
    }
}

class FunctionCallSpec extends UnitSpec {
    "A FunctionCall" should "be well-typed when its arguments are well-typed" in {
        val func = FunctionCallImpl()
        func.isWellTyped(Map()) should be (true)
    }

    it should "return true for doesReturnType with the correct type parameter" in {
        val func = FunctionCallImpl()
        func.doesReturnType[String](Map()) should be (true)
    }

    it should "return false for doesReturnType with invalid type parameters" in {
        val func = FunctionCallImpl()
        func.doesReturnType[Long](Map()) should be (false)
    }

    it should "convert to a protobuf expression based on the name and arguments" in {
        val func = FunctionCallImpl()
        val ret = func.protobuf
        inside(ret) { case Expression(expr, unknownFields) => 
            inside(expr) { case Expression.Expr.Function(value) => 
                inside(value) { case Expression.FunctionCall(functionName, arguments, unknownFields) =>
                    functionName should be ("testName")
                    arguments should have length 1

                    inside (arguments.apply(0)) { case Expression(expr, unknownFields) =>
                        inside(expr) { case Expression.Expr.Value(value) =>
                            inside(value) { case Value(value, unknownFields) => 
                                value.string.value should be ("a")
                            }
                        }
                    }
                }
            }
        }
    }

    it should "call the function correctly" in {
        val func = FunctionCallImpl()

        func.functionCalc(Map()) should be ("a")
    }

    it should "call the function when evaluateAny is called" in {
        val func = FunctionCallImpl()

        func.evaluateAny(Map()) should be ("a")
    }

    private class FunctionCallImpl extends FunctionCall[String]("testName"):
        def isWellTyped(fieldContext : Map[String, TableField]) = true
        val arguments = Seq(V("a"))
        def functionCalc(rowContext : Map[String, TableValue]) = "a"
}

class UnaryFunctionSpec extends UnitSpec {
    val func = (arg) => UnaryFunction[String, String]("testName", (a) => a, arg)

    "A UnaryFunction" should "check the type of its argument matches the type parameter" in {
        func(V("a")).isWellTyped(Map()) should be (true)
        func(V(1)).isWellTyped(Map()) should be (false)
    }

    it should "put its argument into a Sequence for adding to a protobuf" in {
        func(V("a")).arguments should have length 1
        func(V("a")).arguments.apply(0) should be (V("a"))
    }

    it should "evaluate the function according to its argument" in {
        func(V("a")).functionCalc(Map()) should be ("a")
    }
}

class BinaryFunctionSpec extends UnitSpec {
    val func = (l, r) => BinaryFunction[String, String, String]("testName", (a, b) => a + b, l, r)

    "A BinaryFunction" should "check the type of its argument matches the type parameter" in {
        func(V("a"), V("a")).isWellTyped(Map()) should be (true)
        func(V(1), V(1)).isWellTyped(Map()) should be (false)
    }

    it should "put its argument into a Sequence for adding to a protobuf" in {
        func(V("a"), V("b")).arguments should have length 2
        func(V("a"), V("b")).arguments.apply(0) should be (V("a"))
        func(V("a"), V("b")).arguments.apply(1) should be (V("b"))
    }

    it should "evaluate the function according to its argument" in {
        func(V("a"), V("b")).functionCalc(Map()) should be ("ab")
    }
}

class TernaryFunctionSpec extends UnitSpec {
    val func = (l, r, t) => TernaryFunction[String, String, String, String]("testName", (a, b, t) => a + b + t, l, r, t)

    "A TernaryFunction" should "check the type of its argument matches the type parameter" in {
        func(V("a"), V("a"), V("a")).isWellTyped(Map()) should be (true)
        func(V(1), V(1), V(1)).isWellTyped(Map()) should be (false)
    }

    it should "put its argument into a Sequence for adding to a protobuf" in {
        func(V("a"), V("b"), V("c")).arguments should have length 3
        func(V("a"), V("b"), V("c")).arguments.apply(0) should be (V("a"))
        func(V("a"), V("b"), V("c")).arguments.apply(1) should be (V("b"))
        func(V("a"), V("b"), V("c")).arguments.apply(2) should be (V("c"))
    }

    it should "evaluate the function according to its argument" in {
        func(V("a"), V("b"), V("c")).functionCalc(Map()) should be ("abc")
    }
}

class ToStringSpec extends UnitSpec {
    "A ToString Cast" should "convert Strings" in {
        ToString(V("a")).evaluateAny(Map()) should be ("a")
    }

    it should "convert Ints" in {
        ToString(V(1 : Int)).evaluateAny(Map()) should be ("1")
    }

    it should "convert Longs" in {
        ToString(V(1 : Long)).evaluateAny(Map()) should be ("1")
    }

    it should "convert Floats" in {
        ToString(V(1.01 : Float)).evaluateAny(Map()) shouldBe a [String]
    }

    it should "convert Doubles" in {
        ToString(V(1.01 : Double)).evaluateAny(Map()) shouldBe a [String]
    }

    it should "convert LocalDateTimes" in {
        ToString(V(LocalDateTime.of(2000, 1, 1, 1, 0, 0).atOffset(ZoneOffset.UTC).toInstant)).evaluateAny(Map()) should be (LocalDateTime.of(2000, 1, 1, 1, 0, 0).atOffset(ZoneOffset.UTC).toInstant.toString)
    }

    it should "convert OffsetDateTimes" in {
        ToString(V(LocalDateTime.of(2000, 1, 1, 1, 0, 0).atOffset(ZoneOffset.UTC).toInstant)).evaluateAny(Map()) should be (LocalDateTime.of(2000, 1, 1, 1, 0, 0).atOffset(ZoneOffset.UTC).toInstant.toString)
    }

    it should "convert Booleans" in {
        ToString(V(true)).evaluateAny(Map()) should be ("true")
    }
}

class DoubleToStringSpec extends UnitSpec {
    "A DoubleToString cast" should "convert Floats" in {
        DoubleToString(V(1.01 : Float), DecimalFormat("#.##")).evaluateAny(Map()) should be ("1.01")
    }

    "A DoubleToString cast" should "convert Doubles" in {
        DoubleToString(V(1.01 : Double), DecimalFormat("#.##")).evaluateAny(Map()) should be ("1.01")
    }
}

class ToIntSpec extends UnitSpec {
    "A ToInt Cast" should "convert Strings" in {
        ToInt(V("1")).evaluateAny(Map()) should be (1)
    }

    it should "convert Ints" in {
        ToInt(V(1 : Int)).evaluateAny(Map()) should be (1)
    }

    it should "convert Longs" in {
        ToInt(V(1 : Long)).evaluateAny(Map()) should be (1)
    }

    it should "convert Floats" in {
        ToInt(V(1.01 : Float)).evaluateAny(Map()) should be (1)
    }

    it should "convert Doubles" in {
        ToInt(V(1.01 : Double)).evaluateAny(Map()) should be (1)
    }

    it should "fail to convert Instants" in {
        assertThrows[IllegalArgumentException] {
            ToInt(V(LocalDateTime.of(2000, 1, 1, 1, 0, 0).atOffset(ZoneOffset.UTC).toInstant)).evaluateAny(Map())
        }
    }

    it should "fail to convert Booleans" in {
        assertThrows[IllegalArgumentException] {
            ToInt(V(true)).evaluateAny(Map())
        }
    }
}

class ToDoubleSpec extends UnitSpec {
    "A ToDouble Cast" should "convert Strings" in {
        ToDouble(V("1")).evaluateAny(Map()) should be (1)
    }

    it should "convert Ints" in {
        val result = ToDouble(V(1 : Int)).evaluateAny(Map())
        result should be (1)
        result shouldBe a [Double]
    }

    it should "convert Longs" in {
        val result = ToDouble(V(1 : Long)).evaluateAny(Map())
        result should be (1)
        result shouldBe a [Double]
    }

    it should "convert Floats" in {
        val result = ToDouble(V(1.01 : Float)).evaluate[Double](Map())
        result should be (1.01 +- 0.01)
        result shouldBe a [Double] 
    }

    it should "convert Doubles" in {
        ToDouble(V(1.01 : Double)).evaluate[Double](Map()) should be (1.01 +- 0.01)
    }

    it should "fail to convert LocalDateTimes" in {
        assertThrows[IllegalArgumentException] {
            ToDouble(V(LocalDateTime.of(2000, 1, 1, 1, 0, 0).atOffset(ZoneOffset.UTC).toInstant)).evaluateAny(Map())
        }
    }

    it should "fail to convert OffsetDateTimes" in {
        assertThrows[IllegalArgumentException] {
            ToDouble(V(LocalDateTime.of(2000, 1, 1, 1, 0, 0).atOffset(ZoneOffset.UTC).toInstant)).evaluateAny(Map())
        }
    }

    it should "fail to convert Booleans" in {
        assertThrows[IllegalArgumentException] {
            ToDouble(V(true)).evaluateAny(Map())
        }
    }
}