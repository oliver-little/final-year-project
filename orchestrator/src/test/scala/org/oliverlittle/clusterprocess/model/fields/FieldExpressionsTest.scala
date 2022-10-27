package org.oliverlittle.clusterprocess.model.fields

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{Inside, OptionValues, AppendedClues}
import org.scalatest.matchers._
import java.time.OffsetDateTime


import org.oliverlittle.clusterprocess.model.fields.V
import org.oliverlittle.clusterprocess.table_model.{Expression, Value}
import java.time.LocalDateTime
import java.time.ZoneOffset

abstract class UnitSpec extends AnyFlatSpec with Inside with OptionValues with AppendedClues with should.Matchers

class ValueSpec extends UnitSpec {
    // Instantiation
    "A Value" should "return the same value it is given" in {
        val value = V("a")
        assert(value.evaluateAny == "a")
    }

    // Protobufs
    it should "convert Strings to Protobuf expressions" in {
        val literal = "a"
        val value = V(literal)
        val expression : Expression = value.toProtobuf
        
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
        val expression : Expression = value.toProtobuf
        
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
        val expression : Expression = value.toProtobuf
        
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
        val expression : Expression = value.toProtobuf
        
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
        val expression : Expression = value.toProtobuf
        
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
        val expression : Expression = value.toProtobuf
        
        inside(expression) { case Expression(expr, unknownFields) => 
            inside (expr) { case Expression.Expr.Value(value) => 
                inside (value) { case Value(v, unknownFields) => 
                    v.bool.value should be (literal)
                }
            }
        }
    }

    it should "convert LocalDateTimes to Protobuf strings" in {
        val literal = LocalDateTime.of(2000, 1, 1, 1, 0, 0)
        val value = V(literal)
        val expression : Expression = value.toProtobuf
        
        inside(expression) { case Expression(expr, unknownFields) => 
            inside (expr) { case Expression.Expr.Value(value) => 
                inside (value) { case Value(v, unknownFields) => 
                    v.datetime.value should be ("2000-01-01T01:00:00Z")
                }
            }
        }
    }

    it should "convert OffsetDateTimes to Protobuf strings" in {
        val literal = LocalDateTime.of(2000, 1, 1, 1, 0, 0).atOffset(ZoneOffset.UTC)
        val value = V(literal)
        val expression : Expression = value.toProtobuf
        
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
                value.toProtobuf
            }   
        }
    }

    // Well Typed check
    it should "be well-typed for valid types" in {
        Seq("a", 1L, 1, 1.01D, 1.01, true, LocalDateTime.of(2000, 1, 1, 1, 0, 0), LocalDateTime.of(2000, 1, 1, 1, 0, 0).atOffset(ZoneOffset.ofHours(4))) foreach {literal =>
            val value = V(literal)
            
            value.isWellTyped should be (true) withClue (", when literal is: " + literal.toString)
        }
    }

    it should "not be well-typed for invalid types" in {
        Seq(Seq(1, 2, 3)) foreach { literal =>
            val value = V(literal)
            
            value.isWellTyped should be (false)
        }
    }

    // Conversions
    it should "automatically convert Ints to Longs" in {
        val int : Int = 1
        val value = V(int)
        value.evaluateAny shouldBe a [Long]
        value.evaluateAny should be (1)
        value.getValueAsType[Long] shouldBe a [Long]
        value.getLong shouldBe a [Long]
    }

    it should "automatically convert Floats to Doubles" in {
        val float : Float = 1.01
        val value = V(float)
        value.evaluateAny shouldBe a [Double]
        value.evaluateAny should be (float)
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
        func.isWellTyped should be (true)
    }

    it should "calculate doesReturnType correctly based on the provided type" in {
        val func = FunctionCallImpl()
        func.doesReturnType[String] should be (true)
        func.doesReturnType[Long] should be (false)
    }

    it should "check eval type matches the defined return type" in {
        val func = FunctionCallImpl()
        func.checkEvalTypeMatchesReturnType[String] should be (true)
        func.checkEvalTypeMatchesReturnType[Long] should be (false)
    }

    it should "convert to a protobuf expression based on the name and arguments" in {
        val func = FunctionCallImpl()
        val ret = func.toProtobuf
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

    it should "call the function if the type is correct" in {
        val func = FunctionCallImpl()

        func.callFunction[String] should be ("a")
    }

    it should "throw IllegalArgumentException if the type is incorrect" in {
        val func = FunctionCallImpl()

        assertThrows[IllegalArgumentException] {
            func.callFunction[Long]
        }
    }

    it should "throw IllegalArgumentException if the argument types are incorrect" in {
        val func = FailFunctionCallImpl()
        assertThrows[IllegalArgumentException] {
            func.callFunction[String]
        }
    }

    it should "call the function when evaluateAny is called" in {
        val func = FunctionCallImpl()

        func.evaluateAny should be ("a")
    }

    private class FunctionCallImpl extends FunctionCall[String]("testName"):
        def checkArgReturnTypes = true
        val arguments = Seq(V("a"))
        def functionCalc = "a"

    private class FailFunctionCallImpl extends FunctionCall[String]("testName"):
        def checkArgReturnTypes = false
        val arguments = Seq(V("a"))
        def functionCalc = "a"
}

class UnaryFunctionSpec extends UnitSpec {
    val func = (arg) => UnaryFunction[String, String]("testName", (a) => a, arg)

    "A UnaryFunction" should "check the type of its argument matches the type parameter" in {
        func(V("a")).checkArgReturnTypes should be (true)
        func(V(1)).checkArgReturnTypes should be (false)
    }

    it should "put its argument into a Sequence for adding to a protobuf" in {
        func(V("a")).arguments should have length 1
        func(V("a")).arguments.apply(0) should be (V("a"))
    }

    it should "evaluate the function according to its argument" in {
        func(V("a")).callFunction[String] should be ("a")
    }
}

class BinaryFunctionSpec extends UnitSpec {
    val func = (l, r) => BinaryFunction[String, String, String]("testName", (a, b) => a + b, l, r)

    "A BinaryFunction" should "check the type of its argument matches the type parameter" in {
        func(V("a"), V("a")).checkArgReturnTypes should be (true)
        func(V(1), V(1)).checkArgReturnTypes should be (false)
    }

    it should "put its argument into a Sequence for adding to a protobuf" in {
        func(V("a"), V("b")).arguments should have length 2
        func(V("a"), V("b")).arguments.apply(0) should be (V("a"))
        func(V("a"), V("b")).arguments.apply(1) should be (V("b"))
    }

    it should "evaluate the function according to its argument" in {
        func(V("a"), V("b")).callFunction[String] should be ("ab")
    }
}

class TernaryFunctionSpec extends UnitSpec {
    val func = (l, r, t) => TernaryFunction[String, String, String, String]("testName", (a, b, t) => a + b + t, l, r, t)

    "A TernaryFunction" should "check the type of its argument matches the type parameter" in {
        func(V("a"), V("a"), V("a")).checkArgReturnTypes should be (true)
        func(V(1), V(1), V(1)).checkArgReturnTypes should be (false)
    }

    it should "put its argument into a Sequence for adding to a protobuf" in {
        func(V("a"), V("b"), V("c")).arguments should have length 3
        func(V("a"), V("b"), V("c")).arguments.apply(0) should be (V("a"))
        func(V("a"), V("b"), V("c")).arguments.apply(1) should be (V("b"))
        func(V("a"), V("b"), V("c")).arguments.apply(2) should be (V("c"))
    }

    it should "evaluate the function according to its argument" in {
        func(V("a"), V("b"), V("c")).callFunction[String] should be ("abc")
    }
}