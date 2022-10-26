package org.oliverlittle.clusterprocess.model.fields

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{Inside, OptionValues, AppendedClues}
import org.scalatest.matchers._
import java.time.OffsetDateTime


import org.oliverlittle.clusterprocess.model.fields.V
import org.oliverlittle.clusterprocess.table_model.{Expression, Value}
import java.time.LocalDateTime
import java.time.ZoneOffset

class ValueSpec extends AnyFlatSpec with Inside with OptionValues with AppendedClues with should.Matchers {
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