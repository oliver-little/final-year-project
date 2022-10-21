package model

import table_model._
import java.util.Date
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.math.pow

object FieldExpression:
    def fromProtobuf(value : Value) : FieldExpression = {
        return (value match {
            case Value.Value.Field(f) => F[String](f)
            case Value.Value.String(s) => V[String](s)
            case Value.Value.Int(i) => V[Int](i)
            case Value.Value.Double(d) => V[Double](d)
            case Value.Value.Datetime(s) => V[LocalDateTime]e.parse(s, DateTimeFormatter.ISO_INSTANT))
            case Value.Value.Bool(b) => V[Boolean](b)
            case Value.Value.Isnull(_) => V[NoneType](null)
            case _ => throw new IllegalArgumentException("Unknown type ")
        }
    }

abstract class FieldExpression[T]:
    def toProtobuf : Expression
    def evaluate : T // Not sure if Any is correct here


final case class V[T](value : T) extends FieldExpression[T] {
    def toProtobuf : Expression = {
        // Bit messy, this could do with cleaning up
        return Expression().withValue(
            value match {
                case s : String => Value().withString(s)
                case i : Int => Value().withInt(i)
                case f : Double => Value().withDouble(f)
                case f : Float => Value().withDouble(f)
                case d : LocalDateTime => Value().withDatetime(d.format(DateTimeFormatter.ISO_INSTANT))
                case b : Boolean => Value().withBool(b)
                case null => Value().withIsnull(true)
                case _ => throw new IllegalArgumentException("Type of " + value.toString + " is invalid: " + value.getClass.toString)
            }
        )
    }

    def evaluate : T = value
}

abstract class FunctionCall[T](functionName : String) extends FieldExpression[T]:
    val arguments : Seq[FieldExpression[T]]
    def toProtobuf : Expression = Expression().withFunction(Expression.FunctionCall(functionName=functionName, arguments=this.arguments.map(_.toProtobuf)))

// I imagine this will resolve to some type T by doing a lookup on the actual data?
// The actual type being used will be determined when the data is loaded
final case class F[T](fieldName : String) extends FieldExpression[T] {
    def toProtobuf : Expression = Expression().withValue(Value().withField(fieldName))
}
/*
final case class Add[T](left : FieldExpression[T], right : FieldExpression[T]) extends FunctionCall[T]("Add") {
    val arguments : Seq[FieldExpression[T]] = Seq(left, right)
    def evaluate : T = left + right
}

final case class Pow[Double](value : FieldExpression[Double], exponent: FieldExpression[Double]) extends FunctionCall[Double]("Pow") {
    val arguments : Seq[FieldExpression[Double]] = Seq(left, right)
    def evaluate : Double = pow(value, exponent)
}

val value = V("t")
*/