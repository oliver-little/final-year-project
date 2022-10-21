package model

import table_model._
import java.util.Date
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.math.pow

/*
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
*/

abstract class FieldExpression:
    def toProtobuf : Expression
    def evaluate[T] : T


// V takes a value of any type, as it doesn't care what it's actually passed until it's asked to evaluate it..
final case class V(value : Any) extends FieldExpression {
    def toProtobuf : Expression = {
        // Bit messy, this could do with cleaning up
        return Expression().withValue(
            value match {
                case s : String => Value().withString(s)
                case i : Int => Value().withInt(i)
                case i : Long => Value().withInt(i)
                case f : Double => Value().withDouble(f)
                case f : Float => Value().withDouble(f)
                case d : LocalDateTime => Value().withDatetime(d.format(DateTimeFormatter.ISO_INSTANT))
                case b : Boolean => Value().withBool(b)
                case null => Value().withIsnull(true)
                case _ => throw new IllegalArgumentException("Type of " + value.toString + " is invalid: " + value.getClass.toString)
            }
        )
    }

    def evaluate[T](implicit t : T): T = 
        t match {
            case t : String => value.toString.asInstanceOf[T]
            case _ => throw new IllegalArgumentException("Cannot resolve " + value.toString + " as " T.toString)
        }
}

// Implicit definitions used for automatic conversion of literals to Vs in the model
implicit def stringToV(s : String) : V = new V(s)
implicit def intToV(i : Int) : V = new V(i)
implicit def longToV(l : Long) : V = new V(l)
implicit def floatToV(f : Float) : V = new V(f)
implicit def doubleToV(d : Double) : V = new V(d)
implicit def localDateTimeToV(l : LocalDateTime) : V = new V(l)
implicit def boolToV(b : Boolean) : V = new V(b)

abstract class FunctionCall(functionName : String) extends FieldExpression:
    val arguments : Seq[FieldExpression]
    def toProtobuf : Expression = Expression().withFunction(Expression.FunctionCall(functionName=functionName, arguments=this.arguments.map(_.toProtobuf)))

// I imagine this will resolve to some type T by doing a lookup on the actual data?
// The actual type being used will be determined when the data is loaded
final case class F(fieldName : String) extends FieldExpression {
    def toProtobuf : Expression = Expression().withValue(Value().withField(fieldName))
}


final case class Add(left : FieldExpression, right : FieldExpression) extends FunctionCall("Add") {
    val arguments : Seq[FieldExpression] = Seq(left, right)
    def evaluate : T = left + right
}

final case class Pow(value : FieldExpression, exponent: FieldExpression) extends FunctionCall("Pow") {
    val arguments : Seq[FieldExpression] = Seq(left, right)
    def evaluate : Double = pow(value., exponent)
}
