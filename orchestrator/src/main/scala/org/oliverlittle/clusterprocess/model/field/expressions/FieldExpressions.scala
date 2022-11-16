package org.oliverlittle.clusterprocess.model.field.expressions

import java.time.Instant
import java.time.format.DateTimeFormatter
import scala.reflect.{ClassTag, classTag}
import scala.util.Try
import java.text.DecimalFormat
import java.time.OffsetDateTime

import org.oliverlittle.clusterprocess.table_model._
import org.oliverlittle.clusterprocess.model.table.field._

object FieldExpression:
    def fromProtobuf(expr : Expression) : FieldExpression = expr match {
        case Expression(Expression.Expr.Value(Value(Value.Value.Field(f), _)), _) => F(f)
        case Expression(Expression.Expr.Value(value), unknownFields) => V.fromProtobuf(value)
        case Expression(Expression.Expr.Function(functionCall), _) => FunctionCall.fromProtobuf(functionCall).asInstanceOf[FieldExpression]
        case Expression(Expression.Expr.Empty, unknownFields) => throw new IllegalArgumentException("Empty expression found.")
    }

// Defines highest level operations on FieldExpressions 
sealed abstract class FieldExpression:
    // Represents whether this field expression is well-typed (i.e.: we are at least able to attempt computation on it, as the base types all match)
    def isWellTyped(fieldContext : Map[String, TableField]) : Boolean
    // Checks whether this FieldExpression is able to return the provided type parameter
    def doesReturnType[EvalType](fieldContext : Map[String, TableField])(using tag : ClassTag[EvalType]) : Boolean

    // Convert this FieldExpression to a protobuf expression
    lazy val protobuf : Expression

    // Evaluate this FieldExpression from the top down, allowing the functions to resolve their own types.
    def evaluateAny(rowContext : Map[String, TableValue]) : Any 

    // Evaluate this FieldExpression with a specific type, this will throw an error if the expression cannot be computed with the given type.
    def evaluate[EvalType](rowContext : Map[String, TableValue])(using tag : ClassTag[EvalType]): EvalType = {
        if !isWellTyped(rowContext) then throw new IllegalArgumentException("FieldExpression is not well-typed, cannot compute.")
        
        return this match {
            case value : V => value.getValueAsType[EvalType]
            case function : FunctionCall[_] if function.doesReturnType[EvalType](rowContext) => function.functionCalc.asInstanceOf[EvalType]
            case _ => throw new IllegalArgumentException("Cannot evaluate to type " + tag.runtimeClass.getName)
        }
    }

    def as(name : String) : NamedFieldExpression = NamedFieldExpression(name, this)


final case class NamedFieldExpression(name : String, expr : FieldExpression):
    /**
      * Evaluates the stored FieldExpression into a named and typed TableValue
      *
      * @param rowContext The row being evaluated
      * @return A TableValue representing the evaluated NamedFieldExpression
      */
    def evaluateAny(rowContext : Map[String, TableValue]) : TableValue = expr.evaluateAny(rowContext) match {
        case s : String => StringValue(name, s)
        case i : Long => IntValue(name, i)
        case f : Double => DoubleValue(name, f)
        case d : Instant => DateTimeValue(name, d)
        case b : Boolean => BoolValue(name, b)
        // This case should never happen
        case v => throw new IllegalArgumentException("Type of " + v.toString + " is invalid: " + v.getClass.toString)
    }

object V:
    // This needs refactoring - there must be a way of doing this using for-expressions
    def fromProtobuf(v : Value) : V = V(v.value.string.getOrElse(
        v.value.int.getOrElse(
            v.value.double.getOrElse(
                v.value.bool.getOrElse(
                    OffsetDateTime.parse(v.value.datetime.getOrElse(throw new IllegalArgumentException("Could not unpack Value, unknown type:" + v.toString)), DateTimeFormatter.ISO_INSTANT)
                )
            )
        )
    ))

// V takes a value of any type, as it doesn't care what it's actually passed until it's asked to evaluate it.
// Only Longs and Doubles are supported, Ints and Floats are automatically converted
final case class V(inputValue : Any) extends FieldExpression:
    // Preprocess inputValue to convert ints and floats to longs and doubles
    val value = inputValue match {
        case v : Int => v.toLong
        case v : Float => v.toDouble
        case v => v
    }

    def isWellTyped(fieldContext : Map[String, TableField]) = Try{protobuf}.isSuccess
    
    def doesReturnType[EvalType](fieldContext : Map[String, TableField])(using tag : ClassTag[EvalType]) : Boolean = Try{getValueAsType[EvalType]}.isSuccess

    lazy val protobuf : Expression = Expression().withValue(
        value match {
            case s : String => Value().withString(s)
            case i : Int => Value().withInt(i.toLong)
            case i : Long => Value().withInt(i)
            case f : Double => Value().withDouble(f)
            case f : Float => Value().withDouble(f.toDouble)
            case d : Instant => Value().withDatetime(DateTimeFormatter.ISO_INSTANT.format(d))
            case b : Boolean => Value().withBool(b)
            // This case should never happen
            case _ => throw new IllegalArgumentException("Type of " + value.toString + " is invalid: " + value.getClass.toString)
        }
    )

    def evaluateAny(rowContext : Map[String, TableValue]): Any = value

    // Returns this value as the provided type parameter, if possible.
    def getValueAsType[EvalType](using tag : ClassTag[EvalType]): EvalType = {
        var clazz = tag.runtimeClass
        // This is pure Java code, seems to be the easiest way of doing the conversion with a generic type.
        if clazz.isInstance(value) then 
            return value.asInstanceOf[EvalType]
        // These extra cases handle Int/Long and Float/Double where we only support one of the two types
        else if clazz == classOf[Long] then 
            return getLong.asInstanceOf[EvalType]
        else if clazz == classOf[Double] then 
            return getDouble.asInstanceOf[EvalType]
        else return throw new IllegalArgumentException("Cannot convert " + value.toString + " to " + clazz.toString + ".")
    }
    
    // Helper function to convert Int to Long
    def getLong : Long = {
        return value match {
            case v : Int => return v.toLong
            case v : Long => return v
            case _ => throw new IllegalArgumentException("Cannot convert " + value.toString + " to Long.")
        }
    }

    // Helper function to convert Float to Double
    def getDouble : Double = {
        return value match {
            case v : Float => return v.toFloat
            case v : Double => return v
            case _ => throw new IllegalArgumentException("Cannot convert " + value.toString + " to Double.")
        }
    }

    // Standard compare function, includes type safety and will throw an error if the other value cannot be compared
    def compare(that : V) : Int = (value, that.value) match {
        case (x : String, y : String) => x.compare(y)
        case (x : Long, y : Long)=> x.compare(y)
        case (x : Double, y : Double) => x.compare(y)
        case (x : Instant, y : Instant) => x.compareTo(y)
        case (x : Boolean, y : Boolean) => x.compare(y)
        // This case should never happen
        case _ => throw new IllegalArgumentException("Provided value " + that.value.toString + " (type: " + that.value.getClass.toString + ") cannot be compared with this value " + value.toString + " (type: " + value.getClass.toString + ").")
    }


object F:
    implicit def toNamed(f : F) : NamedFieldExpression = NamedFieldExpression(f.fieldName, f)

final case class F(fieldName : String) extends FieldExpression:
    def isWellTyped(fieldContext : Map[String, TableField]): Boolean = true

    def doesReturnType[EvalType](fieldContext : Map[String, TableField])(using tag : ClassTag[EvalType]): Boolean = tag.runtimeClass.isInstance(fieldContext.getOrElse(fieldName, throw new IllegalArgumentException("Field name not found: " + fieldName)).testValue)
    
    lazy val protobuf : Expression = Expression().withValue(Value().withField(fieldName))
    def evaluateAny(rowContext : Map[String, TableValue]): Any = rowContext.getOrElse(fieldName, throw new IllegalArgumentException("Field name not found: " + fieldName)).value


object FunctionCall:
    def fromProtobuf(f : Expression.FunctionCall) : Any = FieldOperations.getClass.getDeclaredMethod(f.functionName, FieldOperations.getClass).invoke(FieldOperations, f.arguments.map(FieldExpression.fromProtobuf(_)))

// Defines an abstract function call with an unknown number of parameters and a fixed return type
// The implicit (using) ClassTag[ReturnType] prevents the type erasure of the generic type ReturnType, which allows it 
abstract class FunctionCall[ReturnType](functionName : String)(using tag : ClassTag[ReturnType])  extends FieldExpression:
    val returnClassTag : ClassTag[ReturnType] = tag
    def doesReturnType[EvalType](fieldContext : Map[String, TableField])(using evalTag : ClassTag[EvalType]) : Boolean = evalTag.equals(tag)

    val arguments : Seq[FieldExpression]
    lazy val protobuf : Expression = Expression().withFunction(Expression.FunctionCall(functionName=functionName, arguments=this.arguments.map(_.protobuf)))
    def evaluateAny(rowContext : Map[String, TableValue]): Any = functionCalc
    
    // Abstract function: this should perform the actual function call on the FieldExpression arguments
    def functionCalc(rowContext : Map[String, TableValue]) : ReturnType

// Defines a function that takes one argument and returns a value
final case class UnaryFunction[ArgType, ReturnType](functionName : String, function : (argument : ArgType) => ReturnType, argument : FieldExpression)(using tag : ClassTag[ArgType])(using retTag : ClassTag[ReturnType]) extends FunctionCall[ReturnType](functionName):
    def isWellTyped(fieldContext : Map[String, TableField]) : Boolean = argument.doesReturnType[ArgType](fieldContext)
    val arguments = Seq(argument)
    def functionCalc(rowContext : Map[String, TableValue]) = function(argument.evaluate[ArgType](rowContext))

// Defines a function that takes two arguments and returns a value
final case class BinaryFunction[LeftArgType, RightArgType, ReturnType](functionName : String, function : (left : LeftArgType, right : RightArgType) => ReturnType, left : FieldExpression, right : FieldExpression)(using lTag : ClassTag[LeftArgType]) (using rTag : ClassTag[RightArgType])(using retTag : ClassTag[ReturnType]) extends FunctionCall[ReturnType](functionName):
    def isWellTyped(fieldContext : Map[String, TableField]) : Boolean = left.doesReturnType[LeftArgType](fieldContext) && right.doesReturnType[RightArgType](fieldContext)
    val arguments = Seq(left, right)
    def functionCalc(rowContext : Map[String, TableValue]) = function(left.evaluate[LeftArgType](rowContext), right.evaluate[RightArgType](rowContext))

// Defines a function that takes three arguments and returns a value
final case class TernaryFunction[ArgOneType, ArgTwoType, ArgThreeType, ReturnType](functionName : String, function : (one : ArgOneType, two : ArgTwoType, three : ArgThreeType) => ReturnType, one : FieldExpression, two : FieldExpression, three : FieldExpression)(using oneTag : ClassTag[ArgOneType], twoTag : ClassTag[ArgTwoType], threeTag : ClassTag[ArgThreeType])(using retTag : ClassTag[ReturnType]) extends FunctionCall[ReturnType](functionName):
    def isWellTyped(fieldContext : Map[String, TableField]) : Boolean = one.doesReturnType[ArgOneType](fieldContext) && two.doesReturnType[ArgTwoType](fieldContext) && three.doesReturnType[ArgThreeType](fieldContext)
    val arguments = Seq(one, two, three)
    def functionCalc(rowContext : Map[String, TableValue]) = function(one.evaluate[ArgOneType](rowContext), two.evaluate[ArgTwoType](rowContext), three.evaluate[ArgThreeType](rowContext))

// Polymorphic cast definitions (takes any argument, and returns the type or an error)
final case class ToString(argument: FieldExpression) extends FunctionCall[String]("ToString"):
    def isWellTyped(fieldContext : Map[String, TableField]) : Boolean = argument.doesReturnType[Any](fieldContext)
    val arguments = Seq(argument)
    def functionCalc(rowContext : Map[String, TableValue]) = argument.evaluateAny(rowContext).toString

// ToString implementation specifically for Doubles, to enable specified precision
final case class DoubleToString(argument: FieldExpression, formatter : DecimalFormat) extends FunctionCall[String]("DoubleToString"):
    def isWellTyped(context : Map[String, TableField]) : Boolean = argument.doesReturnType[Double](context)
    val arguments = Seq(argument)
    def functionCalc(rowContext : Map[String, TableValue]) = formatter.format(argument.evaluate[Double])

final case class ToInt(argument : FieldExpression) extends FunctionCall[Long]("ToInt"):
    def isWellTyped(fieldContext : Map[String, TableField]) : Boolean = argument.doesReturnType[String](fieldContext) || argument.doesReturnType[Double](fieldContext) || argument.doesReturnType[Float](fieldContext) || argument.doesReturnType[Long](fieldContext) || argument.doesReturnType[Int](fieldContext)
    val arguments = Seq(argument)
    def functionCalc(rowContext : Map[String, TableValue]) = argument.evaluateAny(rowContext) match {
        case v : String => v.toLong
        case v : Double => v.toLong
        case v : Float => v.toLong
        case v : Long => v
        case v : Int => v.toLong
        case v => throw new IllegalArgumentException("Cannot convert " + v.toString + " of type " + v.getClass + " to Long.")
    }

final case class ToDouble(argument : FieldExpression) extends FunctionCall[Double]("ToDouble"):
    def isWellTyped(fieldContext : Map[String, TableField]) : Boolean = argument.doesReturnType[String](fieldContext) || argument.doesReturnType[Double](fieldContext) || argument.doesReturnType[Float](fieldContext) || argument.doesReturnType[Long](fieldContext) || argument.doesReturnType[Int](fieldContext)
    val arguments = Seq(argument)
    def functionCalc(rowContext : Map[String, TableValue]) = argument.evaluateAny(rowContext) match {
        case v : String => v.toDouble
        case v : Long => v.toDouble
        case v : Int => v.toDouble
        case v : Double => v
        case v : Float => v.toDouble
        case v => throw new IllegalArgumentException("Cannot convert " + v.toString + " of type " + v.getClass + " to Long.")
    }