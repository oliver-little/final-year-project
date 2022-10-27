package org.oliverlittle.clusterprocess.model.fields

import java.time.{LocalDateTime, OffsetDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import scala.reflect.{ClassTag, classTag}
import scala.util.Try

import org.oliverlittle.clusterprocess.table_model._

enum DataType:
    case String, Long, Double, Float, LocalDateTime, OffsetDateTime, Boolean

// Defines highest level operations on FieldExpressions 
sealed abstract class FieldExpression:
    // Represents whether this field expression is well-typed (i.e.: we are at least able to attempt computation on it, as the base types all match)
    val isWellTyped : Boolean
    // Checks whether this FieldExpression is able to return the provided type parameter
    def doesReturnType(evalType : DataType) : Boolean

    // Convert this FieldExpression to a protobuf expression
    def toProtobuf : Expression

    // Evaluate this FieldExpression from the top down, allowing the functions to resolve their own types.
    def evaluateAny : Any 

    // Evaluate this FieldExpression with a specific type, this will throw an error if the expression cannot be computed with the given type.
    def evaluate(evalType : DataType): EvalType = {
        return this match {
            case value : V => value.getValueAsType[EvalType]
            case function : FunctionCall[_] if function.doesReturnType(evalType) => function.functionCalc.asInstanceOf[EvalType]
            case field : F => field.getFieldData[EvalType].asInstanceOf[EvalType]
            case _ => throw new IllegalArgumentException("Cannot evaluate to type " + tag.runtimeClass.getName)
        }
    }

// V takes a value of any type, as it doesn't care what it's actually passed until it's asked to evaluate it.
// Only Longs and Doubles are supported, Ints and Floats are automatically converted
final case class V(value : Any) extends FieldExpression {
    val isWellTyped = Try{toProtobuf}.isSuccess
    
    def doesReturnType[EvalType](using tag : ClassTag[EvalType]) : Boolean = Try{getValueAsType[EvalType]}.isSuccess

    def toProtobuf : Expression = {
        // Bit messy, this could do with cleaning up
        return Expression().withValue(
            value match {
                case s : String => Value().withString(s)
                case i : Int => Value().withInt(i.toLong)
                case i : Long => Value().withInt(i)
                case f : Double => Value().withDouble(f)
                case f : Float => Value().withDouble(f.toDouble)
                case d : LocalDateTime => Value().withDatetime(d.atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME))
                case d : OffsetDateTime => Value().withDatetime(d.format(DateTimeFormatter.ISO_DATE_TIME))
                case b : Boolean => Value().withBool(b)
                // This case should never happen
                case _ => throw new IllegalArgumentException("Type of " + value.toString + " is invalid: " + value.getClass.toString)
            }
        )
    }

    def evaluateAny : Any = value match {
        case v : Int => v.toLong
        case v : Float => v.toDouble
        case v => v
    }

    // Returns this value as the provided type parameter, if possible.
    def getValueAsType(evalType : DataType) : EvalType = {
        var clazz = tag.runtimeClass
        // This is pure Java code, seems to be the easiest way of doing the conversion with a generic type.
        if clazz.isInstance(value) then 
            return clazz.cast(value).asInstanceOf[EvalType]
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
            case v : Float => return v.toDouble
            case v : Double => return v
            case _ => throw new IllegalArgumentException("Cannot convert " + value.toString + " to Double.")
        }
    }
}

// Implicit definitions used for automatic conversion of literals to Vs in the model
implicit def stringToV(s : String) : V = new V(s)
implicit def intToV(i : Int) : V = new V(i.toLong)
implicit def longToV(l : Long) : V = new V(l)
implicit def floatToV(f : Float) : V = new V(f.toLong)
implicit def doubleToV(d : Double) : V = new V(d)
implicit def offsetDateTimeToV(l : OffsetDateTime) : V = new V(l)
implicit def boolToV(b : Boolean) : V = new V(b)

// F defines a field name, currently this does nothing
final case class F(fieldName : String) extends FieldExpression {
    val isWellTyped: Boolean = true

    def doesReturnType[EvalType](using tag : ClassTag[EvalType]): Boolean = true
    
    def toProtobuf : Expression = Expression().withValue(Value().withField(fieldName))
    def evaluateAny: Any = fieldName
    def getFieldData[EvalType](using evidence : EvalType =:= String = null) : String = 
        if evidence != null then 
            return fieldName
        else
            throw new IllegalArgumentException("Field Name does not support this type")
}


// Defines an abstract function call with an unknown number of parameters and a fixed return type
abstract class FunctionCall(functionName : String, returnType : DataType) extends FieldExpression:
    val isWellTyped: Boolean = checkArgReturnTypes
    def doesReturnType(evalType : DataType) : Boolean = evalType == returnType && checkArgReturnTypes
    // Abstract function: this should validate all arguments to this function call to ensure they return an acceptable type
    def checkArgReturnTypes: Boolean

    val arguments : Seq[FieldExpression]
    def toProtobuf : Expression = Expression().withFunction(Expression.FunctionCall(functionName=functionName, arguments=this.arguments.map(_.toProtobuf)))
    def evaluateAny: Any = functionCalc
    
    // Abstract function: this should perform the actual function call on the FieldExpression arguments
    def functionCalc : ReturnType

// Defines a function that takes one argument and returns a value
final case class UnaryFunction[ArgType, ReturnType](functionName : String, function : (argument : ArgType) => ReturnType, argument : FieldExpression)(using tag : ClassTag[ArgType]) extends FunctionCall[ReturnType](functionName):
    def checkArgReturnTypes : Boolean = argument.doesReturnType[ArgType]
    val arguments = Seq(argument)
    def functionCalc = function(argument.evaluate[ArgType])

// Defines a function that takes two arguments and returns a value
final case class BinaryFunction[LeftArgType, RightArgType, ReturnType](functionName : String, function : (left : LeftArgType, right : RightArgType) => ReturnType, left : FieldExpression, right : FieldExpression)(using lTag : ClassTag[LeftArgType]) (using rTag : ClassTag[RightArgType])  extends FunctionCall[ReturnType](functionName):
    def checkArgReturnTypes : Boolean = left.doesReturnType[LeftArgType] && right.doesReturnType[RightArgType]
    val arguments = Seq(left, right)
    def functionCalc = function(left.evaluate[LeftArgType], right.evaluate[RightArgType])

// Defines a function that takes three arguments and returns a value
final case class TernaryFunction[ArgOneType, ArgTwoType, ArgThreeType, ReturnType](functionName : String, function : (one : ArgOneType, two : ArgTwoType, three : ArgThreeType) => ReturnType, one : FieldExpression, two : FieldExpression, three : FieldExpression)(using oneTag : ClassTag[ArgOneType], twoTag : ClassTag[ArgTwoType], threeTag : ClassTag[ArgThreeType])  extends FunctionCall[ReturnType](functionName):
    def checkArgReturnTypes : Boolean = one.doesReturnType[ArgOneType] && two.doesReturnType[ArgTwoType] && three.doesReturnType[ArgThreeType]
    val arguments = Seq(one, two, three)
    def functionCalc = function(one.evaluate[ArgOneType], two.evaluate[ArgTwoType], three.evaluate[ArgThreeType])

// Polymorphic cast definitions (takes any argument, and returns the type or an error)
final case class ToString(argument: FieldExpression) extends FunctionCall[String]("ToString"):
    def checkArgReturnTypes : Boolean = argument.doesReturnType[Any]
    val arguments = Seq(argument)
    def functionCalc = argument.evaluateAny.toString

final case class ToInt(argument : FieldExpression) extends FunctionCall[Long]("ToInt"):
    def checkArgReturnTypes : Boolean = argument.doesReturnType[String] || argument.doesReturnType[Double] || argument.doesReturnType[Float] || argument.doesReturnType[Long] || argument.doesReturnType[Int]
    val arguments = Seq(argument)
    def functionCalc = argument.evaluateAny match {
        case v : String => v.toLong
        case v : Double => v.toLong
        case v : Float => v.toLong
        case v : Long => v
        case v : Int => v.toLong
        case v => throw new IllegalArgumentException("Cannot convert " + v.toString + " of type " + v.getClass + " to Long.")
    }

final case class ToDouble(argument : FieldExpression) extends FunctionCall[Double]("ToDouble"):
    def checkArgReturnTypes : Boolean = argument.doesReturnType[String]
    val arguments = Seq(argument)
    def functionCalc = argument.evaluateAny match {
        case v : String => v.toDouble
        case v : Long => v.toDouble
        case v : Int => v.toDouble
        case v : Double => v
        case v : Float => v.toDouble
        case v => throw new IllegalArgumentException("Cannot convert " + v.toString + " of type " + v.getClass + " to Long.")
    }