package model.fields

import table_model._
import java.util.Date
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.reflect.{ClassTag, classTag}
import scala.util.Try

sealed abstract class FieldExpression:
    val isWellTyped : Boolean
    def doesReturnType[EvalType](using tag : ClassTag[EvalType]) : Boolean

    // Convert this FieldExpression to a protobuf expression
    def toProtobuf : Expression

    // Evaluate this FieldExpression from the top down, allowing the functions to resolve their own types.
    def topLevelEvaluate : Any 
    // Evaluate this FieldExpression with a specific type, this will throw an error if the expression cannot be computed with the given type.
    def evaluate[EvalType](using tag : ClassTag[EvalType]) : EvalType = {
        return this match {
            case value : V => value.getValueAsType[EvalType]
            case function : FunctionCall[_] => function.callFunction[EvalType].asInstanceOf[EvalType]
            case field : F => field.getFieldData[EvalType].asInstanceOf[EvalType]
        }
    }

// V takes a value of any type, as it doesn'EvalType care what it's actually passed until it's asked to evaluate it.
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
                case d : LocalDateTime => Value().withDatetime(d.format(DateTimeFormatter.ISO_INSTANT))
                case b : Boolean => Value().withBool(b)
                // This case should never happen
                case _ => throw new IllegalArgumentException("Type of " + value.toString + " is invalid: " + value.getClass.toString)
            }
        )
    }

    def topLevelEvaluate : Any = value

    def getValueAsType[EvalType](using tag : ClassTag[EvalType]) : EvalType = {
        var clazz = tag.runtimeClass
        // This is pure Java code, seems to be the easiest way of doing the conversion with a generic type.
        if clazz.isInstance(value) then 
            return clazz.cast(value).asInstanceOf[EvalType]
        // These extra cases handle Int/Long and Float/Double where we only support one of the two types
        else if clazz == classOf[Long] then 
            return getLong.asInstanceOf[EvalType]
        else if clazz == classOf[Double] then 
            return getLong.asInstanceOf[EvalType]
        else return throw new IllegalArgumentException("Cannot convert " + value.toString + " to " + clazz.toString + ".")
    }
    
    def getLong : Long = {
        return value match {
            case v : Int => return v.toLong
            case v : Long => return v
            case _ => throw new IllegalArgumentException("Cannot convert " + value.toString + " to Long.")
        }
    }

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
implicit def intToV(i : Int) : V = new V(i)
implicit def longToV(l : Long) : V = new V(l)
implicit def floatToV(f : Float) : V = new V(f)
implicit def doubleToV(d : Double) : V = new V(d)
implicit def localDateTimeToV(l : LocalDateTime) : V = new V(l)
implicit def boolToV(b : Boolean) : V = new V(b)

// I imagine this will resolve to some type T by doing a lookup on the actual data?
// The actual type being used will be determined when the data is loaded
final case class F(fieldName : String) extends FieldExpression {
    val isWellTyped: Boolean = true

    def doesReturnType[EvalType](using tag : ClassTag[EvalType]): Boolean = true
    
    def toProtobuf : Expression = Expression().withValue(Value().withField(fieldName))
    def topLevelEvaluate: Any = fieldName
    def getFieldData[EvalType](using evidence : EvalType =:= String = null) : String = 
        if evidence != null then 
            return fieldName
        else
            throw new IllegalArgumentException("Field Name does not support this type")
}

abstract class FunctionCall[ReturnType](functionName : String) extends FieldExpression:
    val isWellTyped: Boolean = checkArgReturnTypes

    def doesReturnType[EvalType](using tag : ClassTag[EvalType]) : Boolean = checkReturnType && checkArgReturnTypes
    def checkReturnType[EvalType](using evidence : EvalType =:= ReturnType = null) : Boolean = evidence != null
    def checkArgReturnTypes: Boolean

    val arguments : Seq[FieldExpression]
    def toProtobuf : Expression = Expression().withFunction(Expression.FunctionCall(functionName=functionName, arguments=this.arguments.map(_.toProtobuf)))
    def topLevelEvaluate: Any = functionCalc
    def callFunction[EvalType](using evidence : EvalType =:= ReturnType = null) : ReturnType = {
        if evidence != null then 
            return functionCalc
        else 
            throw new IllegalArgumentException(functionName + " does not support this type.")
    }
    
    def functionCalc : ReturnType

/*
object FunctionBuilder:
    def buildUnaryFunction[ArgumentType, ReturnType](functionName : String, function : ) : UnaryFunction = {
        return (argument : ArgumentType) => {
            
        }
    }


abstract class UnaryFunction[ArgOneType, ReturnType](argument : FieldExpression) extends FunctionCall[ReturnType](functionName):
    val isWellTyped = 
    val arguments
    def functionCalc =*/ 