package org.oliverlittle.clusterprocess.model.field.expressions

import java.time.Instant
import java.time.format.DateTimeFormatter
import scala.reflect.{ClassTag, classTag}
import scala.util.Try
import java.text.DecimalFormat
import java.time.OffsetDateTime

import org.oliverlittle.clusterprocess.table_model._
import org.oliverlittle.clusterprocess.model.table._
import org.oliverlittle.clusterprocess.model.table.field._


// FieldExpressions

object FieldExpression:
    def fromProtobuf(expr : Expression) : FieldExpression = expr match {
        case Expression(Expression.Expr.Value(Value(Value.Value.Field(f), _)), _) => F(f)
        case Expression(Expression.Expr.Value(value), unknownFields) => V.fromProtobuf(value)
        case Expression(Expression.Expr.Function(functionCall), _) => FunctionCall.fromProtobuf(functionCall).asInstanceOf[FieldExpression]
        case Expression(Expression.Expr.Empty, unknownFields) => throw new IllegalArgumentException("Empty expression found.")
    }

trait FieldExpression:
    // Represents whether this field expression is well-typed (i.e.: we are at least able to attempt computation on it, as the base types all match)
    def isWellTyped(header : TableResultHeader) : Boolean 
    // Checks whether this FieldExpression is able to return the provided type parameter
    def doesReturnType[EvalType](header : TableResultHeader)(using tag : ClassTag[EvalType]) : Boolean
    def resolve(header : TableResultHeader) : ResolvedFieldExpression

    // Convert this FieldExpression to a protobuf expression
    lazy val protobuf : Expression

    def as(name : String) : NamedFieldExpression = NamedFieldExpression(name, this)

object ResolvedFieldExpression:
    def fromProtobuf(expr : Expression, header : TableResultHeader) : ResolvedFieldExpression = (expr match {
        case Expression(Expression.Expr.Value(Value(Value.Value.Field(f), _)), _) => F(f)
        case Expression(Expression.Expr.Value(value), unknownFields) => V.fromProtobuf(value)
        case Expression(Expression.Expr.Function(functionCall), _) => FunctionCall.fromProtobuf(functionCall).asInstanceOf[FieldExpression]
        case Expression(Expression.Expr.Empty, unknownFields) => throw new IllegalArgumentException("Empty expression found.")
    }).resolve(header)

/**
  * A FieldExpression that has been type-checked and resolved for runtime. 
  * 
  * Note: DO NOT INSTANTIATE THIS DIRECTLY, as there is no type safety without first using the (unresolved) FieldExpression form first.
  */
sealed abstract class ResolvedFieldExpression:
    // Evaluate this ResolvedFieldExpression from the top down, allowing the functions to resolve their own types.
    def evaluateAny(header : TableResultHeader, row : Seq[TableValue]) : Any 
    def evaluate[EvalType](header : TableResultHeader, row : Seq[TableValue])(using evalTag : ClassTag[EvalType]) : EvalType

object NamedFieldExpression:
    def fromProtobuf(protobuf : NamedExpression) : NamedFieldExpression = NamedFieldExpression(protobuf.name, FieldExpression.fromProtobuf(protobuf.expr.get))

final case class NamedFieldExpression(name : String, expr : FieldExpression):
    def resolve(header : TableResultHeader) = if expr.isWellTyped(header) then NamedResolvedFieldExpression(name, expr.resolve(header))
        else throw new IllegalArgumentException("Expression is not well typed. Cannot resolve.")

    def outputTableField(header : TableResultHeader) : TableField = expr match {
        case expr if expr.doesReturnType[String](header) => StringField(name)
        case expr if expr.doesReturnType[Long](header) => IntField(name)
        case expr if expr.doesReturnType[Double](header) => DoubleField(name)
        case expr if expr.doesReturnType[Instant](header) => DateTimeField(name)
        case expr if expr.doesReturnType[Boolean](header) => BoolField(name)
        case expr => throw new IllegalArgumentException("FieldExpression returns unknown type: " + expr.toString)
    }

    lazy val protobuf : NamedExpression = NamedExpression(name, Some(expr.protobuf))

final case class NamedResolvedFieldExpression(name : String, expr : ResolvedFieldExpression):
    /**
      * Evaluates the stored FieldExpression into a named and typed TableValue
      *
      * @param header The header for the row being evaluated
      * @param row The row being evaluated
      * @return A TableValue representing the evaluated NamedFieldExpression
      */
    def evaluate(header : TableResultHeader, row : Seq[TableValue]) : TableValue = expr.evaluateAny(header, row) match {
        case s : String => StringValue(s)
        case i : Long => IntValue(i)
        case f : Double => DoubleValue(f)
        case d : Instant => DateTimeValue(d)
        case b : Boolean => BoolValue(b)
        // This case should never happen
        case v => throw new IllegalArgumentException("Type of " + v.toString + " is invalid: " + v.getClass.toString)
    }


// Values

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
final case class V(inputValue : Any) extends ResolvedFieldExpression with FieldExpression:
    // Preprocess inputValue to convert ints and floats to longs and doubles
    val value = inputValue match {
        case v : Int => v.toLong
        case v : Float => v.toDouble
        case v => v
    }

    def isWellTyped(header : TableResultHeader) = Try{protobuf}.isSuccess
    
    def doesReturnType[EvalType](header : TableResultHeader)(using tag : ClassTag[EvalType]) : Boolean = Try{getValueAsType[EvalType]}.isSuccess

    lazy val protobuf : Expression = Expression().withValue(
        value match {
            case s : String => Value().withString(s)
            case i : Long => Value().withInt(i)
            case f : Double => Value().withDouble(f)
            case d : Instant => Value().withDatetime(DateTimeFormatter.ISO_INSTANT.format(d))
            case b : Boolean => Value().withBool(b)
            // This case should never happen
            case _ => throw new IllegalArgumentException("Type of " + value.toString + " is invalid: " + value.getClass.toString)
        }
    )

    def evaluateAny(header : TableResultHeader, row : Seq[TableValue]): Any = value

    // Returns this value as the provided type parameter, if possible.
    def evaluate[EvalType](header : TableResultHeader, row : Seq[TableValue])(using evalTag : ClassTag[EvalType]): EvalType = getValueAsType

    def getValueAsType[EvalType](using evalTag : ClassTag[EvalType]) : EvalType = {
        var clazz = evalTag.runtimeClass
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

    def resolve(header : TableResultHeader) : ResolvedFieldExpression = if isWellTyped(header) then this else throw new IllegalArgumentException("Value is not well typed, cannot resolve.")


// Fields

object F:
    implicit def toNamed(f : F) : NamedFieldExpression = NamedFieldExpression(f.fieldName, f)

final case class F(fieldName : String) extends FieldExpression:
    def isWellTyped(header : TableResultHeader): Boolean = header.headerMap.contains(fieldName)
    def doesReturnType[EvalType](header : TableResultHeader)(using tag: ClassTag[EvalType]): Boolean = header.headerMap.getOrElse(fieldName, throw new IllegalArgumentException("Field name not found: " + fieldName)).compareClassTags(tag)
    def resolve(header : TableResultHeader) : ResolvedF = if isWellTyped(header) then ResolvedF(fieldName, header) else throw new IllegalArgumentException("Function is not well typed, cannot resolve.")

    lazy val protobuf : Expression = Expression().withValue(Value().withField(fieldName))

final case class ResolvedF(fieldName : String, header : TableResultHeader) extends ResolvedFieldExpression:
    def evaluateAny(header : TableResultHeader, row : Seq[TableValue]): Any = row.lift(header.headerIndex.getOrElse(fieldName, throw new IllegalArgumentException("Field name not found: " + fieldName))).get.value
    def evaluate[EvalType](header : TableResultHeader, row : Seq[TableValue])(using evalTag : ClassTag[EvalType]) : EvalType = {
        val tableValue = row.lift(header.headerIndex.getOrElse(fieldName, throw new IllegalArgumentException("Field name not found: " + fieldName))).get
        if tableValue.compareClassTags(evalTag) then 
            return tableValue.value.asInstanceOf[EvalType]
        else
            throw new IllegalArgumentException("Cannot return EvalType.")
    }


// Functions

object FunctionCall:
    def fromProtobuf(f : Expression.FunctionCall) : Any = FieldOperations.getClass.getDeclaredMethod(f.functionName, FieldOperations.getClass).invoke(FieldOperations, f.arguments.map(FieldExpression.fromProtobuf(_)))

abstract class FunctionCall(functionName : String) extends FieldExpression:
    def isWellTyped(header : TableResultHeader): Boolean
    def doesReturnType[EvalType](header : TableResultHeader)(using evalTag : ClassTag[EvalType]) : Boolean
    def resolve(header : TableResultHeader) : ResolvedFieldExpression
    val arguments : Seq[FieldExpression]
    lazy val protobuf : Expression = Expression().withFunction(Expression.FunctionCall(functionName=functionName, arguments=this.arguments.map(_.protobuf)))

// Defines an abstract function call with an unknown number of parameters and a fixed return type
// The implicit (using) ClassTag[ReturnType] prevents the type erasure of the generic type ReturnType, which allows it 
final case class ResolvedFunctionCall[ReturnType](runtimeFunction : Seq[TableValue] => ReturnType)(using retTag : ClassTag[ReturnType]) extends ResolvedFieldExpression:
    def evaluateAny(header : TableResultHeader, row : Seq[TableValue]) : Any = runtimeFunction(row)
    def evaluate[EvalType](header : TableResultHeader, row : Seq[TableValue])(using evalTag : ClassTag[EvalType]) : EvalType = if retTag.equals(evalTag) then runtimeFunction(row).asInstanceOf[EvalType] else throw new IllegalArgumentException("Function cannot return type " + evalTag.runtimeClass.toString)

// Defines a function that takes one argument and returns a value
final case class UnaryFunction[ArgType, ReturnType](functionName : String, function : (argument : ArgType) => ReturnType, argument : FieldExpression)(using tag : ClassTag[ArgType])(using retTag : ClassTag[ReturnType]) extends FunctionCall(functionName):
    def isWellTyped(header : TableResultHeader) : Boolean = argument.doesReturnType[ArgType](header)
    def doesReturnType[EvalType](header : TableResultHeader)(using evalTag: ClassTag[EvalType]): Boolean = evalTag.equals(retTag)
    val arguments = Seq(argument)
    def resolve(header : TableResultHeader): ResolvedFunctionCall[ReturnType] = {
        if !isWellTyped(header) then throw new IllegalArgumentException("Function not well typed, cannot resolve.")
        val resolvedArg = argument.resolve(header)
        return ResolvedFunctionCall((row) => function(resolvedArg.evaluateAny(header, row).asInstanceOf[ArgType]))
    }

// Defines a function that takes two arguments and returns a value
final case class BinaryFunction[LeftArgType, RightArgType, ReturnType](functionName : String, function : (left : LeftArgType, right : RightArgType) => ReturnType, left : FieldExpression, right : FieldExpression)(using lTag : ClassTag[LeftArgType]) (using rTag : ClassTag[RightArgType])(using retTag : ClassTag[ReturnType]) extends FunctionCall(functionName):
    def isWellTyped(header : TableResultHeader) : Boolean = left.doesReturnType[LeftArgType](header) && right.doesReturnType[RightArgType](header)
    def doesReturnType[EvalType](header : TableResultHeader)(using evalTag: ClassTag[EvalType]): Boolean = evalTag.equals(retTag)
    val arguments = Seq(left, right)
    def resolve(header : TableResultHeader): ResolvedFunctionCall[ReturnType] = {
        if !isWellTyped(header) then throw new IllegalArgumentException("Function not well typed, cannot resolve.")

        val resolvedLeft = left.resolve(header)
        val resolvedRight = right.resolve(header)
        return ResolvedFunctionCall((row) => function(resolvedLeft.evaluateAny(header, row).asInstanceOf[LeftArgType], resolvedRight.evaluateAny(header, row).asInstanceOf[RightArgType]))
    }

// Defines a function that takes three arguments and returns a value
final case class TernaryFunction[ArgOneType, ArgTwoType, ArgThreeType, ReturnType](functionName : String, function : (one : ArgOneType, two : ArgTwoType, three : ArgThreeType) => ReturnType, one : FieldExpression, two : FieldExpression, three : FieldExpression)(using oneTag : ClassTag[ArgOneType], twoTag : ClassTag[ArgTwoType], threeTag : ClassTag[ArgThreeType])(using retTag : ClassTag[ReturnType]) extends FunctionCall(functionName):
    def isWellTyped(header : TableResultHeader) : Boolean = one.doesReturnType[ArgOneType](header) && two.doesReturnType[ArgTwoType](header) && three.doesReturnType[ArgThreeType](header)
    def doesReturnType[EvalType](header : TableResultHeader)(using evalTag: ClassTag[EvalType]): Boolean = evalTag.equals(retTag)
    val arguments = Seq(one, two, three)
    def resolve(header : TableResultHeader) : ResolvedFunctionCall[ReturnType] = {
        if !isWellTyped(header) then throw new IllegalArgumentException("Function not well typed, cannot resolve.")

        val resolvedOne = one.resolve(header)
        val resolvedTwo = two.resolve(header)
        val resolvedThree = three.resolve(header)
        return ResolvedFunctionCall((row) => function(resolvedOne.evaluateAny(header, row).asInstanceOf[ArgOneType], resolvedTwo.evaluateAny(header, row).asInstanceOf[ArgTwoType], resolvedThree.evaluateAny(header, row).asInstanceOf[ArgThreeType]))
    }

// Polymorphic cast definitions (takes any argument, and returns the type or an error)
final case class ToString(argument: FieldExpression) extends FunctionCall("ToString"):
    def isWellTyped(header : TableResultHeader) : Boolean = argument.doesReturnType[Any](header)
    def doesReturnType[EvalType](header : TableResultHeader)(using evalTag: ClassTag[EvalType]): Boolean = evalTag.equals(classTag[String])
    val arguments = Seq(argument)
    def resolve(header : TableResultHeader) : ResolvedFunctionCall[String] = {
        if !isWellTyped(header) then throw new IllegalArgumentException("Function not well typed, cannot resolve.")
        val resolvedArg = argument.resolve(header)
        return ResolvedFunctionCall((row) => resolvedArg.evaluateAny(header, row).toString)
    }

// ToString implementation specifically for Doubles, to enable specified precision
final case class DoubleToString(argument: FieldExpression, formatter : DecimalFormat) extends FunctionCall("DoubleToString"):
    def isWellTyped(header : TableResultHeader) : Boolean = argument.doesReturnType[Double](header)
    def doesReturnType[EvalType](header : TableResultHeader)(using evalTag: ClassTag[EvalType]): Boolean = evalTag.equals(classTag[String])
    val arguments = Seq(argument)
    def resolve(header : TableResultHeader) : ResolvedFunctionCall[String] = {
        if !isWellTyped(header) then throw new IllegalArgumentException("Function not well typed, cannot resolve.")
        val resolvedArg = argument.resolve(header)
        return ResolvedFunctionCall((row) => formatter.format(resolvedArg.evaluateAny(header, row)))
    }

final case class ToInt(argument : FieldExpression) extends FunctionCall("ToInt"):
    def isWellTyped(header : TableResultHeader) : Boolean = argument.doesReturnType[String](header) || argument.doesReturnType[Double](header) || argument.doesReturnType[Float](header) || argument.doesReturnType[Long](header) || argument.doesReturnType[Int](header)
    
    def doesReturnType[EvalType](header : TableResultHeader)(using evalTag: ClassTag[EvalType]): Boolean = evalTag.equals(classTag[Int])

    val arguments = Seq(argument)

    def resolve(header : TableResultHeader): ResolvedFunctionCall[Long] = {
        val resolvedArg = argument.resolve(header)
        return ResolvedFunctionCall((row) => resolvedArg.evaluateAny(header, row) match {
            case v : String => v.toLong
            case v : Double => v.toLong
            case v : Float => v.toLong
            case v : Long => v
            case v : Int => v.toLong
            case v => throw new IllegalArgumentException("Cannot convert " + v.toString + " of type " + v.getClass + " to Long.")
        })
    }

final case class ToDouble(argument : FieldExpression) extends FunctionCall("ToDouble"):
    def isWellTyped(header : TableResultHeader) : Boolean = argument.doesReturnType[String](header) || argument.doesReturnType[Double](header) || argument.doesReturnType[Float](header) || argument.doesReturnType[Long](header) || argument.doesReturnType[Int](header)
    def doesReturnType[EvalType](header : TableResultHeader)(using evalTag: ClassTag[EvalType]): Boolean = evalTag.equals(classTag[Double])
    val arguments = Seq(argument)

    def resolve(header : TableResultHeader): ResolvedFunctionCall[Double] = {
        if !isWellTyped(header) then throw new IllegalArgumentException("Function not well typed, cannot resolve.")
        val resolvedArg = argument.resolve(header)
        return ResolvedFunctionCall((row) => resolvedArg.evaluateAny(header, row) match {
            case v : String => v.toDouble
            case v : Double => v
            case v : Float => v.toDouble
            case v : Long => v.toDouble
            case v : Int => v.toDouble
            case v => throw new IllegalArgumentException("Cannot convert " + v.toString + " of type " + v.getClass + " to Long.")
        })
    }