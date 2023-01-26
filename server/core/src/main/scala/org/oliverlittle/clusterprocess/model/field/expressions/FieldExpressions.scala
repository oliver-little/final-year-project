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
import org.oliverlittle.clusterprocess.model.table.field.TableValue.given

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
    def doesReturnType[T](header : TableResultHeader)(using evalTag : ClassTag[T]) : Boolean
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
    // Headers should already be checked at the resolution phase, so should not be required
    def evaluate(row : Seq[Option[TableValue]]) : Option[TableValue]

object NamedFieldExpression:
    def fromProtobuf(protobuf : NamedExpression) : NamedFieldExpression = NamedFieldExpression(protobuf.name, FieldExpression.fromProtobuf(protobuf.expr.get))

final case class NamedFieldExpression(name : String, expr : FieldExpression):
    def resolve(header : TableResultHeader) = if expr.isWellTyped(header) then NamedResolvedFieldExpression(name, expr.resolve(header))
        else throw new IllegalArgumentException("Expression is not well typed. Cannot resolve.")

    def outputTableField(header : TableResultHeader) : TableField = 
        if expr.doesReturnType[String](header) then BaseStringField(name)
        else if expr.doesReturnType[Long](header) then BaseIntField(name)
        else if expr.doesReturnType[Double](header) then BaseDoubleField(name)
        else if expr.doesReturnType[Boolean](header) then BaseBoolField(name)
        else if expr.doesReturnType[Instant](header) then BaseDateTimeField(name)
        else throw new IllegalArgumentException("FieldExpression returns unknown type: " + expr.toString)

    lazy val protobuf : NamedExpression = NamedExpression(name, Some(expr.protobuf))

final case class NamedResolvedFieldExpression(name : String, expr : ResolvedFieldExpression):
    def evaluate(row : Seq[Option[TableValue]]) : Option[TableValue] = expr.evaluate(row)

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

    val tableValue = TableValue.valueToTableValue(inputValue)

    def isWellTyped(header : TableResultHeader) = Try{protobuf}.isSuccess
    
    def doesReturnType[T](header : TableResultHeader)(using evalTag : ClassTag[T]) : Boolean = evalTag.equals(tableValue.valueTag)

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

    def evaluate(row : Seq[Option[TableValue]]) : Option[TableValue] = Some(tableValue)
    
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
    given fToNamed : Conversion[F, NamedFieldExpression] = (f) => NamedFieldExpression(f.fieldName, f)

final case class F(fieldName : String) extends FieldExpression:
    def isWellTyped(header : TableResultHeader): Boolean = header.headerMap.contains(fieldName)
    def doesReturnType[T](header : TableResultHeader)(using evalTag : ClassTag[T]) : Boolean = evalTag.equals(header.headerMap.getOrElse(fieldName, throw new IllegalArgumentException("Field name not found: " + fieldName)).valueTag)
    def resolve(header : TableResultHeader) : ResolvedF = if isWellTyped(header) then ResolvedF(fieldName, header) else throw new IllegalArgumentException("Function is not well typed, cannot resolve.")

    lazy val protobuf : Expression = Expression().withValue(Value().withField(fieldName))

final case class ResolvedF(fieldName : String, header : TableResultHeader) extends ResolvedFieldExpression:
    def evaluate(row : Seq[Option[TableValue]]) : Option[TableValue] = row.lift(header.headerIndex.getOrElse(fieldName, throw new IllegalArgumentException("Field name not found: " + fieldName))).get


// Functions

object FunctionCall:
    def fromProtobuf(f : Expression.FunctionCall) : Any = FieldOperations.getClass.getDeclaredMethod(f.functionName, FieldOperations.getClass).invoke(FieldOperations, f.arguments.map(FieldExpression.fromProtobuf(_)))

abstract class FunctionCall(functionName : String) extends FieldExpression:
    def resolve(header : TableResultHeader) : ResolvedFieldExpression
    val arguments : Seq[FieldExpression]
    lazy val protobuf : Expression = Expression().withFunction(Expression.FunctionCall(functionName=functionName, arguments=this.arguments.map(_.protobuf)))

// Defines an abstract function call with an unknown number of parameters and a fixed return type
// The implicit (using) ClassTag[ReturnType] prevents the type erasure of the generic type ReturnType, which allows it 
final case class ResolvedFunctionCall[ReturnType](runtimeFunction : Seq[Option[TableValue]] => Option[ReturnType]) extends ResolvedFieldExpression:
    def evaluate(row : Seq[Option[TableValue]]) : Option[TableValue] = runtimeFunction(row).flatMap(v => Some(TableValue.valueToTableValue(v)))

// Defines a function that takes one argument and returns a value
final case class UnaryFunction[ArgType, ReturnType](functionName : String, function : ArgType => ReturnType, argument : FieldExpression)(using argTag : ClassTag[ArgType])(using retTag : ClassTag[ReturnType]) extends FunctionCall(functionName):
    def isWellTyped(header : TableResultHeader) : Boolean = argument.doesReturnType[ArgType](header)
    def doesReturnType[T](header : TableResultHeader)(using evalTag : ClassTag[T]) : Boolean = evalTag.equals(retTag)
    val arguments = Seq(argument)
    def resolve(header : TableResultHeader): ResolvedFunctionCall[ReturnType] = {
        if !isWellTyped(header) then throw new IllegalArgumentException("Function not well typed, cannot resolve.")
        val resolvedArg = argument.resolve(header)
        return ResolvedFunctionCall((row) => 
            resolvedArg.evaluate(row).flatMap(v => 
                Some(function(v.value.asInstanceOf[ArgType]))
            )
        )
    }

// Defines a function that takes two arguments and returns a value
final case class BinaryFunction[LeftArgType, RightArgType, ReturnType](functionName : String, function : (LeftArgType, RightArgType) => ReturnType, left : FieldExpression, right : FieldExpression)(using lTag : ClassTag[LeftArgType]) (using rTag : ClassTag[RightArgType])(using retTag : ClassTag[ReturnType]) extends FunctionCall(functionName):
    def isWellTyped(header : TableResultHeader) : Boolean = left.doesReturnType[LeftArgType](header) && right.doesReturnType[RightArgType](header)
    def doesReturnType[T](header : TableResultHeader)(using evalTag : ClassTag[T]) : Boolean = evalTag.equals(retTag)
    val arguments = Seq(left, right)
    def resolve(header : TableResultHeader): ResolvedFunctionCall[ReturnType] = {
        if !isWellTyped(header) then throw new IllegalArgumentException("Function not well typed, cannot resolve.")

        val resolvedLeft = left.resolve(header)
        val resolvedRight = right.resolve(header)
        return ResolvedFunctionCall((row) =>
            resolvedLeft.evaluate(row).flatMap(l => 
                resolvedRight.evaluate(row).flatMap(r =>
                    Some(function(l.value.asInstanceOf[LeftArgType], r.value.asInstanceOf[RightArgType]))
                )
            )
        )
    }

// Defines a function that takes three arguments and returns a value
final case class TernaryFunction[ArgOneType, ArgTwoType, ArgThreeType, ReturnType](functionName : String, function : (ArgOneType, ArgTwoType, ArgThreeType) => ReturnType, one : FieldExpression, two : FieldExpression, three : FieldExpression)(using oneTag : ClassTag[ArgOneType], twoTag : ClassTag[ArgTwoType], threeTag : ClassTag[ArgThreeType])(using retTag : ClassTag[ReturnType]) extends FunctionCall(functionName):
    def isWellTyped(header : TableResultHeader) : Boolean = one.doesReturnType[ArgOneType](header) && two.doesReturnType[ArgTwoType](header) && three.doesReturnType[ArgThreeType](header)
    def doesReturnType[T](header : TableResultHeader)(using evalTag : ClassTag[T]) : Boolean = evalTag.equals(retTag)
    val arguments = Seq(one, two, three)
    def resolve(header : TableResultHeader) : ResolvedFunctionCall[ReturnType] = {
        if !isWellTyped(header) then throw new IllegalArgumentException("Function not well typed, cannot resolve.")

        val resolvedOne = one.resolve(header)
        val resolvedTwo = two.resolve(header)
        val resolvedThree = three.resolve(header)
        return ResolvedFunctionCall((row) => 
            resolvedOne.evaluate(row).flatMap(one => 
                resolvedTwo.evaluate(row).flatMap(two => 
                    resolvedThree.evaluate(row).flatMap(three => 
                        Some(function(one.value.asInstanceOf[ArgOneType], two.value.asInstanceOf[ArgTwoType], three.value.asInstanceOf[ArgThreeType]))
                    )
                )
            )
        )
    }

// Polymorphic cast definitions (takes any argument, and returns the type or an error)
final case class ToString(argument: FieldExpression) extends FunctionCall("ToString"):
    def isWellTyped(header : TableResultHeader) : Boolean = argument.doesReturnType[String](header) || argument.doesReturnType[Double](header) || argument.doesReturnType[Float](header) || argument.doesReturnType[Long](header) || argument.doesReturnType[Int](header) || argument.doesReturnType[Boolean](header) || argument.doesReturnType[Instant](header)
    def doesReturnType[T](header : TableResultHeader)(using evalTag : ClassTag[T]) : Boolean = evalTag.equals(classTag[String])
    val arguments = Seq(argument)
    def resolve(header : TableResultHeader) : ResolvedFunctionCall[String] = {
        if !isWellTyped(header) then throw new IllegalArgumentException("Function not well typed, cannot resolve.")
        val resolvedArg = argument.resolve(header)
        return ResolvedFunctionCall((row) => resolvedArg.evaluate(row).flatMap(v => Some(v.value.toString)))
    }

// ToString implementation specifically for Doubles, to enable specified precision
final case class DoubleToString(argument: FieldExpression, formatter : DecimalFormat) extends FunctionCall("DoubleToString"):
    def isWellTyped(header : TableResultHeader) : Boolean = argument.doesReturnType[Double](header)
    def doesReturnType[T](header : TableResultHeader)(using evalTag : ClassTag[T]) : Boolean = evalTag.equals(classTag[String])
    val arguments = Seq(argument)
    def resolve(header : TableResultHeader) : ResolvedFunctionCall[String] = {
        if !isWellTyped(header) then throw new IllegalArgumentException("Function not well typed, cannot resolve.")
        val resolvedArg = argument.resolve(header)
        return ResolvedFunctionCall((row) => resolvedArg.evaluate(row).flatMap(v => Some(formatter.format(v.value))))
    }

final case class ToInt(argument : FieldExpression) extends FunctionCall("ToInt"):
    def isWellTyped(header : TableResultHeader) : Boolean = argument.doesReturnType[String](header) || argument.doesReturnType[Double](header) || argument.doesReturnType[Float](header) || argument.doesReturnType[Long](header) || argument.doesReturnType[Int](header)
    
    def doesReturnType[T](header : TableResultHeader)(using evalTag : ClassTag[T]) : Boolean = evalTag.equals(classTag[Int])

    val arguments = Seq(argument)

    def resolve(header : TableResultHeader): ResolvedFunctionCall[Long] = {
        val resolvedArg = argument.resolve(header)
        return ResolvedFunctionCall((row) => resolvedArg.evaluate(row) flatMap {
            case IntValue(value) => Some(value)
            case DoubleValue(value) => Some(value.toLong)
            case StringValue(value) => Some(value.toLong)
            case v => throw new IllegalArgumentException("Cannot convert " + v.toString + " of type " + v.getClass + " to Long.")
        })
    }

final case class ToDouble(argument : FieldExpression) extends FunctionCall("ToDouble"):
    def isWellTyped(header : TableResultHeader) : Boolean = argument.doesReturnType[String](header) || argument.doesReturnType[Double](header) || argument.doesReturnType[Float](header) || argument.doesReturnType[Long](header) || argument.doesReturnType[Int](header)
    def doesReturnType[T](header : TableResultHeader)(using evalTag : ClassTag[T]) : Boolean = evalTag.equals(classTag[Double])
    val arguments = Seq(argument)

    def resolve(header : TableResultHeader): ResolvedFunctionCall[Double] = {
        if !isWellTyped(header) then throw new IllegalArgumentException("Function not well typed, cannot resolve.")
        val resolvedArg = argument.resolve(header)
        return ResolvedFunctionCall((row) => resolvedArg.evaluate(row) flatMap {
            case IntValue(value) => Some(value.toDouble)
            case DoubleValue(value) => Some(value)
            case StringValue(value) => Some(value.toDouble)
            case v => throw new IllegalArgumentException("Cannot convert " + v.toString + " of type " + v.getClass + " to Double.")
        })
    }