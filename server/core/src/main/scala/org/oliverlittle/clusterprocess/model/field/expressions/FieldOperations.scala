package org.oliverlittle.clusterprocess.model.field.expressions

import org.oliverlittle.clusterprocess.model.field.expressions
import org.oliverlittle.clusterprocess.model.table.TableResultHeader
import org.oliverlittle.clusterprocess.model.table.field._

import scala.math.pow
import java.time.Instant
import java.text.DecimalFormat
import scala.reflect.{classTag, ClassTag}

object FieldOperations:
	// Arithmetic
	def AddInt(l : FieldExpression, r : FieldExpression) : FunctionCall = BinaryFunction[Long, Long, Long]("AddInt", (left, right) => left + right, l, r)
	def AddDouble(l : FieldExpression, r : FieldExpression) : FunctionCall = BinaryFunction[Double, Double, Double]("AddDouble", (left, right) => left + right, l, r)
	def MulInt(l : FieldExpression, r : FieldExpression) : FunctionCall = BinaryFunction[Long, Long, Long]("MulInt", (left, right) => left * right, l, r)
	def MulDouble(l : FieldExpression, r : FieldExpression) : FunctionCall = BinaryFunction[Double, Double, Double]("MulDouble", (left, right) => left * right, l, r)
	def SubInt(l : FieldExpression, r : FieldExpression) : FunctionCall = BinaryFunction[Long, Long, Long]("SubInt", (left, right) => left - right, l, r)
	def SubDouble(l : FieldExpression, r : FieldExpression) : FunctionCall = BinaryFunction[Double, Double, Double]("SubDouble", (left, right) => left - right, l, r)
	def Div(l : FieldExpression, r : FieldExpression) : FunctionCall = BinaryFunction[Double, Double, Double]("Div", (left, right) => left / right, l, r)
	def Pow(l : FieldExpression, r : FieldExpression) : FunctionCall = BinaryFunction[Double, Double, Double]("Pow", (value, exp) => pow(value, exp), l, r)

	// Polymorphic Arithmetic
	def Add(l : FieldExpression, r : FieldExpression) : FunctionCall = PolyAdd(l, r)
	def Mul(l : FieldExpression, r : FieldExpression) : FunctionCall = PolyMul(l, r)
	def Sub(l : FieldExpression, r : FieldExpression) : FunctionCall = PolySub(l, r)

	def Concat(l : FieldExpression, r : FieldExpression) : FunctionCall = BinaryFunction[String, String, String]("Concat", (left, right) => left + right, l, r)
	def Substring(stringToSlice : FieldExpression, left : FieldExpression, right : FieldExpression) : FunctionCall = TernaryFunction[String, Long, Long, String]("Substring", (s, l, r) => s.slice(l.toInt, r.toInt), stringToSlice, left, right)
	def Left(string : FieldExpression, index : FieldExpression) : FunctionCall = BinaryFunction[String, Long, String]("Left", (s, i) => s.slice(0, i.toInt), string, index)
	def Right(string : FieldExpression, index : FieldExpression) : FunctionCall = BinaryFunction[String, Long, String]("Right", (s, i) => s.slice(i.toInt + 1, s.length), string, index)
	
	def ToString(v : FieldExpression) : FunctionCall = CastToString(v)
	def ToInt(v : FieldExpression) : FunctionCall = CastToInt(v)
	def ToDouble(v : FieldExpression) : FunctionCall = CastToDouble(v)

/**
	* 'Polymorphic' version of add, taking two parameters of the same type, then resolving them to either a string concatenation, or an int or double addition depending on their types at runtime
	*
	* @param left Left argument for the add function
	* @param right Right argument for the add function
	*/
final case class PolyAdd(left : FieldExpression, right : FieldExpression) extends FunctionCall:
	val functionName = "Add" 

	def isWellTyped(header : TableResultHeader) : Boolean = (left.doesReturnType[String](header) && right.doesReturnType[String](header)) || (left.doesReturnType[Long](header) && right.doesReturnType[Long](header)) || (left.doesReturnType[Double](header) && right.doesReturnType[Double](header))


	def doesReturnType[EvalType](header : TableResultHeader)(using evalTag: ClassTag[EvalType]) : Boolean = {
		if evalTag.equals(classTag[String]) && left.doesReturnType[String](header) && right.doesReturnType[String](header) then
			return true
		else if	evalTag.equals(classTag[Long]) && left.doesReturnType[Long](header) && right.doesReturnType[Long](header) then
			return true
		else if evalTag.equals(classTag[Double]) && left.doesReturnType[Double](header) && right.doesReturnType[Double](header) then
			return true
		else
			return false
	}

	/**
		* Takes a header, and returns a concrete add function based on the argument types
		*
		* @param header A list of fields in this table, and their types
		* @return Concat for String types, Integer addition for Long types, and Double addition for Double types
		*/
	def resolve(header : TableResultHeader) : ResolvedFieldExpression = {
			if (left.doesReturnType[String](header) && right.doesReturnType[String](header)) {
					return FieldOperations.Concat(left, right).resolve(header)
			}
			else if (left.doesReturnType[Long](header) && right.doesReturnType[Long](header)) {
					return FieldOperations.AddInt(left, right).resolve(header)
			}
			else if (left.doesReturnType[Double](header) && right.doesReturnType[Double](header)) {
					return FieldOperations.AddDouble(left, right).resolve(header)
			} 
			else {
					throw new IllegalArgumentException("Parameter FieldExpressions must return type (String, String), (Long, Long) or (Double, Double). (Are you missing a cast?)")
			}
	}
	val arguments = Seq(left, right)

/**
	* 'Polymorphic' version of mul, taking two parameters of the same type, then resolving them to an int or double multiplication depending on their types at runtime
	*
	* @param left Left argument for the mul function
	* @param right Right argument for the mul function
	*/
final case class PolyMul(left : FieldExpression, right : FieldExpression) extends FunctionCall:
	val functionName = "Mul"

	def isWellTyped(header : TableResultHeader) : Boolean = (left.doesReturnType[Long](header) && right.doesReturnType[Long](header)) || (left.doesReturnType[Double](header) && right.doesReturnType[Double](header))


	def doesReturnType[EvalType](header : TableResultHeader)(using evalTag: ClassTag[EvalType]) : Boolean = {
		if evalTag.equals(classTag[Long]) && left.doesReturnType[Long](header) && right.doesReturnType[Long](header) then
			return true
		else if evalTag.equals(classTag[Double]) && left.doesReturnType[Double](header) && right.doesReturnType[Double](header) then
			return true
		else
			return false
	}

	/**
		* Takes a header, and returns a concrete add function based on the argument types
		*
		* @param header A list of fields in this table, and their types
		* @return Concat for String types, Integer addition for Long types, and Double addition for Double types
		*/
	def resolve(header : TableResultHeader) : ResolvedFieldExpression = {
			if (left.doesReturnType[Long](header) && right.doesReturnType[Long](header)) {
					return FieldOperations.MulInt(left, right).resolve(header)
			}
			else if (left.doesReturnType[Double](header) && right.doesReturnType[Double](header)) {
					return FieldOperations.MulDouble(left, right).resolve(header)
			} 
			else {
					throw new IllegalArgumentException("Parameter FieldExpressions must return type (Long, Long) or (Double, Double). (Are you missing a cast?)")
			}
	}
	val arguments = Seq(left, right)

/**
	* 'Polymorphic' version of mul, taking two parameters of the same type, then resolving them to an int or double multiplication depending on their types at runtime
	*
	* @param left Left argument for the mul function
	* @param right Right argument for the mul function
	*/
final case class PolySub(left : FieldExpression, right : FieldExpression) extends FunctionCall: 	
	val functionName = "Sub"

	def isWellTyped(header : TableResultHeader) : Boolean = (left.doesReturnType[Long](header) && right.doesReturnType[Long](header)) || (left.doesReturnType[Double](header) && right.doesReturnType[Double](header))


	def doesReturnType[EvalType](header : TableResultHeader)(using evalTag: ClassTag[EvalType]) : Boolean = {
		if evalTag.equals(classTag[Long]) && left.doesReturnType[Long](header) && right.doesReturnType[Long](header) then
			return true
		else if evalTag.equals(classTag[Double]) && left.doesReturnType[Double](header) && right.doesReturnType[Double](header) then
			return true
		else
			return false
	}

	/**
		* Takes a header, and returns a concrete add function based on the argument types
		*
		* @param header A list of fields in this table, and their types
		* @return Concat for String types, Integer addition for Long types, and Double addition for Double types
		*/
	def resolve(header : TableResultHeader) : ResolvedFieldExpression = {
			if (left.doesReturnType[Long](header) && right.doesReturnType[Long](header)) {
					return FieldOperations.SubInt(left, right).resolve(header)
			}
			else if (left.doesReturnType[Double](header) && right.doesReturnType[Double](header)) {
					return FieldOperations.SubDouble(left, right).resolve(header)
			} 
			else {
					throw new IllegalArgumentException("Parameter FieldExpressions must return type (Long, Long) or (Double, Double). (Are you missing a cast?)")
			}
	}
	val arguments = Seq(left, right)

// Polymorphic cast definitions (takes any argument, and returns the type or an error)
final case class CastToString(argument: FieldExpression) extends FunctionCall:
	val functionName = "ToString"
	val arguments = Seq(argument)

	def isWellTyped(header : TableResultHeader) : Boolean = argument.doesReturnType[String](header) || argument.doesReturnType[Double](header) || argument.doesReturnType[Float](header) || argument.doesReturnType[Long](header) || argument.doesReturnType[Int](header) || argument.doesReturnType[Boolean](header) || argument.doesReturnType[Instant](header)
	def doesReturnType[T](header : TableResultHeader)(using evalTag : ClassTag[T]) : Boolean = evalTag.equals(classTag[String])
	def resolve(header : TableResultHeader) : ResolvedFunctionCall[String] = {
		if !isWellTyped(header) then throw new IllegalArgumentException("Function not well typed, cannot resolve.")
		val resolvedArg = argument.resolve(header)
		return ResolvedFunctionCall((row) => resolvedArg.evaluate(row).map(v => v.value.toString))
	}

// ToString implementation specifically for Doubles, to enable specified precision
final case class DoubleToString(argument: FieldExpression, formatter : DecimalFormat) extends FunctionCall:
	val functionName = "DoubleToString"
	val arguments = Seq(argument)

	def isWellTyped(header : TableResultHeader) : Boolean = argument.doesReturnType[Double](header)
	def doesReturnType[T](header : TableResultHeader)(using evalTag : ClassTag[T]) : Boolean = evalTag.equals(classTag[String])
	def resolve(header : TableResultHeader) : ResolvedFunctionCall[String] = {
		if !isWellTyped(header) then throw new IllegalArgumentException("Function not well typed, cannot resolve.")
		val resolvedArg = argument.resolve(header)
		return ResolvedFunctionCall((row) => resolvedArg.evaluate(row).map(v => formatter.format(v.value)))
	}

final case class CastToInt(argument : FieldExpression) extends FunctionCall:
	val functionName = "ToInt"
	val arguments = Seq(argument)

	def isWellTyped(header : TableResultHeader) : Boolean = argument.doesReturnType[String](header) || argument.doesReturnType[Double](header) || argument.doesReturnType[Float](header) || argument.doesReturnType[Long](header) || argument.doesReturnType[Int](header)
	
	def doesReturnType[T](header : TableResultHeader)(using evalTag : ClassTag[T]) : Boolean = evalTag.equals(classTag[Int])

	def resolve(header : TableResultHeader): ResolvedFunctionCall[Long] = {
		val resolvedArg = argument.resolve(header)
		return ResolvedFunctionCall((row) => resolvedArg.evaluate(row) map {
			case IntValue(value) => value
			case DoubleValue(value) => value.toLong
			case StringValue(value) => value.toLong
			case v => throw new IllegalArgumentException("Cannot convert " + v.toString + " of type " + v.getClass + " to Long.")
		})
	}

final case class CastToDouble(argument : FieldExpression) extends FunctionCall:
	val functionName = "ToDouble"
	val arguments = Seq(argument)

	def isWellTyped(header : TableResultHeader) : Boolean = argument.doesReturnType[String](header) || argument.doesReturnType[Double](header) || argument.doesReturnType[Float](header) || argument.doesReturnType[Long](header) || argument.doesReturnType[Int](header)
	def doesReturnType[T](header : TableResultHeader)(using evalTag : ClassTag[T]) : Boolean = evalTag.equals(classTag[Double])

	def resolve(header : TableResultHeader): ResolvedFunctionCall[Double] = {
		if !isWellTyped(header) then throw new IllegalArgumentException("Function not well typed, cannot resolve.")
		val resolvedArg = argument.resolve(header)
		return ResolvedFunctionCall((row) => resolvedArg.evaluate(row) map {
			case IntValue(value) => value.toDouble
			case DoubleValue(value) => value
			case StringValue(value) => value.toDouble
			case v => throw new IllegalArgumentException("Cannot convert " + v.toString + " of type " + v.getClass + " to Double.")
		})
	}