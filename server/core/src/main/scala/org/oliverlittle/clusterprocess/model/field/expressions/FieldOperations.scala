package org.oliverlittle.clusterprocess.model.field.expressions

import org.oliverlittle.clusterprocess.model.field.expressions
import org.oliverlittle.clusterprocess.model.table.TableResultHeader
import org.oliverlittle.clusterprocess.model.table.field.{TableField, TableValue}

import scala.math.pow
import java.time.Instant
import scala.reflect.{classTag, ClassTag}

object FieldOperations:
		val Concat = (l, r) => BinaryFunction[String, String, String]("Concat", (left, right) => left + right, l, r)
		val AddInt = (l, r) => BinaryFunction[Long, Long, Long]("AddInt", (left, right) => left + right, l, r)
		val AddDouble = (l, r) => BinaryFunction[Double, Double, Double]("AddDouble", (left, right) => left + right, l, r)
		val Pow = (l, r) => BinaryFunction[Double, Double, Double]("Pow", (value, exp) => pow(value, exp), l, r)
		val Substring = (stringToSlice, left, right) => TernaryFunction[String, Long, Long, String]("Substring", (s, l, r) => s.slice(l.toInt, r.toInt), stringToSlice, left, right)
		val Left = (string, index) => BinaryFunction[String, Long, String]("Left", (s, i) => s.slice(0, i.toInt), string, index)
		val Right = (string, index) => BinaryFunction[String, Long, String]("Right", (s, i) => s.slice(i.toInt, s.length), string, index)
		val Add = (l, r) => PolyAdd(l ,r)

/**
	* 'Polymorphic' version of add, taking two parameters of the same type, then resolving them to either a string concatenation, or an int or double addition depending on their types at runtime
	*
	* @param left Left argument for the add function
	* @param right Right argument for the add function
	*/
final case class PolyAdd(left : FieldExpression, right : FieldExpression) extends FunctionCall("Add"): 	
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