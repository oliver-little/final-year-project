package org.oliverlittle.clusterprocess.model.field.expressions

import org.oliverlittle.clusterprocess.model.field.expressions
import org.oliverlittle.clusterprocess.model.table.field.{TableField, TableValue}

import scala.math.pow
import java.time.Instant
import scala.reflect.ClassTag

object FieldOperations:
    val Concat = (l, r) => BinaryFunction[String, String, String]("Concat", (left, right) => left + right, l, r)
    val AddInt = (l, r) => BinaryFunction[Long, Long, Long]("AddInt", (left, right) => left + right, l, r)
    val AddDouble = (l, r) => BinaryFunction[Double, Double, Double]("AddDouble", (left, right) => left + right, l, r)
    val Pow = (l, r) => BinaryFunction[Double, Double, Double]("Pow", (value, exp) => pow(value, exp), l, r)
    val Substring = (stringToSlice, left, right) => TernaryFunction[String, Long, Long, String]("Substring", (s, l, r) => s.slice(l.toInt, r.toInt), stringToSlice, left, right)
    val Left = (string, index) => BinaryFunction[String, Long, String]("Left", (s, i) => s.slice(0, i.toInt), string, index)
    val Right = (string, index) => BinaryFunction[String, Long, String]("Right", (s, i) => s.slice(i.toInt, s.length), string, index)

    /**
      * 'Polymorphic' version of add, taking two parameters of the same type, then resolving them to either a string concatenation, or an int or double addition depending on their types at runtime
      *
      * @param left Left argument for the add function
      * @param right Right argument for the add function
      */
    class Add(left : FieldExpression, right : FieldExpression) extends FunctionCall[Any]("Add"):
        // Option which is resolved at runtime to determine which add function to actually use
        var resolvedAddFunction : Option[FieldExpression] = None
        
        /**
          * Takes a fieldContext, and performs state management to get a concrete instance of Add
          *
          * @param fieldContext A list of fields in this table, and their types
          * @return The concrete add instance
          */
        def getAddFunction(fieldContext : Map[String, TableField])  : FieldExpression = resolvedAddFunction.getOrElse(resolveRuntimeFunction(fieldContext))
        
        /**
          * Resets the state of this polymorphic Add to allow the add function to be resolved again.
          */
        def reset : Unit = resolvedAddFunction = None

        /**
          * Takes a fieldContext, and returns a concrete add function based on the argument types
          *
          * @param fieldContext A list of fields in this table, and their types
          * @return Concat for String types, Integer addition for Long types, and Double addition for Double types
          */
        def resolveRuntimeFunction(fieldContext : Map[String, TableField]) : FieldExpression = {
            if (left.doesReturnType[String](fieldContext) && right.doesReturnType[String](fieldContext)) {
                resolvedAddFunction = Some(Concat(left, right))
            }
            else if (left.doesReturnType[Long](fieldContext) && right.doesReturnType[Long](fieldContext)) {
                resolvedAddFunction = Some(AddInt(left, right))
            }
            else if (left.doesReturnType[Double](fieldContext) && right.doesReturnType[Double](fieldContext)) {
                resolvedAddFunction = Some(AddDouble(left, right))
            } 
            else {
                throw new IllegalArgumentException("Parameter FieldExpressions must return type (String, String), (Long, Long) or (Double, Double). (Are you missing a cast?)")
            }

            return resolvedAddFunction.get
        }


        def isWellTyped(fieldContext : Map[String, TableField]) : Boolean = getAddFunction(fieldContext).isWellTyped(fieldContext)

        val arguments = Seq(left, right)

        override def doesReturnType[EvalType](fieldContext : Map[String, TableField])(using evalTag: ClassTag[EvalType]) : Boolean = getAddFunction(fieldContext).doesReturnType[EvalType](fieldContext)

        def functionCalc(rowContext : Map[String, TableValue]) : Any = getAddFunction(rowContext).evaluateAny