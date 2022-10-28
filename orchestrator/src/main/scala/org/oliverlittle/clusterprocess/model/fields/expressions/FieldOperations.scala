package org.oliverlittle.clusterprocess.model.field.expressions

import scala.math.pow
import java.time.LocalDateTime

import org.oliverlittle.clusterprocess.model.field.expressions

val Concat = (l, r) => BinaryFunction[String, String, String]("Concat", (left, right) => left + right, l, r)
val AddInt = (l, r) => BinaryFunction[Long, Long, Long]("AddInt", (left, right) => left + right, l, r)
val AddDouble = (l, r) => BinaryFunction[Double, Double, Double]("AddDouble", (left, right) => left + right, l, r)
val Pow = (l, r) => BinaryFunction[Double, Double, Double]("Pow", (value, exp) => pow(value, exp), l, r)
val Substring = (stringToSlice, left, right) => TernaryFunction[String, Long, Long, String]("Substring", (s, l, r) => s.slice(l.toInt, r.toInt), stringToSlice, left, right)
val Left = (string, index) => BinaryFunction[String, Long, String]("Left", (s, i) => s.slice(0, i.toInt), string, index)
val Right = (string, index) => BinaryFunction[String, Long, String]("Right", (s, i) => s.slice(i.toInt, s.length), string, index)

// Polymorphic form of Add, this should resolve the left and right type and pick one of Concat, IntAdd and DoubleAdd accordingly
def Add(left : FieldExpression, right : FieldExpression) : FieldExpression = {
    if (left.doesReturnType[String] && right.doesReturnType[String]) {
        return Concat(left, right)
    }
    else if (left.doesReturnType[Long] && right.doesReturnType[Long]) {
        return AddInt(left, right)
    }
    else if (left.doesReturnType[Double] && right.doesReturnType[Double]) {
        return AddDouble(left, right)
    } 
    else {
        throw new IllegalArgumentException("Parameter FieldExpressions must return type (String, String), (Long, Long) or (Double, Double). (Are you missing a cast?)")
    }
}