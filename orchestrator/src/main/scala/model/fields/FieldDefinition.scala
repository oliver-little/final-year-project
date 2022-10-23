package model.fields

import scala.math.pow
import scala.reflect.{ClassTag, classTag}

/*
// Polymorphic form of Add, this should resolve the left and right type and pick one of Concat, IntAdd and DoubleAdd accordingly
final case class Add(left : FieldExpression, right : FieldExpression) extends FunctionCall("Add") {
    val arguments : Seq[FieldExpression] = Seq(left, right)
}
*/
val Concat = (l, r) => BinaryFunction[String, String, String]("Concat", (left, right) => left + right, l, r)
val AddInt = (l, r) => BinaryFunction[Long, Long, Long]("AddInt", (left, right) => left + right, l, r)
val AddDouble = (l, r) => BinaryFunction[Double, Double, Double]("AddDouble", (left, right) => left + right, l, r)
val Pow = (l, r) => BinaryFunction[Double, Double, Double]("Pow", (value, exp) => pow(value, exp), l, r)
val CastToString = (in) => UnaryFunction[Any, String]("CastToString", (i) => i.toString, in)
// Need to investigate further how to design functions so they can be more polymorphic (infer type from arguments)
// Might require redesigning the type system
// Cast functions in the current state will require a new cast function for each conversion
// Would be better to have cast take a type parameter of the type to convert to
// val CastToInt = (in) => UnaryFunction[String | Double, String]("CastToString", (i) => i.toLong, in)

//@main def main : Unit = System.out.println(Concat("a", 1).isWellTyped)