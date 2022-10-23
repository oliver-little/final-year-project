package model.fields

import scala.math.pow
import scala.reflect.{ClassTag, classTag}

/*
// Polymorphic form of Add, this should resolve the left and right type and pick one of Concat, IntAdd and DoubleAdd accordingly
final case class Add(left : FieldExpression, right : FieldExpression) extends FunctionCall("Add") {
    val arguments : Seq[FieldExpression] = Seq(left, right)
}
*/

final case class Concat(left : FieldExpression, right : FieldExpression) extends FunctionCall[String]("Concat") {
    def checkArgReturnTypes = left.doesReturnType[String] && right.doesReturnType[String]
    val arguments : Seq[FieldExpression] = Seq(left, right)
    def functionCalc : String = left.evaluate[String] + right.evaluate[String]
}


final case class IntAdd(left : FieldExpression, right : FieldExpression) extends FunctionCall[Long]("IntAdd") {
    def checkArgReturnTypes= left.doesReturnType[Long] && right.doesReturnType[Long]
    val arguments : Seq[FieldExpression] = Seq(left, right)
    def functionCalc : Long = left.evaluate[Long] + right.evaluate[Long]
}

final case class DoubleAdd(left : FieldExpression, right : FieldExpression) extends FunctionCall[Double]("IntAdd") {
    def checkArgReturnTypes = left.doesReturnType[Double] && right.doesReturnType[Double]
    val arguments : Seq[FieldExpression] = Seq(left, right)
    def functionCalc : Double = left.evaluate[Double] + right.evaluate[Double]
}

final case class Pow(value : FieldExpression, exponent: FieldExpression) extends FunctionCall[Double]("Pow") {
    def checkArgReturnTypes = value.doesReturnType[Double] && exponent.doesReturnType[Double]
    val arguments : Seq[FieldExpression] = Seq(value, exponent)
    def functionCalc : Double = pow(value.evaluate[Double], exponent.evaluate[Double])
}

//@main def main : Unit = System.out.println(Concat("a", 1).isWellTyped)