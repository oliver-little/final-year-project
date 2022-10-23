package model.fields

import scala.math.pow

/*
// Polymorphic form of Add, this should resolve the left and right type and pick one of Concat, IntAdd and DoubleAdd accordingly
final case class Add(left : FieldExpression, right : FieldExpression) extends FunctionCall("Add") {
    val arguments : Seq[FieldExpression] = Seq(left, right)
}
*/

final case class Concat(left : FieldExpression, right : FieldExpression) extends FunctionCall("Concat") {
    override type ReturnType = String
    val arguments : Seq[FieldExpression] = Seq(left, right)
    def functionCalc : String = left.evaluate[String] + right.evaluate[String]
}


final case class IntAdd(left : FieldExpression, right : FieldExpression) extends FunctionCall("IntAdd") {
    override type ReturnType = Long
    val arguments : Seq[FieldExpression] = Seq(left, right)
    def functionCalc : Long = left.evaluate[Long] + right.evaluate[Long]
}

final case class DoubleAdd(left : FieldExpression, right : FieldExpression) extends FunctionCall("IntAdd") {
    override type ReturnType = Double
    val arguments : Seq[FieldExpression] = Seq(left, right)
    def functionCalc : Double = left.evaluate[Double] + right.evaluate[Double]
}

final case class Pow(value : FieldExpression, exponent: FieldExpression) extends FunctionCall("Pow") {
    override type ReturnType = Double
    val arguments : Seq[FieldExpression] = Seq(value, exponent)
    def functionCalc : Double = pow(value.evaluate[Double], exponent.evaluate[Double])
}