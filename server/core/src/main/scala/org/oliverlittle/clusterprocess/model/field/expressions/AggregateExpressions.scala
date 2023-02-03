package org.oliverlittle.clusterprocess.model.field.expressions

import org.oliverlittle.clusterprocess.model.table.field._
import org.oliverlittle.clusterprocess.table_model

object AggregateExpression:
    def fromProtobuf(expr : table_model.AggregateExpression) : AggregateExpression = expr.aggregateType match {
        case table_model.AggregateExpression.AggregateType.MIN => Min(FieldExpression.fromProtobuf(expr.expr.get)) 
        case table_model.AggregateExpression.AggregateType.MAX => Max(FieldExpression.fromProtobuf(expr.expr.get)) 
        case table_model.AggregateExpression.AggregateType.SUM => Sum(FieldExpression.fromProtobuf(expr.expr.get)) 
        case table_model.AggregateExpression.AggregateType.AVG => Avg(FieldExpression.fromProtobuf(expr.expr.get)) 
        case table_model.AggregateExpression.AggregateType.COUNT => Count(FieldExpression.fromProtobuf(expr.expr.get)) 
        case table_model.AggregateExpression.AggregateType.COUNT_DISTINCT => DistinctCount(FieldExpression.fromProtobuf(expr.expr.get)) 
        case table_model.AggregateExpression.AggregateType.STRING_CONCAT => StringConcat(FieldExpression.fromProtobuf(expr.expr.get)) 
        case table_model.AggregateExpression.AggregateType.STRING_CONCAT_DISTINCT => DistinctStringConcat(FieldExpression.fromProtobuf(expr.expr.get)) 
        case x => throw new IllegalArgumentException("Cannot convert protobuf to AggregateExpression:" + x.toString)
    }

sealed trait AggregateExpression:
    val expr : FieldExpression
    def protobufAggregateType : table_model.AggregateExpression.AggregateType
    def optionalArg : Option[String] = None
    def protobuf : table_model.AggregateExpression = table_model.AggregateExpression(aggregateType=protobufAggregateType, expr=Some(expr.protobuf), arg=optionalArg)

    //def reduce(left : Option[TableValue], right : Option[TableValue]) : Option[TableValue]

case class Max(expr : FieldExpression) extends AggregateExpression:
    val protobufAggregateType = table_model.AggregateExpression.AggregateType.MAX

    //def reduce(left : Option[TableValue], right : Option[TableValue]) : Option[TableValue]

case class Min(expr : FieldExpression) extends AggregateExpression:
    val protobufAggregateType = table_model.AggregateExpression.AggregateType.MIN

case class Sum(expr : FieldExpression) extends AggregateExpression:
    val protobufAggregateType = table_model.AggregateExpression.AggregateType.SUM

case class Avg(expr : FieldExpression) extends AggregateExpression:
    val protobufAggregateType = table_model.AggregateExpression.AggregateType.AVG

case class Count(expr : FieldExpression) extends AggregateExpression:
    val protobufAggregateType = table_model.AggregateExpression.AggregateType.COUNT

case class DistinctCount(expr : FieldExpression) extends AggregateExpression:
    val protobufAggregateType = table_model.AggregateExpression.AggregateType.COUNT_DISTINCT

case class StringConcat(expr : FieldExpression) extends AggregateExpression:
    val protobufAggregateType = table_model.AggregateExpression.AggregateType.STRING_CONCAT

case class DistinctStringConcat(expr : FieldExpression) extends AggregateExpression:
    val protobufAggregateType = table_model.AggregateExpression.AggregateType.STRING_CONCAT_DISTINCT