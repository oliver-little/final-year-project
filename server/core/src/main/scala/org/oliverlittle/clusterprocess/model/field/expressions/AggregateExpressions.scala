package org.oliverlittle.clusterprocess.model.field.expressions

import org.oliverlittle.clusterprocess.model.table._
import org.oliverlittle.clusterprocess.model.table.field._
import org.oliverlittle.clusterprocess.table_model

import scala.math.max
import java.time.Instant

object AggregateExpression:
    def fromProtobuf(expr : table_model.AggregateExpression) : AggregateExpression = expr.aggregateType match {
        case table_model.AggregateExpression.AggregateType.MIN => Min(NamedFieldExpression.fromProtobuf(expr.expr.get)) 
        case table_model.AggregateExpression.AggregateType.MAX => Max(NamedFieldExpression.fromProtobuf(expr.expr.get)) 
        case table_model.AggregateExpression.AggregateType.SUM => Sum(NamedFieldExpression.fromProtobuf(expr.expr.get)) 
        case table_model.AggregateExpression.AggregateType.AVG => Avg(NamedFieldExpression.fromProtobuf(expr.expr.get)) 
        case table_model.AggregateExpression.AggregateType.COUNT => Count(NamedFieldExpression.fromProtobuf(expr.expr.get)) 
        case table_model.AggregateExpression.AggregateType.COUNT_DISTINCT => DistinctCount(NamedFieldExpression.fromProtobuf(expr.expr.get)) 
        case table_model.AggregateExpression.AggregateType.STRING_CONCAT => StringConcat(NamedFieldExpression.fromProtobuf(expr.expr.get), expr.arg.get) 
        case table_model.AggregateExpression.AggregateType.STRING_CONCAT_DISTINCT => DistinctStringConcat(NamedFieldExpression.fromProtobuf(expr.expr.get), expr.arg.get) 
        case x => throw new IllegalArgumentException("Cannot convert protobuf to AggregateExpression:" + x.toString)
    }

sealed trait AggregateExpression:
    val namedExpression : NamedFieldExpression
    def protobufAggregateType : table_model.AggregateExpression.AggregateType
    def optionalArg : Option[String] = None
    def protobuf : table_model.AggregateExpression = table_model.AggregateExpression(aggregateType=protobufAggregateType, expr=Some(namedExpression.protobuf), arg=optionalArg)

    def outputTableField(header : TableResultHeader) : TableField
    def resolve(header : TableResultHeader) : Iterable[Seq[Option[TableValue]]] => Option[TableValue] 

case class Max(namedExpression : NamedFieldExpression) extends AggregateExpression:
    val protobufAggregateType = table_model.AggregateExpression.AggregateType.MAX

    def outputTableField(header : TableResultHeader) : TableField = if namedExpression.expr.doesReturnType[String](header) then BaseStringField("Max_" + namedExpression.name)
        else if namedExpression.expr.doesReturnType[Long](header) then BaseIntField("Max_" + namedExpression.name)
        else if namedExpression.expr.doesReturnType[Double](header) then BaseDoubleField("Max_" + namedExpression.name)
        else if namedExpression.expr.doesReturnType[Instant](header) then BaseDateTimeField("Max_" + namedExpression.name)
        else throw new IllegalArgumentException("FieldExpression returns invalid type: " + namedExpression.expr.toString)

    def resolve(header : TableResultHeader) = {
        if !namedExpression.expr.isWellTyped(header) then throw new IllegalArgumentException("FieldExpression is not well typed, cannot resolve.")
        val resolved = namedExpression.expr.resolve(header)

        // Match on the output type of the expression, then for each row in the expression, evaluate it to its output value, remove any empty values, and calculate the maximum
        return if namedExpression.expr.doesReturnType[Long](header) then items => if items.isEmpty then None else Some(IntValue(items.map(resolved.evaluate(_).map(_.value.asInstanceOf[Long])).flatten.max))
        else if namedExpression.expr.doesReturnType[Double](header) then items => if items.isEmpty then None else Some(DoubleValue(items.map(resolved.evaluate(_).map(_.value.asInstanceOf[Double])).flatten.max))
        else if namedExpression.expr.doesReturnType[String](header) then items => if items.isEmpty then None else Some(StringValue(items.map(resolved.evaluate(_).map(_.value.asInstanceOf[String])).flatten.max))
        else if namedExpression.expr.doesReturnType[Instant](header) then items => if items.isEmpty then None else Some(DateTimeValue(items.map(resolved.evaluate(_).map(_.value.asInstanceOf[Instant])).flatten.max))
        else throw new IllegalArgumentException("Cannot calculate max for expression" + namedExpression.expr)
    }

case class Min(namedExpression : NamedFieldExpression) extends AggregateExpression:
    val protobufAggregateType = table_model.AggregateExpression.AggregateType.MIN

    def outputTableField(header : TableResultHeader) : TableField = if namedExpression.expr.doesReturnType[String](header) then BaseStringField("Min_" + namedExpression.name)
        else if namedExpression.expr.doesReturnType[Long](header) then BaseIntField("Min_" + namedExpression.name)
        else if namedExpression.expr.doesReturnType[Double](header) then BaseDoubleField("Min_" + namedExpression.name)
        else if namedExpression.expr.doesReturnType[Instant](header) then BaseDateTimeField("Min_" + namedExpression.name)
        else throw new IllegalArgumentException("FieldExpression returns invalid type: " + namedExpression.expr.toString)

    def resolve(header : TableResultHeader) = {
        if !namedExpression.expr.isWellTyped(header) then throw new IllegalArgumentException("FieldExpression is not well typed, cannot resolve.")
        val resolved = namedExpression.expr.resolve(header)

        return if namedExpression.expr.doesReturnType[Long](header) then items => if items.isEmpty then None else Some(IntValue(items.map(resolved.evaluate(_).map(_.value.asInstanceOf[Long])).flatten.min))
        else if namedExpression.expr.doesReturnType[Double](header) then items => if items.isEmpty then None else Some(DoubleValue(items.map(resolved.evaluate(_).map(_.value.asInstanceOf[Double])).flatten.min))
        else if namedExpression.expr.doesReturnType[String](header) then items => if items.isEmpty then None else Some(StringValue(items.map(resolved.evaluate(_).map(_.value.asInstanceOf[String])).flatten.min))
        else if namedExpression.expr.doesReturnType[Instant](header) then items=> if items.isEmpty then None else Some(DateTimeValue(items.map(resolved.evaluate(_).map(_.value.asInstanceOf[Instant])).flatten.min))
        else throw new IllegalArgumentException("Cannot calculate min for expression" + namedExpression.expr)
    }

case class Sum(namedExpression : NamedFieldExpression) extends AggregateExpression:
    val protobufAggregateType = table_model.AggregateExpression.AggregateType.SUM

    def outputTableField(header : TableResultHeader) : TableField = if namedExpression.expr.doesReturnType[Long](header) then BaseIntField("Sum_" + namedExpression.name)
        else if namedExpression.expr.doesReturnType[Double](header) then BaseDoubleField("Sum_" + namedExpression.name)
        else throw new IllegalArgumentException("FieldExpression returns invalid type: " + namedExpression.expr.toString)

    def resolve(header : TableResultHeader) = {
        if !namedExpression.expr.isWellTyped(header) then throw new IllegalArgumentException("FieldExpression is not well typed, cannot resolve.")
        val resolved = namedExpression.expr.resolve(header)

        return if namedExpression.expr.doesReturnType[Long](header) then items => if items.isEmpty then None else Some(IntValue(items.map(resolved.evaluate(_).map(_.value.asInstanceOf[Long])).flatten.sum))
        else if namedExpression.expr.doesReturnType[Double](header) then items => if items.isEmpty then None else Some(DoubleValue(items.map(resolved.evaluate(_).map(_.value.asInstanceOf[Long])).flatten.sum))
        else throw new IllegalArgumentException("Cannot sum expression" + namedExpression.expr)
    }

object Avg:
    def averageLong(items : Iterable[Option[Long]]) : DoubleValue = DoubleValue(items.flatten.sum / items.size)
    def averageDouble(items : Iterable[Option[Double]]) : DoubleValue = DoubleValue(items.flatten.sum / items.size)
    def averageDateTime(items : Iterable[Option[Instant]]) : DateTimeValue = DateTimeValue(Instant.ofEpochMilli((items.flatten.map(i => BigInt(i.toEpochMilli)).sum / items.size).toLong))

case class Avg(namedExpression : NamedFieldExpression) extends AggregateExpression:
    val protobufAggregateType = table_model.AggregateExpression.AggregateType.AVG

    def outputTableField(header : TableResultHeader) : TableField = if namedExpression.expr.doesReturnType[Long](header) then BaseDoubleField("Avg_" + namedExpression.name)
        else if namedExpression.expr.doesReturnType[Double](header) then BaseDoubleField("Avg_" + namedExpression.name)
        else if namedExpression.expr.doesReturnType[Instant](header) then BaseDateTimeField("Avg_" + namedExpression.name)
        else throw new IllegalArgumentException("FieldExpression returns invalid type: " + namedExpression.expr.toString)

    def resolve(header : TableResultHeader) = {
        if !namedExpression.expr.isWellTyped(header) then throw new IllegalArgumentException("FieldExpression is not well typed, cannot resolve.")
        val resolved = namedExpression.expr.resolve(header)

        return if namedExpression.expr.doesReturnType[Long](header) then items => if items.isEmpty then None else Some(Avg.averageLong(items.map(resolved.evaluate(_).map(_.value.asInstanceOf[Long]))))
            else if namedExpression.expr.doesReturnType[Double](header) then items => if items.isEmpty then None else Some(Avg.averageDouble(items.map(resolved.evaluate(_).map(_.value.asInstanceOf[Double]))))
            else if namedExpression.expr.doesReturnType[Instant](header) then items => if items.isEmpty then None else Some(Avg.averageDateTime(items.map(resolved.evaluate(_).map(_.value.asInstanceOf[Instant]))))
            else throw new IllegalArgumentException("Cannot average expression" + namedExpression.expr)
    }

case class Count(namedExpression : NamedFieldExpression) extends AggregateExpression:
    val protobufAggregateType = table_model.AggregateExpression.AggregateType.COUNT

    def outputTableField(header : TableResultHeader) = BaseIntField("Count_" + namedExpression.name)

    def resolve(header : TableResultHeader) = items => Some(IntValue(items.size))

case class DistinctCount(namedExpression : NamedFieldExpression) extends AggregateExpression:
    val protobufAggregateType = table_model.AggregateExpression.AggregateType.COUNT_DISTINCT

    def outputTableField(header : TableResultHeader) = BaseIntField("CountDistinct_" + namedExpression.name)

    def resolve(header : TableResultHeader) = {
        val resolved = namedExpression.expr.resolve(header)
        return items => Some(IntValue(items.map(resolved.evaluate(_)).toSet.size))
    }

case class StringConcat(namedExpression : NamedFieldExpression, delimiter : String) extends AggregateExpression:
    val protobufAggregateType = table_model.AggregateExpression.AggregateType.STRING_CONCAT
    override val optionalArg = Some(delimiter)

    def outputTableField(header : TableResultHeader) = BaseStringField("StringConcat_" + namedExpression.name)

    def resolve(header : TableResultHeader) = {
        if !namedExpression.expr.isWellTyped(header) then throw new IllegalArgumentException("FieldExpression is not well typed, cannot resolve.")
        val resolved = namedExpression.expr.resolve(header)

        return items => items.map(resolved.evaluate(_).map(_.value.asInstanceOf[String])).flatten.reduceOption(_ + optionalArg + _) match {
            case Some(s) => Some(StringValue(s))
            case None => Some(StringValue(""))
        }
    }

case class DistinctStringConcat(namedExpression : NamedFieldExpression, delimiter : String) extends AggregateExpression:
    val protobufAggregateType = table_model.AggregateExpression.AggregateType.STRING_CONCAT_DISTINCT
    override val optionalArg = Some(delimiter)

    def outputTableField(header : TableResultHeader) = BaseStringField("StringConcatDistinct_" + namedExpression.name)

    def resolve(header : TableResultHeader) = {
        if !namedExpression.expr.isWellTyped(header) then throw new IllegalArgumentException("FieldExpression is not well typed, cannot resolve.")
        val resolved = namedExpression.expr.resolve(header)

        return items => items.map(resolved.evaluate(_).map(_.value.asInstanceOf[String])).flatten.toSet.reduceOption(_ + optionalArg + _) match {
            case Some(s) => Some(StringValue(s))
            case None => Some(StringValue(""))
        }
    }
