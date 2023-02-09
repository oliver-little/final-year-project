package org.oliverlittle.clusterprocess.model.field.expressions

import org.oliverlittle.clusterprocess.model.table._
import org.oliverlittle.clusterprocess.model.table.field._
import org.oliverlittle.clusterprocess.table_model

import scala.math.max
import scala.util.{Try, Success, Failure}
import java.time.Instant
import BigDecimal._
import java.util.regex.Pattern

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
    val name : String
    def protobufAggregateType : table_model.AggregateExpression.AggregateType
    def optionalArg : Option[String] = None
    def protobuf : table_model.AggregateExpression = table_model.AggregateExpression(aggregateType=protobufAggregateType, expr=Some(namedExpression.protobuf), arg=optionalArg)

    /**
      * Lists the fields this aggregate function outputs after final assembly
      * 
      * This has a default implementation because for most aggregates, we simply want to apply the same aggregate function again on the partial results
      *
      * @param header
      * @return
      */
    def outputTableFields(header : TableResultHeader) : Seq[TableField] = Seq(header.headerMap(name))
    
    /**
      * Lists the fields that this aggregate function outputs when it is a partial result
      *
      * @param header
      * @return
      */
    def outputPartialTableFields(header : TableResultHeader) : Seq[TableField]

    /**
     * Partially resolves this aggregate expression, ready to be combined at the orchestrator or host worker.
     * 
     * Should output the same fields as outputPartialTableFields
     */
    def resolve(header : TableResultHeader)(items : Iterable[Seq[Option[TableValue]]]) : Seq[Option[TableValue]]
    /**
     * Assembles a list of partial results into a final result
     * 
     * Should output the same fields as outputTableFields
     */
    def assemble(header : TableResultHeader)(items : Iterable[Seq[Option[TableValue]]]) : Seq[Option[TableValue]]

case class Max(namedExpression : NamedFieldExpression) extends AggregateExpression:
    val protobufAggregateType = table_model.AggregateExpression.AggregateType.MAX

    val name : String = "Max_" + namedExpression.name

    def outputPartialTableFields(header : TableResultHeader) : Seq[TableField] = if namedExpression.expr.doesReturnType[String](header) then Seq(BaseStringField(name))
        else if namedExpression.expr.doesReturnType[Long](header) then Seq(BaseIntField(name))
        else if namedExpression.expr.doesReturnType[Double](header) then Seq(BaseDoubleField(name))
        else if namedExpression.expr.doesReturnType[Instant](header) then Seq(BaseDateTimeField(name))
        else throw new IllegalArgumentException("FieldExpression returns invalid type: " + namedExpression.expr.toString)

    def resolve(header : TableResultHeader)(items : Iterable[Seq[Option[TableValue]]]) : Seq[Option[TableValue]] = resolveWithExpr(namedExpression.expr, header, items)

    def assemble(header : TableResultHeader)(items : Iterable[Seq[Option[TableValue]]]) : Seq[Option[TableValue]] = 
        if header.headerIndex.contains(name) then resolveWithExpr(F(name), header, items)
        else throw new IllegalArgumentException("TableResultHeader does not contain aggregate name " + name)

    /**
      * Resolves with a given fieldExpression and header (essentially a static function)
      *
      * @param fieldExpr
      * @param header
      * @return
      */
    private def resolveWithExpr(fieldExpr : FieldExpression, header : TableResultHeader, items : Iterable[Seq[Option[TableValue]]]) : Seq[Option[TableValue]] = {
        if !fieldExpr.isWellTyped(header) then throw new IllegalArgumentException("FieldExpression is not well typed, cannot resolve.")
        val resolved = fieldExpr.resolve(header)

        // Match on the output type of the expression, then for each row in the expression, evaluate it to its output value, remove any empty values, and calculate the maximum
        return if fieldExpr.doesReturnType[Long](header) then aggregateIterableMax[IntValue](resolved)(items)
        else if fieldExpr.doesReturnType[Double](header) then aggregateIterableMax[DoubleValue](resolved)(items)
        else if fieldExpr.doesReturnType[String](header) then aggregateIterableMax[StringValue](resolved)(items)
        else if fieldExpr.doesReturnType[Instant](header) then aggregateIterableMax[DateTimeValue](resolved)(items)
        else throw new IllegalArgumentException("Cannot calculate max for expression" + fieldExpr)
    }

    /**
      * Generic function to Max an ordering
      *
      * @param resolvedExpr
      * @param items
      * @param o
      * @return
      */
    private def aggregateIterableMax[T <: TableValue](resolvedExpr : ResolvedFieldExpression)(items : Iterable[Seq[Option[TableValue]]])(using o : Ordering[T]) : Seq[Option[TableValue]] = 
        Try{items.map(resolvedExpr.evaluate(_)).flatten.asInstanceOf[Iterable[T]].max} match {
            case Success(value) => Seq(Some(value))
            case Failure(_) => Seq(None)
        }

case class Min(namedExpression : NamedFieldExpression) extends AggregateExpression:
    val protobufAggregateType = table_model.AggregateExpression.AggregateType.MIN

    val name : String = "Min_" + namedExpression.name

    def outputPartialTableFields(header : TableResultHeader) : Seq[TableField] = if namedExpression.expr.doesReturnType[String](header) then Seq(BaseStringField(name))
        else if namedExpression.expr.doesReturnType[Long](header) then Seq(BaseIntField(name))
        else if namedExpression.expr.doesReturnType[Double](header) then Seq(BaseDoubleField(name))
        else if namedExpression.expr.doesReturnType[Instant](header) then Seq(BaseDateTimeField(name))
        else throw new IllegalArgumentException("FieldExpression returns invalid type: " + namedExpression.expr.toString)

    def resolve(header : TableResultHeader)(items : Iterable[Seq[Option[TableValue]]]) : Seq[Option[TableValue]] = resolveWithExpr(namedExpression.expr, header, items)

    def assemble(header : TableResultHeader)(items : Iterable[Seq[Option[TableValue]]]) : Seq[Option[TableValue]] = 
        if header.headerIndex.contains(name) then resolveWithExpr(F(name), header, items)
        else throw new IllegalArgumentException("TableResultHeader does not contain aggregate name " + name)

    private def resolveWithExpr(fieldExpr : FieldExpression, header : TableResultHeader, items : Iterable[Seq[Option[TableValue]]]) : Seq[Option[TableValue]] = {
        if !fieldExpr.isWellTyped(header) then throw new IllegalArgumentException("FieldExpression is not well typed, cannot resolve.")
        val resolved = fieldExpr.resolve(header)

        // Match on the output type of the expression, then for each row in the expression, evaluate it to its output value, remove any empty values, and calculate the maximum
        return if fieldExpr.doesReturnType[Long](header) then aggregateIterableMin[IntValue](resolved)(items)
        else if fieldExpr.doesReturnType[Double](header) then aggregateIterableMin[DoubleValue](resolved)(items)
        else if fieldExpr.doesReturnType[String](header) then aggregateIterableMin[StringValue](resolved)(items)
        else if fieldExpr.doesReturnType[Instant](header) then aggregateIterableMin[DateTimeValue](resolved)(items)
        else throw new IllegalArgumentException("Cannot calculate max for expression" + fieldExpr)
    }

    /**
     * Generic function to Min an ordering
     */
    private def aggregateIterableMin[T <: TableValue](resolvedExpr : ResolvedFieldExpression)(items : Iterable[Seq[Option[TableValue]]])(using o : Ordering[T]) : Seq[Option[TableValue]] = 
        Try{items.map(resolvedExpr.evaluate(_)).flatten.asInstanceOf[Iterable[T]].min} match {
            case Success(value) => Seq(Some(value))
            case Failure(_) => Seq(None)
        }

case class Sum(namedExpression : NamedFieldExpression) extends AggregateExpression:
    val protobufAggregateType = table_model.AggregateExpression.AggregateType.SUM

    val name : String = "Sum_" + namedExpression.name

    def outputPartialTableFields(header : TableResultHeader) : Seq[TableField] = if namedExpression.expr.doesReturnType[Long](header) then Seq(BaseIntField(name))
        else if namedExpression.expr.doesReturnType[Double](header) then Seq(BaseDoubleField(name))
        else throw new IllegalArgumentException("FieldExpression returns invalid type: " + namedExpression.expr.toString)

    def resolve(header : TableResultHeader)(items : Iterable[Seq[Option[TableValue]]]) : Seq[Option[TableValue]] = resolveWithExpr(namedExpression.expr, header, items)

    def assemble(header : TableResultHeader)(items : Iterable[Seq[Option[TableValue]]]) : Seq[Option[TableValue]] = 
        if header.headerIndex.contains(name) then resolveWithExpr(F(name), header, items)
        else throw new IllegalArgumentException("TableResultHeader does not contain aggregate name " + name)

    private def resolveWithExpr(fieldExpr : FieldExpression, header : TableResultHeader, items : Iterable[Seq[Option[TableValue]]]) : Seq[Option[TableValue]] = {
        if !fieldExpr.isWellTyped(header) then throw new IllegalArgumentException("FieldExpression is not well typed, cannot resolve.")
        val resolved = fieldExpr.resolve(header)

        if items.isEmpty then return Seq(None)

        return if fieldExpr.doesReturnType[Long](header) then Seq(Some(IntValue(items.map(resolved.evaluate(_).map(_.value.asInstanceOf[Long])).flatten.sum)))
        else if fieldExpr.doesReturnType[Double](header) then Seq(Some(DoubleValue(items.map(resolved.evaluate(_).map(_.value.asInstanceOf[Double])).flatten.sum)))
        else throw new IllegalArgumentException("Cannot sum expression" + fieldExpr)
    }

object Avg:
    def partialAverageLong(items : Iterable[Long]) : Seq[Option[TableValue]] = Seq(Some(IntValue(items.sum)), Some(IntValue(items.size)))
    def partialAverageDouble(items : Iterable[Double]) : Seq[Option[TableValue]] = Seq(Some(DoubleValue(items.sum)), Some(IntValue(items.size)))
    def partialAverageDateTime(items : Iterable[Instant]) : Seq[Option[TableValue]] = Seq(Some(StringValue(items.map(i => BigInt(i.toEpochMilli)).sum.toString)), Some(IntValue(items.size)))

    def averageLong(items : Iterable[(IntValue, IntValue)]) : Option[DoubleValue] = Try{
            Some(
                DoubleValue(
                    (items.map((sum, size) => 
                        BigDecimal(sum.value)).sum / 
                        items.map((sum, size) => BigDecimal(size.value)).sum)
                    .toDouble
                )
            )
        }.getOrElse(None)

    def averageDouble(items : Iterable[(DoubleValue, IntValue)]) : Option[DoubleValue] = Try{
            Some(
                DoubleValue(
                    (items.map((sum, size) => 
                        BigDecimal(sum.value)).sum / 
                        items.map((sum, size) => BigDecimal(size.value)).sum)
                    .toDouble
                )
            )
        }.getOrElse(None)
    def averageDateTime(items : Iterable[(StringValue, IntValue)]) : Option[DateTimeValue] = Try{
            Some(
                DateTimeValue(
                    Instant.ofEpochMilli(
                        (items.map((sum, size) => BigDecimal(sum.value)).sum / 
                        items.map((sum, size) => BigDecimal(size.value)).sum)
                        .setScale(0, RoundingMode.HALF_UP).toLong
                    )
                )
            )
        }.getOrElse(None)

case class Avg(namedExpression : NamedFieldExpression) extends AggregateExpression:
    val protobufAggregateType = table_model.AggregateExpression.AggregateType.AVG

    val name : String = "Avg_" + namedExpression.name
    val outputSumName : String = "AvgSum_" + namedExpression.name
    val outputCountName : String = "AvgCount_" + namedExpression.name

    /**
      * Final output for Avg is the calculated avg from sum and count
      *
      * @param header
      * @return
      */
    override def outputTableFields(header : TableResultHeader) : Seq[TableField] = 
        if header.headerIndex.contains(outputCountName) && header.headerMap.contains(outputSumName) then header.headerMap(outputSumName) match {
            case _ : IntField => Seq(BaseIntField(name))
            case _ : DoubleField => Seq(BaseDoubleField(name))
            case _ : DateTimeField => Seq(BaseDateTimeField(name))
            case x => throw new IllegalArgumentException("Cannot average given type " + x.toString)
        }
        else throw new IllegalArgumentException("Missing required  column information to calculate average.")

    /**
      * Partial table fields for avg is a sum and a count
      *
      * @param header
      * @return
      */
    def outputPartialTableFields(header : TableResultHeader) : Seq[TableField] = if namedExpression.expr.doesReturnType[Long](header) then Seq(BaseIntField(outputSumName), BaseIntField(outputCountName))
        else if namedExpression.expr.doesReturnType[Double](header) then Seq(BaseDoubleField(outputSumName), BaseIntField(outputCountName))
        else if namedExpression.expr.doesReturnType[Instant](header) then Seq(BaseDateTimeField(outputSumName), BaseIntField(outputCountName))
        else throw new IllegalArgumentException("FieldExpression returns invalid type: " + namedExpression.expr.toString)

    def resolve(header : TableResultHeader)(items : Iterable[Seq[Option[TableValue]]]) : Seq[Option[TableValue]] = {
        if !namedExpression.expr.isWellTyped(header) then throw new IllegalArgumentException("FieldExpression is not well typed, cannot resolve.")
        val resolved = namedExpression.expr.resolve(header)

        return if namedExpression.expr.doesReturnType[Long](header) then Avg.partialAverageLong(items.map(resolved.evaluate(_).map(_.value.asInstanceOf[Long])).flatten)
            else if namedExpression.expr.doesReturnType[Double](header) then Avg.partialAverageDouble(items.map(resolved.evaluate(_).map(_.value.asInstanceOf[Double])).flatten)
            else if namedExpression.expr.doesReturnType[Instant](header) then Avg.partialAverageDateTime(items.map(resolved.evaluate(_).map(_.value.asInstanceOf[Instant])).flatten)
            else throw new IllegalArgumentException("Cannot average expression" + namedExpression.expr)
    }

    def assemble(header : TableResultHeader)(items : Iterable[Seq[Option[TableValue]]]) : Seq[Option[TableValue]] = {
        val sumIndex = header.headerIndex(outputSumName)
        val countIndex = header.headerIndex(outputCountName)

        return header.headerMap(outputSumName) match {
            case _ : IntField => Seq(Avg.averageLong(items.map(i => (i(sumIndex).getOrElse(IntValue(0)).asInstanceOf[IntValue], i(countIndex).getOrElse(IntValue(0)).asInstanceOf[IntValue]))))
            case _ : DoubleField => Seq(Avg.averageDouble(items.map(i => (i(sumIndex).getOrElse(DoubleValue(0)).asInstanceOf[DoubleValue], i(countIndex).getOrElse(IntValue(0)).asInstanceOf[IntValue]))))
            case _ : DateTimeField => Seq(Avg.averageDateTime(items.map(i => (i(sumIndex).getOrElse(StringValue("0")).asInstanceOf[StringValue], i(countIndex).getOrElse(IntValue(0)).asInstanceOf[IntValue]))))
            case x => throw new IllegalArgumentException("Cannot average expression with sum type: " + x.toString)
        }
    }

case class Count(namedExpression : NamedFieldExpression) extends AggregateExpression:
    val protobufAggregateType = table_model.AggregateExpression.AggregateType.COUNT

    val name : String = "Count_" + namedExpression.name

    def outputPartialTableFields(header : TableResultHeader) = Seq(BaseIntField(name))

    def resolve(header : TableResultHeader)(items : Iterable[Seq[Option[TableValue]]]) : Seq[Option[TableValue]] = {
        if !namedExpression.expr.isWellTyped(header) then throw new IllegalArgumentException("FieldExpression is not well typed, cannot resolve.")
        val resolved = namedExpression.expr.resolve(header)
        val itemCount = items.map(resolved.evaluate(_)).flatten.size
        return Seq(Some(IntValue(itemCount)))
    }

    def assemble(header: TableResultHeader)(items: Iterable[Seq[Option[TableValue]]]): Seq[Option[TableValue]] = {
        val resolved = F(name).resolve(header)

        // For each partial result, get the integer out, sum them all and return
        return Seq(Some(IntValue(items.map(resolved.evaluate(_).map(_.value.asInstanceOf[Long])).flatten.sum)))
    }

case class DistinctCount(namedExpression : NamedFieldExpression) extends AggregateExpression:
    val protobufAggregateType = table_model.AggregateExpression.AggregateType.COUNT_DISTINCT

    // Attempting to choose a nonsense delimiter that shouldn't come up in real text
    // Need a better solution than this
    val delimiter : String = ">^<"

    val name : String = "CountDistinct_" + namedExpression.name

    override def outputTableFields(header : TableResultHeader) = header.headerMap(name) match {
        case _ : StringField => Seq(BaseIntField(name))
        case x => throw new IllegalArgumentException("Invalid field type for partial DistinctCount" + x.toString)
    }

    def outputPartialTableFields(header : TableResultHeader) = Seq(BaseStringField(name))

    def resolve(header : TableResultHeader)(items : Iterable[Seq[Option[TableValue]]]) : Seq[Option[TableValue]] = {
        if !namedExpression.expr.isWellTyped(header) then throw new IllegalArgumentException("FieldExpression is not well typed, cannot resolve.")
        val resolved = namedExpression.expr.resolve(header)
        val uniqueItems = items.map(resolved.evaluate(_)).flatten.map(_.value.toString).toSet
        return Seq(Some(StringValue(uniqueItems.reduceOption(_ + delimiter + _).getOrElse(""))))
    }

    def assemble(header: TableResultHeader)(items: Iterable[Seq[Option[TableValue]]]): Seq[Option[TableValue]] = {
        val resolved = F(name).resolve(header)

        // Parse all delimited strings to get unique items
        val uniqueItems = items.map(resolved.evaluate(_)).flatten.map(_.asInstanceOf[StringValue].value.split(Pattern.quote(delimiter))).flatten.toSet

        return Seq(Some(IntValue(uniqueItems.size)))
    }

case class StringConcat(namedExpression : NamedFieldExpression, delimiter : String) extends AggregateExpression:
    val protobufAggregateType = table_model.AggregateExpression.AggregateType.STRING_CONCAT
    override val optionalArg = Some(delimiter)

    val name : String = "StringConcat_" + namedExpression.name

    def outputPartialTableFields(header : TableResultHeader) = Seq(BaseStringField(name))

    def resolve(header : TableResultHeader)(items : Iterable[Seq[Option[TableValue]]]) : Seq[Option[TableValue]] = resolveWithExpr(namedExpression.expr, header, items)

    def assemble(header: TableResultHeader)(items: Iterable[Seq[Option[TableValue]]]): Seq[Option[TableValue]] = resolveWithExpr(F(name), header, items)

    private def resolveWithExpr(fieldExpr : FieldExpression, header : TableResultHeader, items : Iterable[Seq[Option[TableValue]]]) : Seq[Option[TableValue]] = {
        if !fieldExpr.isWellTyped(header) then throw new IllegalArgumentException("FieldExpression is not well typed, cannot resolve.")
        val resolved = fieldExpr.resolve(header)

        return items.map(resolved.evaluate(_).map(_.value.asInstanceOf[String])).flatten.reduceOption(_ + delimiter + _) match {
            case Some(s) => Seq(Some(StringValue(s)))
            case None => Seq(None)
        }
    }

case class DistinctStringConcat(namedExpression : NamedFieldExpression, delimiter : String) extends AggregateExpression:
    val protobufAggregateType = table_model.AggregateExpression.AggregateType.STRING_CONCAT_DISTINCT
    override val optionalArg = Some(delimiter)

    val name : String = "StringConcatDistinct_" + namedExpression.name

    def outputPartialTableFields(header: TableResultHeader): Seq[TableField] = Seq(BaseStringField(name))

    def resolve(header : TableResultHeader)(items : Iterable[Seq[Option[TableValue]]]) : Seq[Option[TableValue]] = {
        if !namedExpression.expr.isWellTyped(header) then throw new IllegalArgumentException("FieldExpression is not well typed, cannot resolve.")
        val resolved = namedExpression.expr.resolve(header)

        return items.map(resolved.evaluate(_).map(_.value.asInstanceOf[String])).flatten.toSet.reduceOption(_ + delimiter + _) match {
            case Some(s) => Seq(Some(StringValue(s)))
            case None => Seq(None)
        }
    }

    def assemble(header: TableResultHeader)(items: Iterable[Seq[Option[TableValue]]]): Seq[Option[TableValue]] = {
        val resolved = F(name).resolve(header)
        return items.map(resolved.evaluate(_)).flatten.flatMap(_.value.asInstanceOf[String].split(Pattern.quote(delimiter))).toSet.reduceOption(_ + delimiter + _) match {
            case Some(s) => Seq(Some(StringValue(s)))
            case None => Seq(None)
        }
    }
