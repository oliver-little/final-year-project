package org.oliverlittle.clusterprocess.model.field.comparisons

import scala.reflect.{ClassTag, classTag}
import scala.util.Try
import java.time.Instant

import org.oliverlittle.clusterprocess.table_model._
import org.oliverlittle.clusterprocess.model.field.expressions.{FieldExpression, ResolvedFieldExpression, V, FunctionCall}
import org.oliverlittle.clusterprocess.model.table.TableResultHeader
import org.oliverlittle.clusterprocess.model.table.field._

object FieldComparison:
    def fromProtobuf(f : Filter) : FieldComparison = f.expression match {
        case Filter.Expression.Filter(expr) => FieldComparison.filterExpressionFromProtobuf(expr)
        case Filter.Expression.CombinedFilter(expr) => FieldComparison.combinedFilterExpressionFromProtobuf(expr)
        case Filter.Expression.Empty => throw new IllegalArgumentException("Cannot process this filter type")
    }
        
    def filterExpressionFromProtobuf(f : FilterExpression) : FieldComparison = {
        val leftFieldExpression = FieldExpression.fromProtobuf(f.leftValue.getOrElse(throw new IllegalArgumentException("Missing required left value for FieldComparison")))
        f.filterType match {
            case u @ (FilterExpression.FilterType.IS_NULL | FilterExpression.FilterType.NULL | FilterExpression.FilterType.IS_NOT_NULL | FilterExpression.FilterType.NOT_NULL) => UnaryFieldComparison(leftFieldExpression, UnaryComparator.valueOf(f.filterType.name))
            case e @ (FilterExpression.FilterType.EQ | FilterExpression.FilterType.EQUAL | FilterExpression.FilterType.NE | FilterExpression.FilterType.NOT_EQUAL) => {
                val rightFieldExpression = FieldExpression.fromProtobuf(f.rightValue.getOrElse(throw new IllegalArgumentException("Missing required right value for EqualityFieldComparison")))
                EqualityFieldComparison(leftFieldExpression, EqualsComparator.valueOf(f.filterType.name), rightFieldExpression)
            }
            case o @ (FilterExpression.FilterType.LESS_THAN | FilterExpression.FilterType.LT | FilterExpression.FilterType.LESS_THAN_EQUAL | FilterExpression.FilterType.LTE | FilterExpression.FilterType.GREATER_THAN | FilterExpression.FilterType.GT | FilterExpression.FilterType.GREATER_THAN_EQUAL | FilterExpression.FilterType.GTE) => {
                val rightFieldExpression = FieldExpression.fromProtobuf(f.rightValue.getOrElse(throw new IllegalArgumentException("Missing required right value for OrderedFieldComparison")))
                OrderedFieldComparison(leftFieldExpression, OrderedComparator.valueOf(f.filterType.name), rightFieldExpression)
            }
            case s @ (FilterExpression.FilterType.CONTAINS | FilterExpression.FilterType.ICONTAINS | FilterExpression.FilterType.STARTS_WITH | FilterExpression.FilterType.ISTARTS_WITH | FilterExpression.FilterType.ENDS_WITH | FilterExpression.FilterType.IENDS_WITH) =>  {
                val string : Value = f.rightValue.getOrElse(throw new IllegalArgumentException("Missing required right value for StringFieldComparison")).expr._value.getOrElse(throw new IllegalArgumentException("Right argument for StringFieldComparison must be a string literal."))
                StringFieldComparison(leftFieldExpression, StringComparator.valueOf(f.filterType.name), V.fromProtobuf(string))
            }
            case _ => throw new IllegalArgumentException("Cannot process this filter type.")
        }
    }

    def combinedFilterExpressionFromProtobuf(f : CombinedFilterExpression) : CombinedFieldComparison = CombinedFieldComparison(
        FieldComparison.fromProtobuf(f.leftExpression.get),
        BooleanOperator.valueOf(f.operator.name),
        FieldComparison.fromProtobuf(f.rightExpression.get)
    )

sealed abstract class FieldComparison:
    lazy val protobuf : Filter

    def resolve(header : TableResultHeader) : ResolvedFieldComparison

sealed abstract class ResolvedFieldComparison:
    def evaluate(row : Seq[Option[TableValue]]) : Boolean

enum BooleanOperator(val protobuf : CombinedFilterExpression.BooleanOperator):
    case AND extends BooleanOperator(CombinedFilterExpression.BooleanOperator.AND)
    case OR extends BooleanOperator(CombinedFilterExpression.BooleanOperator.OR)

final case class CombinedFieldComparison(left : FieldComparison, operator : BooleanOperator, right : FieldComparison) extends FieldComparison:
    def resolve(header: TableResultHeader): ResolvedFieldComparison = ResolvedCombinedFieldComparison(left.resolve(header), operator, right.resolve(header))

    lazy val protobuf : Filter = Filter().withCombinedFilter(CombinedFilterExpression(Some(left.protobuf), operator.protobuf, Some(right.protobuf)))

final case class ResolvedCombinedFieldComparison(left : ResolvedFieldComparison, operator : BooleanOperator, right : ResolvedFieldComparison) extends ResolvedFieldComparison:
    def evaluate(row: Seq[Option[TableValue]]): Boolean = operator match {
        case BooleanOperator.AND => left.evaluate(row) && right.evaluate(row)
        case BooleanOperator.OR => left.evaluate(row) || right.evaluate(row)
    }

final case class FieldComparisonCall(runtimeFunction : Seq[Option[TableValue]] => Boolean) extends ResolvedFieldComparison:
    def evaluate(row : Seq[Option[TableValue]]): Boolean = runtimeFunction(row)

enum UnaryComparator(val protobuf : FilterExpression.FilterType):
    case IS_NULL extends UnaryComparator(FilterExpression.FilterType.IS_NULL)
    case NULL extends UnaryComparator(FilterExpression.FilterType.NULL)
    case IS_NOT_NULL extends UnaryComparator(FilterExpression.FilterType.IS_NOT_NULL)
    case NOT_NULL extends UnaryComparator(FilterExpression.FilterType.NOT_NULL)

enum EqualsComparator(val protobuf : FilterExpression.FilterType):
    case EQUAL extends EqualsComparator(FilterExpression.FilterType.EQUAL)
    case EQ extends EqualsComparator(FilterExpression.FilterType.EQ)
    case NOT_EQUAL extends EqualsComparator(FilterExpression.FilterType.NOT_EQUAL)
    case NE extends EqualsComparator(FilterExpression.FilterType.NE)

enum OrderedComparator(val protobuf : FilterExpression.FilterType):
    case LESS_THAN extends OrderedComparator(FilterExpression.FilterType.LESS_THAN)
    case LT extends OrderedComparator(FilterExpression.FilterType.LT)
    case LESS_THAN_EQUAL extends OrderedComparator(FilterExpression.FilterType.LESS_THAN_EQUAL)
    case LTE extends OrderedComparator(FilterExpression.FilterType.LTE)
    case GREATER_THAN extends OrderedComparator(FilterExpression.FilterType.GREATER_THAN)
    case GT extends OrderedComparator(FilterExpression.FilterType.GT)
    case GREATER_THAN_EQUAL extends OrderedComparator(FilterExpression.FilterType.GREATER_THAN_EQUAL)
    case GTE extends OrderedComparator(FilterExpression.FilterType.GTE)

enum StringComparator(val protobuf : FilterExpression.FilterType):
    case CONTAINS extends StringComparator(FilterExpression.FilterType.CONTAINS)
    case ICONTAINS extends StringComparator(FilterExpression.FilterType.ICONTAINS)
    case STARTS_WITH extends StringComparator(FilterExpression.FilterType.STARTS_WITH)
    case ISTARTS_WITH extends StringComparator(FilterExpression.FilterType.ISTARTS_WITH)
    case ENDS_WITH extends StringComparator(FilterExpression.FilterType.ENDS_WITH)
    case IENDS_WITH extends StringComparator(FilterExpression.FilterType.IENDS_WITH)

object UnaryFieldComparison:
    def evaluate(header : TableResultHeader, row : Seq[Option[TableValue]], comparator : UnaryComparator, expr : ResolvedFieldExpression) : Boolean = comparator match {
        case isNull @ (UnaryComparator.IS_NULL | UnaryComparator.NULL) => expr.evaluate(row).isEmpty
        case isNotNull @ (UnaryComparator.IS_NOT_NULL | UnaryComparator.NOT_NULL) => expr.evaluate(row).isDefined
    }

final case class UnaryFieldComparison(expression : FieldExpression, comparator : UnaryComparator) extends FieldComparison:
    lazy val protobuf: Filter = Filter().withFilter(FilterExpression(leftValue=Some(expression.protobuf), filterType=comparator.protobuf))

    def resolve(header : TableResultHeader): ResolvedFieldComparison = {
        val resolvedExpr = expression.resolve(header)
        return FieldComparisonCall((row) => UnaryFieldComparison.evaluate(header, row, comparator, resolvedExpr))
    }

object EqualityFieldComparison:
    def evaluate(header : TableResultHeader, row : Seq[Option[TableValue]], left: ResolvedFieldExpression, comparator : EqualsComparator, right : ResolvedFieldExpression) : Boolean = comparator match {
        case eq @ (EqualsComparator.EQ | EqualsComparator.EQUAL) => left.evaluate(row).equals(right.evaluate(row))
        case ne @ (EqualsComparator.NE | EqualsComparator.NOT_EQUAL) => !left.evaluate(row).equals(right.evaluate(row))
    }

final case class EqualityFieldComparison(left : FieldExpression, comparator : EqualsComparator, right : FieldExpression) extends FieldComparison:
    lazy val protobuf: Filter = Filter().withFilter(FilterExpression(leftValue=Some(left.protobuf), filterType=comparator.protobuf, rightValue=Some(right.protobuf)))

    def resolve(header : TableResultHeader): ResolvedFieldComparison = {
        val resolvedLeft = left.resolve(header)
        val resolvedRight = right.resolve(header)
        return FieldComparisonCall((row) => EqualityFieldComparison.evaluate(header, row, resolvedLeft, comparator, resolvedRight))
    }

object OrderedFieldComparison:
    def evaluate(header : TableResultHeader, row : Seq[Option[TableValue]], left : ResolvedFieldExpression, comparatorFunction : Int => Boolean, right : ResolvedFieldExpression) : Boolean = {
        val leftOption : Option[TableValue] = left.evaluate(row)
        val rightOption : Option[TableValue] = right.evaluate(row)
        
        if leftOption.isEmpty || rightOption.isEmpty then return false

        val diff = (leftOption.get.value, rightOption.get.value) match {
            case (x : String, y : String) => x.compare(y)
            case (x : Long, y : Long)=> x.compare(y)
            case (x : Double, y : Double) => x.compare(y)
            case (x : Instant, y : Instant) => x.compareTo(y)
            case (x : Boolean, y : Boolean) => x.compare(y)
            // This case should never happen
            case (x, y) => throw new IllegalArgumentException("Provided value " + x.toString + " (type: " + x.getClass.toString + ") cannot be compared with this value " + y.toString + " (type: " + y.getClass.toString + ").")
        }

        return comparatorFunction(diff)
    }

final case class OrderedFieldComparison(left : FieldExpression, comparator : OrderedComparator, right : FieldExpression)extends FieldComparison:
    lazy val protobuf: Filter = Filter().withFilter(FilterExpression(leftValue=Some(left.protobuf), filterType=comparator.protobuf, rightValue=Some(right.protobuf)))   

    val mappedComparator = comparator match {
        case lt @ (OrderedComparator.LESS_THAN | OrderedComparator.LT) => (x : Int) => x < 0
        case lte @ (OrderedComparator.LESS_THAN_EQUAL | OrderedComparator.LTE) => (x : Int) => x <= 0
        case gt @ (OrderedComparator.GREATER_THAN | OrderedComparator.GT) => (x : Int) => x > 0
        case gte @ (OrderedComparator.GREATER_THAN_EQUAL | OrderedComparator.GTE) => (x : Int) => x >= 0
    }

    def resolve(header : TableResultHeader): ResolvedFieldComparison = {
        val resolvedLeft = left.resolve(header)
        val resolvedRight = right.resolve(header)
        return FieldComparisonCall((row) => OrderedFieldComparison.evaluate(header, row, resolvedLeft, mappedComparator, resolvedRight))
    }

object StringFieldComparison:
    def evaluate(header : TableResultHeader, row : Seq[Option[TableValue]], left: ResolvedFieldExpression, comparator : StringComparator, right : ResolvedFieldExpression) : Boolean = {
            val leftOption : Option[TableValue] = left.evaluate(row)
            val rightOption : Option[TableValue] = right.evaluate(row)
            
            if leftOption.isEmpty || rightOption.isEmpty then return false

            val leftString = leftOption.get.value.asInstanceOf[String]
            val rightString = rightOption.get.value.asInstanceOf[String]

            return comparator match {
                case c @ (StringComparator.CONTAINS) => leftString.contains(rightString)
                case c @ (StringComparator.ICONTAINS) => leftString.toLowerCase.contains(rightString.toLowerCase)
                case c @ (StringComparator.STARTS_WITH) => leftString.startsWith(rightString)
                case c @ (StringComparator.ISTARTS_WITH) => leftString.toLowerCase.startsWith(rightString.toLowerCase)
                case c @ (StringComparator.ENDS_WITH) => leftString.endsWith(rightString)
                case c @ (StringComparator.IENDS_WITH) => leftString.toLowerCase.endsWith(rightString.toLowerCase)
            }
        }

final case class StringFieldComparison(left : FieldExpression, comparator : StringComparator, right : V) extends FieldComparison:
    lazy val protobuf: Filter = Filter().withFilter(FilterExpression(leftValue=Some(left.protobuf), filterType=comparator.protobuf, rightValue=Some(right.protobuf)))
    
    def resolve(header : TableResultHeader): ResolvedFieldComparison = {
        if !(left.doesReturnType[String](header) && right.doesReturnType[String](header)) then throw new IllegalArgumentException("Left and right expressions must return Strings in StringFieldComparison")

        val resolvedLeft = left.resolve(header)
        val resolvedRight = right.resolve(header)
        return FieldComparisonCall((row) => StringFieldComparison.evaluate(header, row, resolvedLeft, comparator, resolvedRight))
    }
