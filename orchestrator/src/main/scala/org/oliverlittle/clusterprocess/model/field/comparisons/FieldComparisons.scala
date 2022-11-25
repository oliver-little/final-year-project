package org.oliverlittle.clusterprocess.model.field.comparisons

import scala.reflect.{ClassTag, classTag}
import scala.util.Try
import java.time.Instant

import org.oliverlittle.clusterprocess.table_model._
import org.oliverlittle.clusterprocess.model.field.expressions.{FieldExpression, ResolvedFieldExpression, V, FunctionCall}
import org.oliverlittle.clusterprocess.model.table.field._

// Need to add resolved/unresolved forms of FieldComparison
object FieldComparison:
    def fromProtobuf(f : Filter.FilterExpression, fieldContext : Map[String, TableField]) : FieldComparison = {
        val leftFieldExpression = FieldExpression.fromProtobuf(f.leftValue.getOrElse(throw new IllegalArgumentException("Missing required left value for FieldComparison")))
        f.filterType match {
            case u @ (Filter.FilterType.IS_NULL | Filter.FilterType.NULL | Filter.FilterType.IS_NOT_NULL | Filter.FilterType.NOT_NULL) => UnaryFieldComparison(leftFieldExpression, UnaryComparator.valueOf(f.filterType.name))
            case e @ (Filter.FilterType.EQ | Filter.FilterType.EQUAL | Filter.FilterType.NE | Filter.FilterType.NOT_EQUAL) => {
                val rightFieldExpression = FieldExpression.fromProtobuf(f.rightValue.getOrElse(throw new IllegalArgumentException("Missing required right value for EqualityFieldComparison")))
                EqualityFieldComparison(leftFieldExpression, EqualsComparator.valueOf(f.filterType.name), rightFieldExpression)
            }
            case o @ (Filter.FilterType.LESS_THAN | Filter.FilterType.LT | Filter.FilterType.LESS_THAN_EQUAL | Filter.FilterType.LTE | Filter.FilterType.GREATER_THAN | Filter.FilterType.GT | Filter.FilterType.GREATER_THAN_EQUAL | Filter.FilterType.GTE) => {
                val rightFieldExpression = FieldExpression.fromProtobuf(f.rightValue.getOrElse(throw new IllegalArgumentException("Missing required right value for OrderedFieldComparison")))
                OrderedFieldComparison(leftFieldExpression, OrderedComparator.valueOf(f.filterType.name), rightFieldExpression)
            }
            case s @ (Filter.FilterType.CONTAINS | Filter.FilterType.ICONTAINS | Filter.FilterType.STARTS_WITH | Filter.FilterType.ISTARTS_WITH | Filter.FilterType.ENDS_WITH | Filter.FilterType.IENDS_WITH) =>  {
                val string : Value = f.rightValue.getOrElse(throw new IllegalArgumentException("Missing required right value for StringFieldComparison")).expr._value.getOrElse(throw new IllegalArgumentException("Right argument for StringFieldComparison must be a string literal."))
                StringFieldComparison(leftFieldExpression, StringComparator.valueOf(f.filterType.name), V.fromProtobuf(string))
            }
            case _ => throw new IllegalArgumentException("Cannot process this filter type.")
        }
    }
sealed abstract class FieldComparison:
    lazy val protobuf : Filter.FilterExpression

    def resolve(fieldContext : Map[String, TableField]) : ResolvedFieldComparison

sealed abstract class ResolvedFieldComparison:
    def evaluate(rowContext : Map[String, TableValue]) : Boolean

final case class FieldComparisonCall(runtimeFunction : Map[String, TableValue] => Boolean) extends ResolvedFieldComparison:
    def evaluate(rowContext: Map[String, TableValue]): Boolean = runtimeFunction(rowContext)

enum UnaryComparator(val protobuf : Filter.FilterType):
    case IS_NULL extends UnaryComparator(Filter.FilterType.IS_NULL)
    case NULL extends UnaryComparator(Filter.FilterType.NULL)
    case IS_NOT_NULL extends UnaryComparator(Filter.FilterType.IS_NOT_NULL)
    case NOT_NULL extends UnaryComparator(Filter.FilterType.NOT_NULL)

enum EqualsComparator(val protobuf : Filter.FilterType):
    case EQUAL extends EqualsComparator(Filter.FilterType.EQUAL)
    case EQ extends EqualsComparator(Filter.FilterType.EQ)
    case NOT_EQUAL extends EqualsComparator(Filter.FilterType.NOT_EQUAL)
    case NE extends EqualsComparator(Filter.FilterType.NE)

enum OrderedComparator(val protobuf : Filter.FilterType):
    case LESS_THAN extends OrderedComparator(Filter.FilterType.LESS_THAN)
    case LT extends OrderedComparator(Filter.FilterType.LT)
    case LESS_THAN_EQUAL extends OrderedComparator(Filter.FilterType.LESS_THAN_EQUAL)
    case LTE extends OrderedComparator(Filter.FilterType.LTE)
    case GREATER_THAN extends OrderedComparator(Filter.FilterType.GREATER_THAN)
    case GT extends OrderedComparator(Filter.FilterType.GT)
    case GREATER_THAN_EQUAL extends OrderedComparator(Filter.FilterType.GREATER_THAN_EQUAL)
    case GTE extends OrderedComparator(Filter.FilterType.GTE)

enum StringComparator(val protobuf : Filter.FilterType):
    case CONTAINS extends StringComparator(Filter.FilterType.CONTAINS)
    case ICONTAINS extends StringComparator(Filter.FilterType.ICONTAINS)
    case STARTS_WITH extends StringComparator(Filter.FilterType.STARTS_WITH)
    case ISTARTS_WITH extends StringComparator(Filter.FilterType.ISTARTS_WITH)
    case ENDS_WITH extends StringComparator(Filter.FilterType.ENDS_WITH)
    case IENDS_WITH extends StringComparator(Filter.FilterType.IENDS_WITH)

object UnaryFieldComparison:
    def evaluate(rowContext : Map[String, TableValue], comparator : UnaryComparator, expr : ResolvedFieldExpression) : Boolean = comparator match {
        case isNull @ (UnaryComparator.IS_NULL | UnaryComparator.NULL) => expr.evaluateAny(rowContext) == null
        case isNotNull @ (UnaryComparator.IS_NOT_NULL | UnaryComparator.NOT_NULL) => expr.evaluateAny(rowContext) != null
    }

final case class UnaryFieldComparison(expression : FieldExpression, comparator : UnaryComparator) extends FieldComparison:
    lazy val protobuf: Filter.FilterExpression = Filter.FilterExpression(leftValue=Some(expression.protobuf), filterType=comparator.protobuf)

    def resolve(fieldContext: Map[String, TableField]): ResolvedFieldComparison = {
        val resolvedExpr = expression.resolve(fieldContext)
        return FieldComparisonCall((rowContext) => UnaryFieldComparison.evaluate(rowContext, comparator, resolvedExpr))
    }

object EqualityFieldComparison:
    def evaluate(rowContext : Map[String, TableValue], left: ResolvedFieldExpression, comparator : EqualsComparator, right : ResolvedFieldExpression) : Boolean = comparator match {
        case eq @ (EqualsComparator.EQ | EqualsComparator.EQUAL) => left.evaluateAny(rowContext).equals(right.evaluateAny(rowContext))
        case ne @ (EqualsComparator.NE | EqualsComparator.NOT_EQUAL) => !left.evaluateAny(rowContext).equals(right.evaluateAny(rowContext))
    }

final case class EqualityFieldComparison(left : FieldExpression, comparator : EqualsComparator, right : FieldExpression) extends FieldComparison:
    lazy val protobuf: Filter.FilterExpression = Filter.FilterExpression(leftValue=Some(left.protobuf), filterType=comparator.protobuf, rightValue=Some(right.protobuf))

    def resolve(fieldContext: Map[String, TableField]): ResolvedFieldComparison = {
        val resolvedLeft = left.resolve(fieldContext)
        val resolvedRight = right.resolve(fieldContext)
        return FieldComparisonCall((rowContext) => EqualityFieldComparison.evaluate(rowContext, resolvedLeft, comparator, resolvedRight))
    }

object OrderedFieldComparison:
    def evaluate(rowContext : Map[String, TableValue], left : ResolvedFieldExpression, comparatorFunction : Int => Boolean, right : ResolvedFieldExpression) : Boolean = comparatorFunction((left.evaluateAny(rowContext), right.evaluateAny(rowContext)) match {
        case (x : String, y : String) => x.compare(y)
        case (x : Long, y : Long)=> x.compare(y)
        case (x : Double, y : Double) => x.compare(y)
        case (x : Instant, y : Instant) => x.compareTo(y)
        case (x : Boolean, y : Boolean) => x.compare(y)
        // This case should never happen
        case (x, y) => throw new IllegalArgumentException("Provided value " + x.toString + " (type: " + x.getClass.toString + ") cannot be compared with this value " + y.toString + " (type: " + y.getClass.toString + ").")
    })
final case class OrderedFieldComparison(left : FieldExpression, comparator : OrderedComparator, right : FieldExpression)extends FieldComparison:
    lazy val protobuf: Filter.FilterExpression = Filter.FilterExpression(leftValue=Some(left.protobuf), filterType=comparator.protobuf, rightValue=Some(right.protobuf))    

    val mappedComparator = comparator match {
        case lt @ (OrderedComparator.LESS_THAN | OrderedComparator.LT) => (x : Int) => x < 0
        case lte @ (OrderedComparator.LESS_THAN_EQUAL | OrderedComparator.LTE) => (x : Int) => x <= 0
        case gt @ (OrderedComparator.GREATER_THAN | OrderedComparator.GT) => (x : Int) => x > 0
        case gte @ (OrderedComparator.GREATER_THAN_EQUAL | OrderedComparator.GTE) => (x : Int) => x >= 0
    }

    def resolve(fieldContext: Map[String, TableField]): ResolvedFieldComparison = {
        val resolvedLeft = left.resolve(fieldContext)
        val resolvedRight = right.resolve(fieldContext)
        return FieldComparisonCall((rowContext) => OrderedFieldComparison.evaluate(rowContext, resolvedLeft, mappedComparator, resolvedRight))
    }

object StringFieldComparison:
    def evaluate(rowContext : Map[String, TableValue], left: ResolvedFieldExpression, comparator : StringComparator, right : ResolvedFieldExpression) : Boolean = {
            comparator match {
                case c @ (StringComparator.CONTAINS) => left.evaluate[String](rowContext).contains(right.evaluate[String](rowContext))
                case c @ (StringComparator.ICONTAINS) => left.evaluate[String](rowContext).toLowerCase.contains(right.evaluate[String](rowContext).toLowerCase)
                case c @ (StringComparator.STARTS_WITH) => left.evaluate[String](rowContext).startsWith(right.evaluate[String](rowContext))
                case c @ (StringComparator.ISTARTS_WITH) => left.evaluate[String](rowContext).toLowerCase.startsWith(right.evaluate[String](rowContext).toLowerCase)
                case c @ (StringComparator.ENDS_WITH) => left.evaluate[String](rowContext).endsWith(right.evaluate[String](rowContext))
                case c @ (StringComparator.IENDS_WITH) => left.evaluate[String](rowContext).toLowerCase.endsWith(right.evaluate[String](rowContext).toLowerCase)
            }
        }

final case class StringFieldComparison(left : FieldExpression, comparator : StringComparator, right : V) extends FieldComparison:
    lazy val protobuf: Filter.FilterExpression = Filter.FilterExpression(leftValue=Some(left.protobuf), filterType=comparator.protobuf, rightValue=Some(right.protobuf))
    
    def resolve(fieldContext: Map[String, TableField]): ResolvedFieldComparison = {
        if !(left.doesReturnType[String](fieldContext) && right.doesReturnType[String](fieldContext)) then throw new IllegalArgumentException("Left and right expressions must return Strings in StringFieldComparison")

        val resolvedLeft = left.resolve(fieldContext)
        val resolvedRight = right.resolve(fieldContext)
        return FieldComparisonCall((rowContext) => StringFieldComparison.evaluate(rowContext, resolvedLeft, comparator, resolvedRight))
    }
