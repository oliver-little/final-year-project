package org.oliverlittle.clusterprocess.model.field.comparisons

import scala.reflect.ClassTag
import math.Ordered.orderingToOrdered

import org.oliverlittle.clusterprocess.table_model._
import org.oliverlittle.clusterprocess.model.field.expressions.{FieldExpression, V}

sealed abstract class FieldComparison:
    lazy val protobuf : Filter.FilterExpression

    def evaluate : Boolean

enum UnaryComparator(val protobuf : Filter.FilterType):
    case IS_NULL extends UnaryComparator(Filter.FilterType.IS_NULL)
    case NULL extends UnaryComparator(Filter.FilterType.NULL)
    case IS_NOT_NULL extends UnaryComparator(Filter.FilterType.IS_NOT_NULL)
    case NOT_NULL extends UnaryComparator(Filter.FilterType.NOT_NULL)


final case class UnaryFieldComparison(expression : FieldExpression, comparator : UnaryComparator) extends FieldComparison:
    lazy val protobuf: Filter.FilterExpression = Filter.FilterExpression(leftValue=Some(expression.protobuf), filterType=comparator.protobuf)
    def evaluate : Boolean = comparator match {
        case isNull @ (UnaryComparator.IS_NULL | UnaryComparator.NULL) => expression.evaluateAny == null
        case isNotNull @ (UnaryComparator.IS_NOT_NULL | UnaryComparator.NOT_NULL) => expression.evaluateAny != null
    }

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

final case class EqualityFieldComparison[ExprType](left : FieldExpression, comparator : EqualsComparator, right : FieldExpression)(using tag : ClassTag[ExprType]) extends FieldComparison:
    lazy val protobuf: Filter.FilterExpression = Filter.FilterExpression(leftValue=Some(left.protobuf), filterType=comparator.protobuf, rightValue=Some(right.protobuf))
    def evaluate : Boolean = comparator match {
        case eq @ (EqualsComparator.EQ | EqualsComparator.EQUAL) => left.evaluate[ExprType].equals(right.evaluate[ExprType])
        case ne @ (EqualsComparator.NE | EqualsComparator.NOT_EQUAL) => !left.evaluate[ExprType].equals(right.evaluate[ExprType])
    }

final case class OrderedFieldComparison[ExprType : Ordering](left : FieldExpression, comparator : OrderedComparator, right : FieldExpression)(using tag : ClassTag[ExprType]) extends FieldComparison:
    lazy val protobuf: Filter.FilterExpression = Filter.FilterExpression(leftValue=Some(left.protobuf), filterType=comparator.protobuf, rightValue=Some(right.protobuf))
    def evaluate : Boolean = comparator match {
        case lt @ (OrderedComparator.LESS_THAN | OrderedComparator.LT) => left.evaluate[ExprType] < right.evaluate[ExprType]
        case lte @ (OrderedComparator.LESS_THAN_EQUAL | OrderedComparator.LTE) => left.evaluate[ExprType] <= right.evaluate[ExprType]
        case gt @ (OrderedComparator.GREATER_THAN | OrderedComparator.GT) => left.evaluate[ExprType] > right.evaluate[ExprType]
        case gte @ (OrderedComparator.GREATER_THAN_EQUAL | OrderedComparator.GTE) => left.evaluate[ExprType] >= right.evaluate[ExprType]
    }

final case class StringFieldComparison(left : FieldExpression, comparator : StringComparator, right : V) extends FieldComparison:
    lazy val protobuf: Filter.FilterExpression = Filter.FilterExpression(leftValue=Some(left.protobuf), filterType=comparator.protobuf, rightValue=Some(right.protobuf))
    
    def evaluate : Boolean = comparator match {
        case c @ (StringComparator.CONTAINS) => left.evaluate[String].contains(right.getValueAsType[String])
        case c @ (StringComparator.ICONTAINS) => left.evaluate[String].toLowerCase.contains(right.getValueAsType[String].toLowerCase)
        case c @ (StringComparator.STARTS_WITH) => left.evaluate[String].startsWith(right.getValueAsType[String])
        case c @ (StringComparator.ISTARTS_WITH) => left.evaluate[String].toLowerCase.startsWith(right.getValueAsType[String].toLowerCase)
        case c @ (StringComparator.ENDS_WITH) => left.evaluate[String].endsWith(right.getValueAsType[String])
        case c @ (StringComparator.IENDS_WITH) => left.evaluate[String].toLowerCase.endsWith(right.getValueAsType[String].toLowerCase)
    }