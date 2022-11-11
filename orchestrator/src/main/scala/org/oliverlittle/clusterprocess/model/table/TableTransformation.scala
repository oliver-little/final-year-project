package org.oliverlittle.clusterprocess.model.table

import org.oliverlittle.clusterprocess.model.field.expressions.FieldExpression
import org.oliverlittle.clusterprocess.model.field.comparisons.FieldComparison
import org.oliverlittle.clusterprocess.table_model._

sealed trait TableTransformation

final case class SelectTransformation(selectColumns : Seq[FieldExpression]) extends TableTransformation

final case class FilterTransformation(filters : Seq[FieldComparison]) extends TableTransformation

final case class JoinTransformation(joinType : Join.JoinType, joinTable : Table) extends TableTransformation

final case class GroupByTransformation(groupByColumns : Seq[FieldExpression]) extends TableTransformation

final case class AggregateTransformation(aggregateColumns : Seq[FieldExpression]) extends TableTransformation

final case class OrderByTransformation(orderByColumns : (FieldExpression, OrderBy.OrderByType)) extends TableTransformation

final case class WindowTransformation(windowFunctions : Seq[FieldExpression], partitionFields : Seq[FieldExpression], orderBy : OrderByTransformation) extends TableTransformation