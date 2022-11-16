package org.oliverlittle.clusterprocess.model.table

import org.oliverlittle.clusterprocess.model.field.expressions.{NamedFieldExpression, FieldExpression}
import org.oliverlittle.clusterprocess.model.field.comparisons.FieldComparison
import org.oliverlittle.clusterprocess.table_model._
import org.oliverlittle.clusterprocess.model.table.field.{TableField, TableValue}

/**
  * Defines a transformation on a table
  */
sealed trait TableTransformation:
    /**
      * Evaluates this transformation on some provided source data
      *
      * @param data An iterable containing a map of field names to TableValues (field data and metadata)
      * @return A new iterable of the same form, with the transformation applied
      */
    def evaluate(data : Iterable[Map[String, TableValue]]) : Iterable[Map[String, TableValue]]

final case class SelectTransformation(selectColumns : NamedFieldExpression*) extends TableTransformation:
    def evaluate(data : Iterable[Map[String, TableValue]]) : Iterable[Map[String, TableValue]] = data.map(row => selectColumns.map(col => col.name -> col.evaluateAny(row)).toMap)

abstract final case class FilterTransformation(filters : FieldComparison*) extends TableTransformation

abstract final case class JoinTransformation(joinType : Join.JoinType, joinTable : Table) extends TableTransformation

abstract final case class GroupByTransformation(groupByColumns : FieldExpression*) extends TableTransformation

abstract final case class AggregateTransformation(aggregateColumns : FieldExpression*) extends TableTransformation

abstract final case class OrderByTransformation(orderByColumns : (FieldExpression, OrderBy.OrderByType)) extends TableTransformation

abstract final case class WindowTransformation(windowFunctions : Seq[FieldExpression], partitionFields : Seq[FieldExpression], orderBy : OrderByTransformation) extends TableTransformation