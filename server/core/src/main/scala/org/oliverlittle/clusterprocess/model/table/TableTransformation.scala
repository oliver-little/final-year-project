package org.oliverlittle.clusterprocess.model.table

import org.oliverlittle.clusterprocess.model.field.expressions.{NamedFieldExpression, FieldExpression}
import org.oliverlittle.clusterprocess.model.field.comparisons.FieldComparison
import org.oliverlittle.clusterprocess.model.table.field.{TableField, TableValue}
import org.oliverlittle.clusterprocess.table_model

import scala.util.Try

object TableTransformation:
	def fromProtobuf(table: table_model.Table) : Seq[TableTransformation] = table.transformations.map(_.instruction match {
		case x if x.isSelect => SelectTransformation.fromProtobuf(x.select.get)
		case _ => throw new IllegalArgumentException("Not implemented yet")
	})

/**
* Defines a transformation on a table
*/
sealed trait TableTransformation:
	/**
	  * Checks whether this TableTransformation can be resolved with a given header
	  * 
	  * @param header A TableResultHeader instance (field metadata)
	  * @return A boolean representing whether this transformation is valid in the provided header
	  */
	def isValid(header : TableResultHeader) : Boolean 

	/**
		* Evaluates this transformation on some provided source data
		*
		* @param data An iterable containing a map of field names to TableValues (field data and metadata)
		* @return A new iterable of the same form, with the transformation applied
		*/
	def evaluate(data : TableResult) : TableResult

	def outputHeaders(inputHeaders : TableResultHeader) : TableResultHeader

	def protobuf : table_model.Table.TableTransformation

object SelectTransformation:
	def fromProtobuf(select : table_model.Select) : SelectTransformation = SelectTransformation(select.fields.map(NamedFieldExpression.fromProtobuf(_))*)

final case class SelectTransformation(selectColumns : NamedFieldExpression*) extends TableTransformation:
	def isValid(header : TableResultHeader) : Boolean = Try(selectColumns.map(_.resolve(header))).isSuccess

	def evaluate(data : TableResult) : TableResult = {
		val resolved = selectColumns.map(_.resolve(data.header))
		return TableResult(outputHeaders(data.header), data.rows.map(row => resolved.map(_.evaluate(row))))
	}

	def outputHeaders(inputHeaders : TableResultHeader) : TableResultHeader = TableResultHeader(selectColumns.map(_.outputTableField(inputHeaders)))

	lazy val protobuf = table_model.Table.TableTransformation().withSelect(table_model.Select(selectColumns.map(_.protobuf)))

abstract final case class FilterTransformation(filters : FieldComparison*) extends TableTransformation

abstract final case class JoinTransformation(joinType : table_model.Join.JoinType, joinTable : Table) extends TableTransformation

abstract final case class GroupByTransformation(groupByColumns : FieldExpression*) extends TableTransformation

abstract final case class AggregateTransformation(aggregateColumns : FieldExpression*) extends TableTransformation

abstract final case class OrderByTransformation(orderByColumns : (FieldExpression, table_model.OrderBy.OrderByType)) extends TableTransformation

abstract final case class WindowTransformation(windowFunctions : Seq[FieldExpression], partitionFields : Seq[FieldExpression], orderBy : OrderByTransformation) extends TableTransformation