package org.oliverlittle.clusterprocess.model.table

import org.oliverlittle.clusterprocess.model.field.expressions.{NamedFieldExpression, FieldExpression, AggregateExpression}
import org.oliverlittle.clusterprocess.model.field.comparisons.FieldComparison
import org.oliverlittle.clusterprocess.model.table.field.{TableField, TableValue}
import org.oliverlittle.clusterprocess.table_model

import scala.util.Try

object TableTransformation:
	def fromProtobuf(table: table_model.Table) : Seq[TableTransformation] = table.transformations.map(_.instruction match {
		case table_model.Table.TableTransformation.Instruction.Select(expr) => SelectTransformation.fromProtobuf(expr)
		case table_model.Table.TableTransformation.Instruction.Filter(expr) => FilterTransformation.fromProtobuf(expr)
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

	/**
	 * Returns the headers that this transformation will output
	 * 
	 * @param inputHeaders The headers being input to this transformation
	 * @return A TableResultHeader instance representing the output headers.
	 */
	def outputHeaders(inputHeaders : TableResultHeader) : TableResultHeader

	def protobuf : table_model.Table.TableTransformation

	/**
	 * Function to combine partial results
	 * Default function simply compares headers to ensure they are the same, then appends rows
	 * 
	 * @param left The first partial result
	 * @param right The second partial result
	 * @return A TableResult of the combined partial results
	 */
	def partialResultAssembler(left : TableResult, right : TableResult) : TableResult = left ++ right

object SelectTransformation:
	def fromProtobuf(select : table_model.Select) : SelectTransformation = SelectTransformation(select.fields.map(NamedFieldExpression.fromProtobuf(_))*)

final case class SelectTransformation(selectColumns : NamedFieldExpression*) extends TableTransformation:
	def isValid(header : TableResultHeader) : Boolean = Try(selectColumns.map(_.resolve(header))).isSuccess

	def evaluate(data : TableResult) : TableResult = {
		val resolved = selectColumns.map(_.resolve(data.header))
		return LazyTableResult(outputHeaders(data.header), data.rows.map(row => resolved.map(_.evaluate(row))))
	}

	def outputHeaders(inputHeaders : TableResultHeader) : TableResultHeader = TableResultHeader(selectColumns.map(_.outputTableField(inputHeaders)))

	lazy val protobuf = table_model.Table.TableTransformation().withSelect(table_model.Select(selectColumns.map(_.protobuf)))

object FilterTransformation:
	def fromProtobuf(filter : table_model.Filter) : FilterTransformation = FilterTransformation(FieldComparison.fromProtobuf(filter))

final case class FilterTransformation(filter : FieldComparison) extends TableTransformation:
	def isValid(header : TableResultHeader) : Boolean = Try(filter.resolve(header)).isSuccess

	def evaluate(data: TableResult): TableResult = {
		val resolved = filter.resolve(data.header)
		return LazyTableResult(data.header, data.rows.filter(resolved.evaluate(_)))
	}

	def outputHeaders(inputHeaders : TableResultHeader) : TableResultHeader = inputHeaders

	lazy val protobuf = table_model.Table.TableTransformation().withFilter(filter.protobuf)

final case class AggregateTransformation(aggregateColumns : AggregateExpression*) extends TableTransformation:
	def isValid(header : TableResultHeader) : Boolean = Try{aggregateColumns.map(_.resolve(header))}.isSuccess

	def evaluate(data : TableResult) : TableResult = {
		val resolved = aggregateColumns.map(_.resolve(data.header))
		return LazyTableResult(outputHeaders(data.header), Seq(resolved.map(_(data.rows))))
	}

	def outputHeaders(inputHeader : TableResultHeader) : TableResultHeader = TableResultHeader(aggregateColumns.map(_.outputTableField(inputHeader)).toSeq)

	lazy val protobuf = table_model.Table.TableTransformation().withAggregate(table_model.Aggregate(aggregateColumns.map(_.protobuf)))

abstract final case class OrderByTransformation(orderByColumns : (FieldExpression, table_model.OrderBy.OrderByType)) extends TableTransformation

abstract final case class GroupByTransformation(groupByColumns : FieldExpression*) extends TableTransformation

abstract final case class JoinTransformation(joinType : table_model.Join.JoinType, joinTable : Table) extends TableTransformation

abstract final case class WindowTransformation(windowFunctions : Seq[FieldExpression], partitionFields : Seq[FieldExpression], orderBy : OrderByTransformation) extends TableTransformation