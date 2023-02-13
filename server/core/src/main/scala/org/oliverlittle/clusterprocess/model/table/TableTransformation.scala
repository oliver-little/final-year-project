package org.oliverlittle.clusterprocess.model.table

import org.oliverlittle.clusterprocess.model.field.expressions.{NamedFieldExpression, FieldExpression, AggregateExpression}
import org.oliverlittle.clusterprocess.model.field.comparisons.FieldComparison
import org.oliverlittle.clusterprocess.model.table.field._
import org.oliverlittle.clusterprocess.table_model

import scala.util.Try
import scala.util.hashing.MurmurHash3

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
	  * Combines a set of partial results into a final output
	  *
	  * @param data An iterable containing TableResults, already partially evaluated by this transformation (field data and metadata)
	  * @return A new iterable of the same form, with all TableResults combined
	  */
	def assemblePartial(data : Iterable[TableResult]) : TableResult

	/**
	 * Returns the headers that this transformation will output
	 * 
	 * @param inputHeaders The headers being input to this transformation
	 * @return A TableResultHeader instance representing the output headers.
	 */
	def outputHeaders(inputHeaders : TableResultHeader) : TableResultHeader

	/**
	  * Returns the headers that this transformation will output when producing a partial result
	  *
	  * @param inputHeaders
	  * @return
	  */
	def outputPartialHeaders(inputHeaders : TableResultHeader) : TableResultHeader

	def protobuf : table_model.Table.TableTransformation

object SelectTransformation:
	def fromProtobuf(select : table_model.Select) : SelectTransformation = SelectTransformation(select.fields.map(NamedFieldExpression.fromProtobuf(_))*)

final case class SelectTransformation(selectColumns : NamedFieldExpression*) extends TableTransformation:
	def isValid(header : TableResultHeader) : Boolean = Try(selectColumns.map(_.resolve(header))).isSuccess

	def evaluate(data : TableResult) : TableResult = {
		val resolved = selectColumns.map(_.resolve(data.header))
		return LazyTableResult(outputHeaders(data.header), data.rows.map(row => resolved.map(_.evaluate(row))))
	}

	def assemblePartial(data: Iterable[TableResult]): TableResult = data.reduce(_ ++ _)

	def outputHeaders(inputHeaders : TableResultHeader) : TableResultHeader = TableResultHeader(selectColumns.map(_.outputTableField(inputHeaders)))

	def outputPartialHeaders(inputHeaders: TableResultHeader): TableResultHeader = outputHeaders(inputHeaders)

	lazy val protobuf = table_model.Table.TableTransformation().withSelect(table_model.Select(selectColumns.map(_.protobuf)))

object FilterTransformation:
	def fromProtobuf(filter : table_model.Filter) : FilterTransformation = FilterTransformation(FieldComparison.fromProtobuf(filter))

final case class FilterTransformation(filter : FieldComparison) extends TableTransformation:
	def isValid(header : TableResultHeader) : Boolean = Try(filter.resolve(header)).isSuccess

	def evaluate(data: TableResult): TableResult = {
		val resolved = filter.resolve(data.header)
		return LazyTableResult(data.header, data.rows.filter(resolved.evaluate(_)))
	}

	def assemblePartial(data: Iterable[TableResult]): TableResult = data.reduce(_ ++ _)

	def outputHeaders(inputHeaders : TableResultHeader) : TableResultHeader = inputHeaders

	def outputPartialHeaders(inputHeaders: TableResultHeader): TableResultHeader = outputHeaders(inputHeaders)

	lazy val protobuf = table_model.Table.TableTransformation().withFilter(filter.protobuf)

final case class AggregateTransformation(aggregateColumns : AggregateExpression*) extends TableTransformation:
	def isValid(header : TableResultHeader) : Boolean = Try{aggregateColumns.map(_.resolve(header))}.isSuccess


	def evaluate(data : TableResult) : TableResult = LazyTableResult(
		// Get partial headers
		outputPartialHeaders(data.header), 
		// For each aggregate column, calculate its output, pivot all outputs to become one row, and return this
		Seq(aggregateColumns.map(_.resolve(data.header)(data.rows)).flatten)
	)

	def assemblePartial(data: Iterable[TableResult]): TableResult = {
		// Sense check: do the headers of all partial results match.
		if !data.forall(_.header == data.head.header) then throw new IllegalArgumentException("Headers of all results do not match.")
		return LazyTableResult(
			outputHeaders(data.head.header),
			// Lots of data pivoting going on here:
			// Assemble takes a header (we pick the first as we know they're all the same now)
			// We also have to give it a list of rows, but we already have a list of tables, so we append all the tables together.
			// We then flatten the results of each aggregate output to get the final table
			Seq(aggregateColumns.map(_.assemble(data.head.header)(data.map(_.rows).reduce(_ ++ _))).flatten)
		)
	}

	def outputHeaders(inputHeaders : TableResultHeader) : TableResultHeader = TableResultHeader(aggregateColumns.flatMap(_.outputTableFields(inputHeaders)))

	def outputPartialHeaders(inputHeaders: TableResultHeader): TableResultHeader = TableResultHeader(aggregateColumns.flatMap(_.outputPartialTableFields(inputHeaders)))

	lazy val protobuf = table_model.Table.TableTransformation().withAggregate(table_model.Aggregate(aggregateColumns.map(_.protobuf)))

// Calculates a Murmur3Hash on the previous result
final case class HashTransformation(uniqueColumns : FieldExpression*):
	def isValid(header : TableResultHeader) : Boolean = Try{uniqueColumns.map(_.resolve(header))}.isSuccess

	def evaluate(data : TableResult) : TableResult = {
		val resolved = uniqueColumns.map(_.resolve(data.header))

		return LazyTableResult(
			outputPartialHeaders(data.header),
			data.rows.map(row => 
				row :+ 
				Some(IntValue(MurmurHash3.unorderedHash(
					// Evaluate each column
					resolved.map(_.evaluate(row))
				)))
			)
		)
	}

	def assemblePartial(data: Iterable[TableResult]): TableResult = data.reduce(_ ++ _)

	def outputHeaders(inputHeaders : TableResultHeader) : TableResultHeader = TableResultHeader(inputHeaders.fields :+ BaseIntField("UniqueHash"))

	def outputPartialHeaders(inputHeaders: TableResultHeader): TableResultHeader = outputHeaders(inputHeaders)

abstract final case class OrderByTransformation(orderByColumns : (FieldExpression, table_model.OrderBy.OrderByType)) extends TableTransformation

abstract final case class GroupByTransformation(groupByColumns : FieldExpression*) extends TableTransformation

abstract final case class JoinTransformation(joinType : table_model.Join.JoinType, joinTable : Table) extends TableTransformation

abstract final case class WindowTransformation(windowFunctions : Seq[FieldExpression], partitionFields : Seq[FieldExpression], orderBy : OrderByTransformation) extends TableTransformation