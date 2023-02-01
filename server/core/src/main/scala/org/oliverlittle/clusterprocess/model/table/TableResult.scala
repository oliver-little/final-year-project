package org.oliverlittle.clusterprocess.model.table

import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.model.table.field._

object TableResult:
    def fromProtobuf(tableResult : table_model.TableResult) = EvaluatedTableResult(
        TableResultHeader.fromProtobuf(tableResult.headers.get), 
        tableResult.rows.map(_.values.map(TableValue.fromProtobuf(_)))
    )

trait TableResult:
    val header : TableResultHeader
    val rows : Iterable[Seq[Option[TableValue]]] 
    /**
      * TableResult protobuf, will generate the entire table at once, use header.protobuf and rowIteratorProtobuf to generate lazily 
      */
    lazy val protobuf : table_model.TableResult = table_model.TableResult(headers=Some(header.protobuf), rows=rowsProtobuf.toSeq)
    def rowsProtobuf : Iterator[table_model.TableResultRow] = 
        rows.iterator.map(row => 
            table_model.TableResultRow(values = row.map(_.protobuf))
        )

    /**
      * Helper function to fully evaluate the TableResult
      *
      * @return The EvaluatedTableResult
      */
    def evaluate : EvaluatedTableResult = EvaluatedTableResult(header, rows.toSeq)

    def ++(that : TableResult) : TableResult

/**
  * Base, lazy version of TableResult to defer row evaluation until needed
  *
  * @param header
  * @param rows
  */
case class LazyTableResult(header : TableResultHeader, rows : Iterable[Seq[Option[TableValue]]]) extends TableResult:
    override def ++(that: TableResult): TableResult = {
        if header != that.header then throw new IllegalArgumentException("TableResult headers do not match:" + header.toString + " and " + that.header.toString)

        return copy(rows = rows ++ that.rows)
    }

/**
  * Evaluated version of table result where rows have been calculated and are stored in memory
  * This doesn't specify which kind of Seq to use, there are two main options:
  * - IndexedSeq: fast random-access and length operations
  * - LinearSeq: fast memory allocation, and fast when using head and tail operations
  *
  * @param header
  * @param rows
  */
case class EvaluatedTableResult(header : TableResultHeader, rows : Seq[Seq[Option[TableValue]]]) extends TableResult:
    override def ++(that: TableResult): TableResult = {
        if header != that.header then throw new IllegalArgumentException("TableResult headers do not match:" + header.toString + " and " + that.header.toString)

        return copy(rows = rows ++ that.rows)
    }

    def getCell(row : Int, col : Int) : Option[TableValue] = rows.lift(row).flatMap(_.lift(col)).flatten
    def getCell(row : Int, col : String) : Option[TableValue] = header.headerIndex.get(col).flatMap(index => rows.lift(row).flatMap(rowData => rowData.lift(index).flatten))
    def getRow(row : Int) : Option[Seq[Option[TableValue]]] = rows.lift(row)

object TableResultHeader:
    def fromProtobuf(header : table_model.TableResultHeader) = TableResultHeader(header.fields map {TableField.fromProtobuf(_)})

case class TableResultHeader(fields : Seq[TableField]) {
    val headerIndex : Map[String, Int] = fields.zipWithIndex.map((header, index) => header.name -> index).toMap
    val headerMap : Map[String, TableField] = fields.map(header => header.name -> header).toMap
    
    lazy val protobuf : table_model.TableResultHeader = table_model.TableResultHeader(fields = fields.map(_.protobuf))
}