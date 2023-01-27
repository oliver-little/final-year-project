package org.oliverlittle.clusterprocess.model.table

import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.model.table.field._

object TableResult:
    def fromProtobuf(tableResult : table_model.TableResult) = TableResult(
        TableResultHeader.fromProtobuf(tableResult.headers.get), 
        tableResult.rows.map(_.values.map(TableValue.fromProtobuf(_)))
    )

// Case Class does not specify what kind of Seq to use, there are two main options:
// IndexedSeq: fast random-access and length operations
// LinearSeq: fast memory allocation, and fast when using head and tail operations
case class TableResult(header : TableResultHeader, rows : Seq[Seq[Option[TableValue]]]) {

    /**
      * TableResult protobuf, will generate the entire table at once, use header.protobuf and rowIteratorProtobuf to generate lazily 
      */
    lazy val protobuf : table_model.TableResult = table_model.TableResult(headers=Some(header.protobuf), rows=rowIteratorProtobuf.toSeq)
    def rowIteratorProtobuf : Iterator[table_model.TableResultRow] = 
        rows.iterator.map(row => 
            table_model.TableResultRow(values = row.map(_.protobuf))
        )

    def getCell(row : Int, col : Int) : Option[TableValue] = rows.lift(row).flatMap(_.lift(col)).flatten
    def getCell(row : Int, col : String) : Option[TableValue] = header.headerIndex.get(col).flatMap(index => rows.lift(row).flatMap(rowData => rowData.lift(index).flatten))
    def getRow(row : Int) : Option[Seq[Option[TableValue]]] = rows.lift(row)
    
    def addRows(newRows : Seq[Seq[Option[TableValue]]]) = copy(header, rows ++ newRows)
}

object TableResultHeader:
    def fromProtobuf(header : table_model.TableResultHeader) = TableResultHeader(header.fields map {TableField.fromProtobuf(_)})

case class TableResultHeader(fields : Seq[TableField]) {
    val headerIndex : Map[String, Int] = fields.zipWithIndex.map((header, index) => header.name -> index).toMap
    val headerMap : Map[String, TableField] = fields.map(header => header.name -> header).toMap
    
    lazy val protobuf : table_model.TableResultHeader = table_model.TableResultHeader(fields = fields.map(_.protobuf))
}