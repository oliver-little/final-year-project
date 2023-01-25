package org.oliverlittle.clusterprocess.model.table

import org.oliverlittle.clusterprocess.model.table.field._

// Case Class does not specify what kind of Seq to use, there are two main options:
// IndexedSeq: fast random-access and length operations
// LinearSeq: fast memory allocation, and fast when using head and tail operations
case class TableResult(header : TableResultHeader, rows : Seq[Seq[TableValue]]) {

    def getCell(row : Int, col : Int) : Option[TableValue] = rows.lift(row).flatMap(_.lift(col))
    def getCell(row : Int, col : String) : Option[TableValue] = header.headerIndex.get(col).flatMap(index => rows.lift(row).flatMap(rowData => rowData.lift(index)))

    def getRow(row : Int) : Option[Seq[TableValue]] = rows.lift(row)
}

case class TableResultHeader(fields : Seq[TableField]) {
    val headerIndex : Map[String, Int] = fields.zipWithIndex.map((header, index) => header.name -> index).toMap
    val headerMap : Map[String, TableField] = fields.map(header => header.name -> header).toMap
}