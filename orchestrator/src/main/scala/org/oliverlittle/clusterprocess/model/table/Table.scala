package org.oliverlittle.clusterprocess.model.table

abstract class Table:
    val fields : Seq[TableField]
    val fieldMap : Map[String, TableField] = fields.map(t => t.name -> t).toMap
    val transformations : Seq[TableTransformation] = Seq()

    def getData : Iterator[Seq[TableField]]
    
    def compute : Unit = {
        transformations.foreach(
            transformation => {
                transformation match {
                    case SelectTransformation(selectColumns) => getData.map(row => selectColumns.map)
                    case _ => UnsupportedOperationException("Not implemented yet.")
                }
            }
        )
    }