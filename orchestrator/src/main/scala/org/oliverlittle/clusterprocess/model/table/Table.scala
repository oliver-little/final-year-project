package org.oliverlittle.clusterprocess.model.table

trait Table:
    val fields : Seq[TableField]
    val fieldMap : Map[String, TableField] = fields.map(t => t.name -> t).toMap