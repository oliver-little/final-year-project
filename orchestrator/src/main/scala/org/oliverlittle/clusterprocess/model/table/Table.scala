package org.oliverlittle.clusterprocess.model.table

import org.oliverlittle.clusterprocess.model.table.field._
import org.oliverlittle.clusterprocess.model.table.sources.DataSource

case class Table(dataSource : DataSource, transformations : Seq[TableTransformation] = Seq()):
    
    def addTransformation(transformation : TableTransformation) : Table = Table(dataSource, transformations :+ transformation)

    def compute : Iterable[Map[String, TableValue]] = {
        var headers = dataSource.getHeaders
        var data = dataSource.getData
        for (transformation <- transformations) {
            data = transformation.evaluate(headers, data)
        }
        return data
    }