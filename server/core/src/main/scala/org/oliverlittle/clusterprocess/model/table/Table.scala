package org.oliverlittle.clusterprocess.model.table

import org.oliverlittle.clusterprocess.model.table.field._
import org.oliverlittle.clusterprocess.model.table.sources.DataSource
import org.oliverlittle.clusterprocess.table_model

case class Table(dataSource : DataSource, transformations : Seq[TableTransformation] = Seq()):

    lazy val protobuf : table_model.Table = table_model.Table(transformations=transformations.map(_.protobuf))

    def addTransformation(transformation : TableTransformation) : Table = Table(dataSource, transformations :+ transformation)

    /**
      * Returns whether this table can be evaluated with the provided data source and transformations
      */
    def isValid : Boolean = {
        val headerList = transformations.iterator.scanLeft(dataSource.getHeaders)((inputContext, item) => item.outputHeaders(inputContext))
        val itemHeaderPairs = transformations.iterator.zip(headerList)
        val validStages = itemHeaderPairs.takeWhile((item, header) => item.isValid(header))
        return validStages.size == transformations.size
    }

    def compute : TableResult = {
        var data = dataSource.getData
        for (transformation <- transformations) {
            data = transformation.evaluate(data)
        }
        return data
    }