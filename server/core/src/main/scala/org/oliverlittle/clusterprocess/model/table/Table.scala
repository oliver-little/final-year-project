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
        val fieldContextList = transformations.iterator.scanLeft(dataSource.getHeaders)((inputContext, item) => item.outputFieldContext(inputContext))
        val itemFieldContextPairs = transformations.iterator.zip(fieldContextList)
        val validStages = itemFieldContextPairs.takeWhile((item, fieldContext) => item.isValid(fieldContext))
        return validStages.size == transformations.size
    }

    def compute : Iterable[Map[String, TableValue]] = {
        var headers = dataSource.getHeaders
        var data = dataSource.getData
        for (transformation <- transformations) {
            data = transformation.evaluate(headers, data)
        }
        return data
    }