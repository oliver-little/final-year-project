package org.oliverlittle.clusterprocess.model.table

import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.model.table.field._
import org.oliverlittle.clusterprocess.model.table.sources.{DataSource, PartialDataSource}
import org.oliverlittle.clusterprocess.connector.grpc.{ChannelManager, WorkerHandler}

case class Table(dataSource : DataSource, transformations : Seq[TableTransformation] = Seq()):

    lazy val protobuf : table_model.Table = table_model.Table(transformations=transformations.map(_.protobuf))

    lazy val headerList = transformations.iterator.scanLeft(dataSource.getHeaders)((inputContext, item) => item.outputHeaders(inputContext))
    lazy val outputHeaders : TableResultHeader = headerList.toSeq.last

    def addTransformation(transformation : TableTransformation) : Table = Table(dataSource, transformations :+ transformation)

    def getPartialTables(workerHandler : WorkerHandler) : Seq[(Seq[ChannelManager], Seq[PartialTable])] = dataSource.getPartitions(workerHandler).map((channels, partialSources) => (channels, partialSources.map(PartialTable(_, transformations))))

    /**
      * Returns whether this table can be evaluated with the provided data source and transformations
      */
    def isValid : Boolean = {
        if !dataSource.isValid then return false
        // This could possibly be simplified to just a forall check?
        val itemHeaderPairs = transformations.iterator.zip(headerList)
        val validStages = itemHeaderPairs.takeWhile((item, header) => item.isValid(header))
        return validStages.size == transformations.size
    }

    def assemble(partialResults : Iterable[TableResult]) : TableResult = transformations.last.assemblePartial(partialResults)

case class PartialTable(dataSource : PartialDataSource, transformations : Seq[TableTransformation] = Seq()):
    lazy val protobuf : table_model.Table = table_model.Table(transformations=transformations.map(_.protobuf))
    
    def compute(input : TableResult) : TableResult = {
        if input.header != dataSource.getHeaders then throw new IllegalArgumentException("Input data headers do not match dataSource headers")

        var data = input

        for (transformation <- transformations) {
            data = transformation.evaluate(data)
        }
        return data
    }
