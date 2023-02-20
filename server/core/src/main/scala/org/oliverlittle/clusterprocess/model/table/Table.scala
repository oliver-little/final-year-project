package org.oliverlittle.clusterprocess.model.table

import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.model.table.field._
import org.oliverlittle.clusterprocess.model.table.sources.{DataSource, PartialDataSource}
import org.oliverlittle.clusterprocess.connector.grpc.{ChannelManager, WorkerHandler}
import org.oliverlittle.clusterprocess.connector.cassandra.CassandraConnector
import org.oliverlittle.clusterprocess.query._

object Table:
    def fromProtobuf(table : table_model.Table) : Table = Table(DataSource.fromProtobuf(table.dataSource.get), TableTransformation.fromProtobuf(table.transformations))

case class Table(dataSource : DataSource, transformations : Seq[TableTransformation] = Seq()):

    lazy val protobuf : table_model.Table = table_model.Table(Some(dataSource.protobuf), transformations.map(_.protobuf))

    lazy val headerList = transformations.iterator.scanLeft(dataSource.getHeaders)((inputContext, item) => item.outputHeaders(inputContext))
    lazy val outputHeaders : TableResultHeader = headerList.toSeq.last

    lazy val empty : TableResult = EvaluatedTableResult(outputHeaders, Seq())

    def addTransformation(transformation : TableTransformation) : Table = Table(dataSource, transformations :+ transformation)

    def getPartialTables(workerHandler : WorkerHandler) : Seq[(Seq[ChannelManager], Seq[PartialTable])] = dataSource.getPartitions(workerHandler).map((channels, partialSources) => (channels, partialSources.map(PartialTable(_, transformations))))

    /**
	  * Generates the high level query plan to create this table in the TableStore
	  *
	  * @return
	  */
    def getQueryPlan : Seq[QueryPlanItem] = dataSource.getQueryPlan ++ Seq(PrepareResult(this)) ++ dataSource.getCleanupQueryPlan
    
    /**
	  * Generates the high level query plan to create this table in the TableStore
	  *
	  * @return
	  */
    def getCleanupQueryPlan : Seq[QueryPlanItem] = Seq(DeleteResult(this))

    def withPartialDataSource(partialDataSource : PartialDataSource) : PartialTable = if partialDataSource.parent != dataSource then throw new IllegalArgumentException("PartialDataSource parent DataSource does not match this table's DataSource")
        else PartialTable(partialDataSource, transformations)

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

object PartialTable:
    def fromProtobuf(table : table_model.PartialTable) : PartialTable = PartialTable(PartialDataSource.fromProtobuf(table.dataSource.get), TableTransformation.fromProtobuf(table.transformations))

case class PartialTable(dataSource : PartialDataSource, transformations : Seq[TableTransformation] = Seq()):
    lazy val parent = Table(dataSource.parent, transformations)
    lazy val protobuf : table_model.PartialTable = table_model.PartialTable(Some(dataSource.protobuf), transformations.map(_.protobuf))

    def compute(input : TableResult) : TableResult = {
        if input.header != dataSource.getHeaders then throw new IllegalArgumentException("Input data headers do not match dataSource headers")

        var data = input

        for (transformation <- transformations) {
            data = transformation.evaluate(data)
        }
        return data
    }
