package org.oliverlittle.clusterprocess.query

import org.oliverlittle.clusterprocess.data_source
import org.oliverlittle.clusterprocess.model.table._
import org.oliverlittle.clusterprocess.model.table.sources._

object PartialQueryPlanItem:
    def fromProtobuf(item : data_source.QueryPlanItem) : PartialQueryPlanItem = item.item match {
        case data_source.QueryPlanItem.Item.PrepareResult(dataSource, table) => PartialPrepareResult(PartialTable(PartialDataSource.fromProtobuf(dataSource), table.transformations.map(TableTransformation.fromProtobuf(_))))
        case data_source.QueryPlanItem.Item.DeleteResult(dataSource, table) => PartialDeleteResult(PartialTable(PartialDataSource.fromProtobuf(dataSource), table.transformations.map(TableTransformation.fromProtobuf(_))))
        case data_source.QueryPlanItem.Item.PreparePartition(dataSource, numPartitions) => PartialPreparePartition(PartialDataSource.fromProtobuf(dataSource), numPartitions)
        case data_source.QueryPlanItem.Item.GetPartition(dataSource, workerURLs) => PartialGetPartition(PartialDataSource.fromProtobuf(dataSource), workerURLs)
        case data_source.QueryPlanItem.Item.DeletePartition(dataSource) => PartialDeletePartition(PartialDataSource.fromProtobuf(dataSource))
    }

sealed trait PartialQueryPlanItem:
    val innerProtobuf : worker_query.QueryPlanItem.Item
    def protobuf : worker_query.QueryPlanItem = worker_query.QueryPlanItem(innerProtobuf)

case class PartialPrepareResult(table : PartialTable) extends PartialQueryPlanItem:
    val innerProtobuf : worker_query.QueryPlanItem.Item = worker_query.QueryPlanItem.Item.PrepareResult(worker_query.PrepareResult(Some(table.dataSource.protobuf), Some(table.protobuf)))

case class PartialDeleteResult(table : PartialTable) extends PartialQueryPlanItem:
    val innerProtobuf : worker_query.QueryPlanItem.Item = worker_query.QueryPlanItem.Item.DeleteResult(worker_query.DeleteResult(Some(table.dataSource.protobuf), Some(table.protobuf)))

case class PartialPreparePartition(dataSource : PartialDataSource, numPartitions : Int) extends PartialQueryPlanItem:
    val innerProtobuf : worker_query.QueryPlanItem.Item = worker_query.QueryPlanItem.Item.PreparePartition(worker_query.PreparePartition(Some(dataSource.protobuf), numPartitions))

case class PartialGetPartition(dataSource : PartialDataSource, workerURLs : Seq[String]) extends PartialQueryPlanItem:
    val innerProtobuf : worker_query.QueryPlanItem.Item = worker_query.QueryPlanItem.Item.GetPartition(worker_query.GetPartition(Some(dataSource.protobuf), workerURLs))

case class PartialDeletePartition(dataSource : PartialDataSource) extends PartialQueryPlanItem:
    val innerProtobuf : worker_query.QueryPlanItem.Item = worker_query.QueryPlanItem.Item.DeletePartition(worker_query.DeletePartition(Some(dataSource.protobuf)))