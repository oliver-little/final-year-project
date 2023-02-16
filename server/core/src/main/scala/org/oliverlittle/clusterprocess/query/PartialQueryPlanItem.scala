package org.oliverlittle.clusterprocess.query

import org.oliverlittle.clusterprocess.worker_query
import org.oliverlittle.clusterprocess.model.table._
import org.oliverlittle.clusterprocess.model.table.sources._
import org.oliverlittle.clusterprocess.connector.cassandra.CassandraConnector

object PartialQueryPlanItem:
    def fromProtobuf(item : worker_query.QueryPlanItem, connector : CassandraConnector) : PartialQueryPlanItem = item.item match {
        case worker_query.QueryPlanItem.Item.PrepareResult(worker_query.PrepareResult(Some(table), unknownFields)) => PartialPrepareResult(PartialTable.fromProtobuf(table))
        case worker_query.QueryPlanItem.Item.DeleteResult(worker_query.DeleteResult(Some(table), unknownFields)) => PartialDeleteResult(PartialTable.fromProtobuf(table))
        case worker_query.QueryPlanItem.Item.PreparePartition(worker_query.PreparePartition(Some(dataSource), numPartitions, unknownFields)) => PartialPreparePartition(PartialDataSource.fromProtobuf(dataSource), numPartitions)
        case worker_query.QueryPlanItem.Item.GetPartition(worker_query.GetPartition(Some(dataSource), workerURLs, unknownFields)) => PartialGetPartition(PartialDataSource.fromProtobuf(dataSource), workerURLs)
        case worker_query.QueryPlanItem.Item.DeletePartition(worker_query.DeletePartition(Some(dataSource), unknownFields)) => PartialDeletePartition(PartialDataSource.fromProtobuf(dataSource))
        case x => throw new IllegalArgumentException("Invalid QueryPlanItem received: " + x.toString)
    }

sealed trait PartialQueryPlanItem:
    val innerProtobuf : worker_query.QueryPlanItem.Item
    def protobuf : worker_query.QueryPlanItem = worker_query.QueryPlanItem(innerProtobuf)

case class PartialPrepareResult(table : PartialTable) extends PartialQueryPlanItem:
    val innerProtobuf : worker_query.QueryPlanItem.Item = worker_query.QueryPlanItem.Item.PrepareResult(worker_query.PrepareResult(Some(table.protobuf)))

case class PartialDeleteResult(table : PartialTable) extends PartialQueryPlanItem:
    val innerProtobuf : worker_query.QueryPlanItem.Item = worker_query.QueryPlanItem.Item.DeleteResult(worker_query.DeleteResult(Some(table.protobuf)))

case class PartialPreparePartition(dataSource : PartialDataSource, numPartitions : Int) extends PartialQueryPlanItem:
    val innerProtobuf : worker_query.QueryPlanItem.Item = worker_query.QueryPlanItem.Item.PreparePartition(worker_query.PreparePartition(Some(dataSource.protobuf), numPartitions))

case class PartialGetPartition(dataSource : PartialDataSource, workerURLs : Seq[String]) extends PartialQueryPlanItem:
    val innerProtobuf : worker_query.QueryPlanItem.Item = worker_query.QueryPlanItem.Item.GetPartition(worker_query.GetPartition(Some(dataSource.protobuf), workerURLs))

case class PartialDeletePartition(dataSource : PartialDataSource) extends PartialQueryPlanItem:
    val innerProtobuf : worker_query.QueryPlanItem.Item = worker_query.QueryPlanItem.Item.DeletePartition(worker_query.DeletePartition(Some(dataSource.protobuf)))