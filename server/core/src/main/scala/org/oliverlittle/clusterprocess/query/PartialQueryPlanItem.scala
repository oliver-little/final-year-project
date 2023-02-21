package org.oliverlittle.clusterprocess.query

import org.oliverlittle.clusterprocess.worker_query
import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.model.table._
import org.oliverlittle.clusterprocess.model.table.sources._
import org.oliverlittle.clusterprocess.connector.grpc.ChannelManager

import akka.actor.typed.{ActorRef, ActorSystem}
import akka.actor.typed.scaladsl.AskPattern._
import akka.Done
import akka.pattern.StatusReply
import akka.util.Timeout

import java.net.InetSocketAddress
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Success, Failure}
import scala.concurrent.ExecutionContext

object PartialQueryPlanItem:
    def fromProtobuf(item : worker_query.QueryPlanItem) : PartialQueryPlanItem = item.item match {
        case worker_query.QueryPlanItem.Item.PrepareResult(worker_query.PrepareResult(Some(table), unknownFields)) => PartialPrepareResult(PartialTable.fromProtobuf(table))
        case worker_query.QueryPlanItem.Item.DeleteResult(worker_query.DeleteResult(Some(table), unknownFields)) => PartialDeleteResult(Table.fromProtobuf(table))
        case worker_query.QueryPlanItem.Item.PrepareHashes(worker_query.PrepareHashes(Some(dataSource), numPartitions, unknownFields)) => PartialPrepareHashes(DependentDataSource.fromProtobuf(dataSource), numPartitions)
        case worker_query.QueryPlanItem.Item.DeletePreparedHashes(worker_query.DeletePreparedHashes(Some(dataSource), numPartitions, unknownFields)) => PartialDeletePreparedHashes(DependentDataSource.fromProtobuf(dataSource), numPartitions)
        case worker_query.QueryPlanItem.Item.GetPartition(worker_query.GetPartition(Some(dataSource), workerURLs, unknownFields)) => PartialGetPartition(PartialDataSource.fromProtobuf(dataSource), workerURLs.map(address => new InetSocketAddress(address.host, address.port)))
        case worker_query.QueryPlanItem.Item.DeletePartition(worker_query.DeletePartition(Some(dataSource), unknownFields)) => PartialDeletePartition(DataSource.fromProtobuf(dataSource))
        case x => throw new IllegalArgumentException("Invalid QueryPlanItem received: " + x.toString)
    }

sealed trait PartialQueryPlanItem:
    val innerProtobuf : worker_query.QueryPlanItem.Item
    def protobuf : worker_query.QueryPlanItem = worker_query.QueryPlanItem(innerProtobuf)

    def execute(store: ActorRef[TableStore.TableStoreEvent])(using t : Timeout)(using system : ActorSystem[_])(using ec : ExecutionContext = system.executionContext) : Future[worker_query.ProcessQueryPlanItemResult]

case class PartialPrepareResult(table : PartialTable) extends PartialQueryPlanItem:
    val innerProtobuf : worker_query.QueryPlanItem.Item = worker_query.QueryPlanItem.Item.PrepareResult(worker_query.PrepareResult(Some(table.protobuf)))

    def execute(store: ActorRef[TableStore.TableStoreEvent])(using t : Timeout)(using system : ActorSystem[_])(using ec : ExecutionContext = system.executionContext) : Future[worker_query.ProcessQueryPlanItemResult] = 
        // Get the partition from the table store and match on the result
        store.ask[Option[TableResult]](ref => TableStore.GetPartition(table.dataSource, ref)).flatMap {
            case Some(t) => store.ask(ref => TableStore.AddResult(table, table.compute(t), ref)) // If we got a partition, try to compute and store the result
            case None => throw new IllegalArgumentException("Missing partial data source for table") // Otherwise, throw an error
        }.map {
            case StatusReply.Success(_) => worker_query.ProcessQueryPlanItemResult(true) // If that operation was successful, return successful state
            case StatusReply.Error(e) => throw e
        }

case class PartialDeleteResult(table : Table) extends PartialQueryPlanItem:
    val innerProtobuf : worker_query.QueryPlanItem.Item = worker_query.QueryPlanItem.Item.DeleteResult(worker_query.DeleteResult(Some(table.protobuf)))

    def execute(store: ActorRef[TableStore.TableStoreEvent])(using t : Timeout)(using system : ActorSystem[_])(using ec : ExecutionContext = system.executionContext) : Future[worker_query.ProcessQueryPlanItemResult] = {
        store ! TableStore.DeleteResult(table)
        return Future.successful(worker_query.ProcessQueryPlanItemResult(true))
    }

case class PartialPrepareHashes(dataSource : DependentDataSource, numPartitions : Int) extends PartialQueryPlanItem:
    val innerProtobuf : worker_query.QueryPlanItem.Item = worker_query.QueryPlanItem.Item.PrepareHashes(worker_query.PrepareHashes(Some(dataSource.protobuf), numPartitions))

    def execute(store: ActorRef[TableStore.TableStoreEvent])(using t : Timeout)(using system : ActorSystem[_])(using ec : ExecutionContext = system.executionContext) : Future[worker_query.ProcessQueryPlanItemResult] =
        store.ask[StatusReply[Done]](ref => TableStore.HashPartition(dataSource, numPartitions, ref)).map {
            case StatusReply.Success(_) => worker_query.ProcessQueryPlanItemResult(true)
            case StatusReply.Error(e) => worker_query.ProcessQueryPlanItemResult(false)
        }

case class PartialDeletePreparedHashes(dataSource : DataSource, numPartitions : Int) extends PartialQueryPlanItem:
    val innerProtobuf: worker_query.QueryPlanItem.Item = worker_query.QueryPlanItem.Item.DeletePreparedHashes(worker_query.DeletePreparedHashes(Some(dataSource.protobuf), numPartitions))

    def execute(store: ActorRef[TableStore.TableStoreEvent])(using t : Timeout)(using system : ActorSystem[_])(using ec : ExecutionContext = system.executionContext) : Future[worker_query.ProcessQueryPlanItemResult] = {
        store ! TableStore.DeleteHash(dataSource, numPartitions)
        return Future.successful(worker_query.ProcessQueryPlanItemResult(true))
    }

case class PartialGetPartition(dataSource : PartialDataSource, workerURLs : Seq[InetSocketAddress]) extends PartialQueryPlanItem:
    val innerProtobuf : worker_query.QueryPlanItem.Item = worker_query.QueryPlanItem.Item.GetPartition(worker_query.GetPartition(Some(dataSource.protobuf), workerURLs.map(address => table_model.InetSocketAddress(address.getHostName, address.getPort))))

    def execute(store: ActorRef[TableStore.TableStoreEvent])(using t : Timeout)(using system : ActorSystem[_])(using ec : ExecutionContext = system.executionContext) : Future[worker_query.ProcessQueryPlanItemResult] = 
        dataSource.getPartialData( // Get the partial data from the data source
            store,  // Requires the TableStore reference
            workerURLs.map(address => ChannelManager(address.getHostName, address.getPort)) // Also requires references to the other channels to be able to collate data
        ).flatMap {
            // Once the partial data is ready, store the partition in the TableStore
            result => store.ask[StatusReply[Done]](ref => TableStore.AddPartition(dataSource, result, ref)) 
        }.map {
            case StatusReply.Success(_) => worker_query.ProcessQueryPlanItemResult(true) // If that operation was successful, return successful state
            case StatusReply.Error(e) => throw e
        } 

case class PartialDeletePartition(dataSource : DataSource) extends PartialQueryPlanItem:
    val innerProtobuf : worker_query.QueryPlanItem.Item = worker_query.QueryPlanItem.Item.DeletePartition(worker_query.DeletePartition(Some(dataSource.protobuf)))

    def execute(store: ActorRef[TableStore.TableStoreEvent])(using t : Timeout)(using system : ActorSystem[_])(using ec : ExecutionContext = system.executionContext) : Future[worker_query.ProcessQueryPlanItemResult] = {
        store ! TableStore.DeletePartition(dataSource)
        return Future.successful(worker_query.ProcessQueryPlanItemResult(true))
    }