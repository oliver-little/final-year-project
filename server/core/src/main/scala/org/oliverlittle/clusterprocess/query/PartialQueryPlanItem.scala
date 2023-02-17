package org.oliverlittle.clusterprocess.query

import org.oliverlittle.clusterprocess.worker_query
import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.model.table._
import org.oliverlittle.clusterprocess.model.table.sources._
import org.oliverlittle.clusterprocess.connector.grpc.ChannelManager

import akka.actor.typed.{ActorRef, ActorSystem}
import akka.actor.typed.scaladsl.AskPattern._
import akka.Done
import akka.actor.Status
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
        case worker_query.QueryPlanItem.Item.PreparePartition(worker_query.PreparePartition(Some(dataSource), numPartitions, unknownFields)) => PartialPreparePartition(DependentDataSource.fromProtobuf(dataSource), numPartitions)
        case worker_query.QueryPlanItem.Item.GetPartition(worker_query.GetPartition(Some(dataSource), workerURLs, unknownFields)) => PartialGetPartition(PartialDataSource.fromProtobuf(dataSource), workerURLs.map(address => new InetSocketAddress(address.host, address.port)))
        case worker_query.QueryPlanItem.Item.DeletePartition(worker_query.DeletePartition(Some(dataSource), unknownFields)) => PartialDeletePartition(DataSource.fromProtobuf(dataSource))
        case x => throw new IllegalArgumentException("Invalid QueryPlanItem received: " + x.toString)
    }

sealed trait PartialQueryPlanItem:
    given Timeout = 3.seconds

    val innerProtobuf : worker_query.QueryPlanItem.Item
    def protobuf : worker_query.QueryPlanItem = worker_query.QueryPlanItem(innerProtobuf)

    def execute(store: ActorRef[TableStore.TableStoreEvent])(using t : Timeout)(using system : ActorSystem[_])(using ec : ExecutionContext = system.executionContext) : Future[worker_query.ProcessQueryPlanItemResult]

case class PartialPrepareResult(table : PartialTable) extends PartialQueryPlanItem:
    val innerProtobuf : worker_query.QueryPlanItem.Item = worker_query.QueryPlanItem.Item.PrepareResult(worker_query.PrepareResult(Some(table.protobuf)))

    def execute(store: ActorRef[TableStore.TableStoreEvent])(using t : Timeout)(using system : ActorSystem[_])(using ec : ExecutionContext = system.executionContext) : Future[worker_query.ProcessQueryPlanItemResult] = 
        // Get the partition from the table store and match on the result
        store.ask(ref => TableStore.GetPartition(table.dataSource, ref)).map {
            // If the result exists, compute the output partition table, and store it
            case Success(Some(partition : TableResult)) => 
                val output = table.compute(partition)
                store.ask(ref => TableStore.AddResult(table, output, ref)).onComplete {
                    case Success(_) => worker_query.ProcessQueryPlanItemResult(true)
                    case Failure(e) => worker_query.ProcessQueryPlanItemResult(false) // Add proper error handling here
                }
                worker_query.ProcessQueryPlanItemResult(true)
            // Otherwise, throw an error
            case _ => throw new IllegalArgumentException("Missing partial data source for table")
        }

case class PartialDeleteResult(table : Table) extends PartialQueryPlanItem:
    val innerProtobuf : worker_query.QueryPlanItem.Item = worker_query.QueryPlanItem.Item.DeleteResult(worker_query.DeleteResult(Some(table.protobuf)))

    def execute(store: ActorRef[TableStore.TableStoreEvent])(using t : Timeout)(using system : ActorSystem[_])(using ec : ExecutionContext = system.executionContext) : Future[worker_query.ProcessQueryPlanItemResult] = {
        store ! TableStore.DeleteResult(table)
        return Future.successful(worker_query.ProcessQueryPlanItemResult(true))
    }
case class PartialPreparePartition(dataSource : DependentDataSource, numPartitions : Int) extends PartialQueryPlanItem:
    val innerProtobuf : worker_query.QueryPlanItem.Item = worker_query.QueryPlanItem.Item.PreparePartition(worker_query.PreparePartition(Some(dataSource.protobuf), numPartitions))

    def execute(store: ActorRef[TableStore.TableStoreEvent])(using t : Timeout)(using system : ActorSystem[_])(using ec : ExecutionContext = system.executionContext) : Future[worker_query.ProcessQueryPlanItemResult] =
        store.ask(ref => TableStore.HashPartition(dataSource, numPartitions, ref)).map {
            case Success(_) => worker_query.ProcessQueryPlanItemResult(true)
            case Failure(e) => worker_query.ProcessQueryPlanItemResult(false)
        }

case class PartialDeletePreparedPartition(dataSource : DataSource, numPartitions : Int) extends PartialQueryPlanItem:
    val innerProtobuf: worker_query.QueryPlanItem.Item = worker_query.QueryPlanItem.Item.DeletePreparedPartition(worker_query.DeletePreparedPartition(Some(dataSource.protobuf), numPartitions))

    def execute(store: ActorRef[TableStore.TableStoreEvent])(using t : Timeout)(using system : ActorSystem[_])(using ec : ExecutionContext = system.executionContext) : Future[worker_query.ProcessQueryPlanItemResult] = {
        store ! TableStore.DeleteHash(dataSource, numPartitions)
        return Future.successful(worker_query.ProcessQueryPlanItemResult(true))
    }

case class PartialGetPartition(dataSource : PartialDataSource, workerURLs : Seq[InetSocketAddress]) extends PartialQueryPlanItem:
    val innerProtobuf : worker_query.QueryPlanItem.Item = worker_query.QueryPlanItem.Item.GetPartition(worker_query.GetPartition(Some(dataSource.protobuf), workerURLs.map(address => table_model.InetSocketAddress(address.getHostName, address.getPort))))

    def execute(store: ActorRef[TableStore.TableStoreEvent])(using t : Timeout)(using system : ActorSystem[_])(using ec : ExecutionContext = system.executionContext) : Future[worker_query.ProcessQueryPlanItemResult] = {
        val channels = workerURLs.map(address => ChannelManager(address.getHostName, address.getPort))

        val resultFuture =  dataSource.getPartialData(store, channels)
        
        return Future.successful(worker_query.ProcessQueryPlanItemResult(true))
    }

case class PartialDeletePartition(dataSource : DataSource) extends PartialQueryPlanItem:
    val innerProtobuf : worker_query.QueryPlanItem.Item = worker_query.QueryPlanItem.Item.DeletePartition(worker_query.DeletePartition(Some(dataSource.protobuf)))

    def execute(store: ActorRef[TableStore.TableStoreEvent])(using t : Timeout)(using system : ActorSystem[_])(using ec : ExecutionContext = system.executionContext) : Future[worker_query.ProcessQueryPlanItemResult] = {
        store ! TableStore.DeletePartition(dataSource)
        return Future.successful(worker_query.ProcessQueryPlanItemResult(true))
    }