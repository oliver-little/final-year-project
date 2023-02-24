package org.oliverlittle.clusterprocess.worker

import org.oliverlittle.clusterprocess.worker_query
import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.query.PartialQueryPlanItem
import org.oliverlittle.clusterprocess.model.table.{Table, TableTransformation, TableStore, TableStoreData, TableResult}
import org.oliverlittle.clusterprocess.connector.grpc.{StreamedTableResult, TableResultRunnable, DelayedTableResultRunnable}
import org.oliverlittle.clusterprocess.connector.cassandra.{CassandraConfig, CassandraConnector}
import org.oliverlittle.clusterprocess.dependency.SizeEstimator

import io.grpc.{ServerBuilder}
import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}
import io.grpc.protobuf.StatusProto
import com.google.rpc.{Status, Code}
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.actor.typed.scaladsl.AskPattern._
import akka.pattern.StatusReply
import akka.util.Timeout
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future, Await}
import scala.concurrent.duration._
import scala.util.{Success, Failure}


// Globally set timeout for asks
given timeout : Timeout = 3.seconds

object WorkerQueryServer {
    private val logger = LoggerFactory.getLogger(classOf[WorkerQueryServer].getName)
    private val port = 50052

    def main(): Unit = {

        
        val system : ActorSystem[TableStoreSystem.TableStoreSystemEvent] = TableStoreSystem.create()
        implicit val ec: ExecutionContext = system.executionContext
        implicit val scheduler = system.scheduler
        val tableStoreFuture : Future[ActorRef[TableStore.TableStoreEvent]] = system.ask(ref => TableStoreSystem.GetStore(ref))

        val tableStoreActor = Await.result(tableStoreFuture, 3.seconds)
        val server = new WorkerQueryServer(ExecutionContext.global, tableStoreActor)(using system)
        server.blockUntilShutdown()
    }
}

class WorkerQueryServer(executionContext: ExecutionContext, store : ActorRef[TableStore.TableStoreEvent])(using system : ActorSystem[_])(using ec : ExecutionContext = system.executionContext) {
    private val server =  ServerBuilder.forPort(WorkerQueryServer.port).addService(worker_query.WorkerComputeServiceGrpc.bindService(new WorkerQueryServicer(store), executionContext)).build.start
    WorkerQueryServer.logger.info("gRPC Server started, listening on " + WorkerQueryServer.port)
    
    sys.addShutdownHook({
        WorkerQueryServer.logger.info("*** Shutting down gRPC server since JVM is shutting down.")
        this.stop()
        WorkerQueryServer.logger.info("*** gRPC server shut down.")
    })

    private def stop() : Unit = this.server.shutdown()

    private def blockUntilShutdown(): Unit = this.server.awaitTermination()

    private class WorkerQueryServicer(store : ActorRef[TableStore.TableStoreEvent]) extends worker_query.WorkerComputeServiceGrpc.WorkerComputeService {
        /*override def computePartialResultCassandra(request : worker_query.ComputePartialResultCassandraRequest, responseObserver : StreamObserver[table_model.StreamedTableResult]) : Unit = {
            WorkerQueryServer.logger.info("computePartialResultCassandra")

            // Parse table
            val cassandraSourcePB = request.dataSource.get
            // Deserialise protobuf token range and pass into datasource here:
            val tokenRangeProtobuf = request.tokenRange.get
            val tokenRange = CassandraTokenRange.fromLong(tokenRangeProtobuf.start, tokenRangeProtobuf.end)
            val dataSource = CassandraDataSource.inferDataSourceFromCassandra(connector, cassandraSourcePB.keyspace, cassandraSourcePB.table, Some(tokenRange))
            val tableTransformations = TableTransformation.fromProtobuf(request.table.get)
            WorkerQueryServer.logger.info(tableTransformations.toString)
            val table = Table(dataSource, tableTransformations)
            WorkerQueryServer.logger.info("Created table instance.")
            
            if !table.isValid then responseObserver.onError(new IllegalArgumentException("Table cannot be computed."))
            
            WorkerQueryServer.logger.info("Computing table result")
            val result = table.computePartial
            WorkerQueryServer.logger.info("Table result ready.")

            // Able to make this unchecked cast because this is a response from a server
            val serverCallStreamObserver = responseObserver.asInstanceOf[ServerCallStreamObserver[table_model.StreamedTableResult]]
            val data = StreamedTableResult.tableResultToIterator(result)

            val runnable = TableResultRunnable(serverCallStreamObserver, data)

            serverCallStreamObserver.setOnReadyHandler(runnable)
            
            runnable.run()
        }*/

        override def getLocalCassandraNode(request : worker_query.GetLocalCassandraNodeRequest) : Future[worker_query.GetLocalCassandraNodeResult] = {
            WorkerQueryServer.logger.info("getLocalCassandraNode")
            val connector = CassandraConfig().connector
            WorkerQueryServer.logger.info("Host: " + connector.socket.getHostName + ", port: " + connector.socket.getPort.toString)
            val response = worker_query.GetLocalCassandraNodeResult(address=Some(table_model.InetSocketAddress(host=connector.socket.getHostName, port=connector.socket.getPort)))
            Future.successful(response)
        }


        override def processQueryPlanItem(item : worker_query.QueryPlanItem) : Future[worker_query.ProcessQueryPlanItemResult] = {
            val queryPlanItem = PartialQueryPlanItem.fromProtobuf(item)
            WorkerQueryServer.logger.info("processQueryPlanItem - " + queryPlanItem.getClass.getSimpleName)
            val res = queryPlanItem.execute(store)
            WorkerQueryServer.logger.info("processQueryPlanItem - done")
            return res
        }

        override def getTableData(request : worker_query.GetTableDataRequest, responseObserver : StreamObserver[table_model.StreamedTableResult]) : Unit = {
            val table = Table.fromProtobuf(request.table.get)

            val runnable = responseObserverToDelayedRunnable(responseObserver)

            WorkerQueryServer.logger.info("getTableData")
            WorkerQueryServer.logger.info(table.toString)

            store.ask[Seq[TableResult]](ref => TableStore.GetAllResults(table, ref)).onComplete {
                case Success(Seq()) => runnable.setData(table.empty)
                case Success(results) => runnable.setData(results.reduce(_ ++ _))
                case Failure(e) => runnable.setError(e)
            }
        }

        override def getHashedPartitionData(request : worker_query.GetHashedPartitionDataRequest, responseObserver : StreamObserver[table_model.StreamedTableResult]) : Unit = {
            val table = Table.fromProtobuf(request.table.get)

            val runnable = responseObserverToDelayedRunnable(responseObserver)

            WorkerQueryServer.logger.info("getHashedPartitionData")
            WorkerQueryServer.logger.info(table.toString)

            store.ask[Option[TableResult]](ref => TableStore.GetHash(table, request.totalPartitions, request.partitionNum, ref)).onComplete {
                case Success(None) => runnable.setData(table.empty)
                case Success(Some(t)) => runnable.setData(t)
                case Failure(e) => runnable.setError(e)
            }
        }

        override def getTableStoreData(request : worker_query.GetTableStoreDataRequest) : Future[worker_query.TableStoreData] = store.ask[TableStoreData](ref => TableStore.GetData(ref)).map(_.protobuf)

        override def modifyCache(request : worker_query.ModifyCacheRequest) : Future[worker_query.TableStoreData] = request.cacheOperation match {
            case worker_query.ModifyCacheRequest.CacheOperation.PUSH => 
                WorkerQueryServer.logger.info("Cache state stored")
                store ! TableStore.PushCache()
                store.ask[TableStoreData](ref => TableStore.GetData(ref)).map(_.protobuf)
            case worker_query.ModifyCacheRequest.CacheOperation.POP =>
                WorkerQueryServer.logger.info("Cache popped")
                store.ask[Option[TableStoreData]](ref => TableStore.PopCache(ref)).map {
                    case Some(data) => data.protobuf
                    case None => throw IllegalStateException("No cache data available to pop")
                }
            case _ => throw new IllegalArgumentException("Unknown cache operation provided")
        }

        override def getEstimatedTableSize(request : worker_query.GetEstimatedTableSizeRequest) : Future[worker_query.GetEstimatedTableSizeResult] = 
            val table = Table.fromProtobuf(request.table.get)

            store.ask[Seq[TableResult]](ref => TableStore.GetAllResults(table, ref)).map {
                case Seq() => worker_query.GetEstimatedTableSizeResult(estimatedSizeMb = 0)
                case results => worker_query.GetEstimatedTableSizeResult(estimatedSizeMb = (SizeEstimator.estimate(results).toDouble / 1000000).round)
            }.recover { _ =>
                worker_query.GetEstimatedTableSizeResult(estimatedSizeMb = 0)
            }

        private def responseObserverToDelayedRunnable(responseObserver : StreamObserver[table_model.StreamedTableResult]) : DelayedTableResultRunnable = {
            // Able to make this unchecked cast because this is a response from a server
            val serverCallStreamObserver = responseObserver.asInstanceOf[ServerCallStreamObserver[table_model.StreamedTableResult]]
            // Prepare onReady hook 
            val runnable = DelayedTableResultRunnable(serverCallStreamObserver)
            serverCallStreamObserver.setOnReadyHandler(runnable)
            return runnable
        }
    }
}
