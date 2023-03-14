package org.oliverlittle.clusterprocess.worker

import org.oliverlittle.clusterprocess.worker_query
import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.query.PartialQueryPlanItem
import org.oliverlittle.clusterprocess.model.table.{Table, TableTransformation, TableStore, TableStoreData, TableStoreSystem, TableResult}
import org.oliverlittle.clusterprocess.connector.grpc.{StreamedTableResult, TableResultRunnable, DelayedTableResultRunnable}
import org.oliverlittle.clusterprocess.connector.cassandra.{CassandraConfig, CassandraConnector}
import org.oliverlittle.clusterprocess.dependency.SizeEstimator
import org.oliverlittle.clusterprocess.util.MemoryUsage

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


// Globally set timeout for asks (this might not be long enough, as some PartialQueryPlanItems will be long running jobs)
given timeout : Timeout = 1.minute

object WorkerQueryServer {
    private val logger = LoggerFactory.getLogger(classOf[WorkerQueryServer].getName)
    private val defaultPort = 50052

    def start(args : Array[String]): Unit = {
        val system : ActorSystem[TableStoreSystem.TableStoreSystemEvent] = TableStoreSystem.create()
        implicit val ec: ExecutionContext = system.executionContext
        implicit val scheduler = system.scheduler
        val tableStoreFuture : Future[ActorRef[TableStore.TableStoreEvent]] = system.ask(ref => TableStoreSystem.GetStore(ref))

        val tableStoreActor = Await.result(tableStoreFuture, 3.seconds)
        val usedPort = if args.size > 0 then args(0).toInt else defaultPort

        val server = new WorkerQueryServer(ExecutionContext.global, tableStoreActor, usedPort)(using system)
        server.blockUntilShutdown()
    }
}

class WorkerQueryServer(executionContext: ExecutionContext, store : ActorRef[TableStore.TableStoreEvent], port : Int)(using system : ActorSystem[_])(using ec : ExecutionContext = system.executionContext) {
    private val server =  ServerBuilder.forPort(port).addService(worker_query.WorkerComputeServiceGrpc.bindService(new WorkerQueryServicer(store), executionContext)).build.start
    WorkerQueryServer.logger.info("gRPC Server started, listening on " + port)
    WorkerQueryServer.logger.info("Allocated " + (MemoryUsage.getMaxMemory(Runtime.getRuntime).toDouble / 1000000).round.toString +  "MB of memory.")
    
    sys.addShutdownHook({
        WorkerQueryServer.logger.info("*** Shutting down gRPC server since JVM is shutting down.")
        this.stop()
        WorkerQueryServer.logger.info("*** gRPC server shut down.")
    })

    private def stop() : Unit = this.server.shutdown()

    private def blockUntilShutdown(): Unit = this.server.awaitTermination()
}

object WorkerQueryServicer:
    private val logger = LoggerFactory.getLogger(classOf[WorkerQueryServicer].getName)

class WorkerQueryServicer(store : ActorRef[TableStore.TableStoreEvent])(using system : ActorSystem[_])(using ec : ExecutionContext = system.executionContext)(using env : CassandraConfig {val connector : CassandraConnector} = CassandraConfig()) extends worker_query.WorkerComputeServiceGrpc.WorkerComputeService {
        override def getLocalCassandraNode(request : worker_query.GetLocalCassandraNodeRequest) : Future[worker_query.GetLocalCassandraNodeResult] = {
            WorkerQueryServicer.logger.info("getLocalCassandraNode")
            val connector = env.connector
            WorkerQueryServicer.logger.info("Host: " + connector.socket.getHostName + ", port: " + connector.socket.getPort.toString)
            val response = worker_query.GetLocalCassandraNodeResult(address=Some(table_model.InetSocketAddress(host=connector.socket.getHostName, port=connector.socket.getPort)))
            Future.successful(response)
        }


        override def processQueryPlanItem(item : worker_query.QueryPlanItem) : Future[worker_query.ProcessQueryPlanItemResult] = {
            val queryPlanItem = PartialQueryPlanItem.fromProtobuf(item)
            WorkerQueryServicer.logger.info("processQueryPlanItem - " + queryPlanItem.getClass.getSimpleName)
            val res = queryPlanItem.execute(store).recoverWith {
                case e : Throwable => 
                    WorkerQueryServicer.logger.error("processQueryPlanItem failed:", e)
                    val status = Status.newBuilder
                        .setCode(Code.UNKNOWN.getNumber)
                        .setMessage(e.getMessage)
                        .build()
                    Future.failed(StatusProto.toStatusRuntimeException(status))
            }
            return res
        }

        override def getTableData(request : worker_query.GetTableDataRequest, responseObserver : StreamObserver[table_model.StreamedTableResult]) : Unit = {
            val table = Table.fromProtobuf(request.table.get)

            val runnable = responseObserverToDelayedRunnable(responseObserver)

            WorkerQueryServicer.logger.info("getTableData")
            WorkerQueryServicer.logger.info(table.toString)

            store.ask[Iterator[TableResult]](ref => TableStore.GetResultIterator(table, ref)).onComplete {
                case Success(i) if i.isEmpty => runnable.setData(table.empty)
                case Success(i) => runnable.setData(i)
                case Failure(e) => 
                    WorkerQueryServicer.logger.error("getTableData failed:", e)
                    runnable.setError(e)
            }
        }

        override def getHashedPartitionData(request : worker_query.GetHashedPartitionDataRequest, responseObserver : StreamObserver[table_model.StreamedTableResult]) : Unit = {
            val table = Table.fromProtobuf(request.table.get)

            val runnable = responseObserverToDelayedRunnable(responseObserver)

            WorkerQueryServicer.logger.info("getHashedPartitionData")

            store.ask[Option[TableResult]](ref => TableStore.GetHash(table, request.totalPartitions, request.partitionNum, ref)).onComplete {
                case Success(None) => 
                    WorkerQueryServicer.logger.info("getHashedPartitionData - empty table")
                    runnable.setData(table.empty)
                case Success(Some(t)) => 
                    WorkerQueryServicer.logger.info("getHashedPartitionData - iterator")
                    runnable.setData(t)
                case Failure(e) => 
                    WorkerQueryServicer.logger.error("getHashedPartitionData failed:", e)
                    runnable.setError(e)
            }
        }

        override def getTableStoreData(request : worker_query.GetTableStoreDataRequest) : Future[worker_query.TableStoreData] = 
            store.ask[TableStoreData](ref => TableStore.GetData(ref)) map {res =>
                res.protobuf
            }

        override def clearCache(request : worker_query.ClearCacheRequest) : Future[worker_query.ClearCacheResult] = {
            WorkerQueryServicer.logger.info("Cache cleared.")
            store ! TableStore.Reset()
            Future.successful(worker_query.ClearCacheResult(true))
        }

        override def getEstimatedTableSize(request : worker_query.GetEstimatedTableSizeRequest) : Future[worker_query.GetEstimatedTableSizeResult] = 
            val table = Table.fromProtobuf(request.table.get)

            WorkerQueryServicer.logger.info("getEstimatedTableSize")

            store.ask[Long](ref => TableStore.GetTableSize(table, ref)).map { res =>
                worker_query.GetEstimatedTableSizeResult(estimatedSizeMb = res)
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