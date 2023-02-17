package org.oliverlittle.clusterprocess.worker

import org.oliverlittle.clusterprocess.worker_query
import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.query.PartialQueryPlanItem
import org.oliverlittle.clusterprocess.model.table.{Table, TableTransformation, TableStore, TableResult}
import org.oliverlittle.clusterprocess.connector.grpc.{StreamedTableResult, TableResultRunnable, DelayedTableResultRunnable}
import org.oliverlittle.clusterprocess.connector.cassandra.{CassandraConfig, CassandraConnector}

import io.grpc.{ServerBuilder}
import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}
import io.grpc.protobuf.StatusProto
import com.google.rpc.{Status, Code}
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.actor.typed.scaladsl.AskPattern._
import akka.util.Timeout

import scala.concurrent.{ExecutionContext, Future, Await}
import scala.concurrent.duration._
import scala.util.{Success, Failure}
import java.util.logging.Logger

// Globally set timeout for asks
given timeout : Timeout = 3.seconds

object WorkerQueryServer {
    private val logger = Logger.getLogger(classOf[WorkerQueryServer].getName)
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
            WorkerQueryServer.logger.info("processQueryPlanItem")
            val queryPlanItem = PartialQueryPlanItem.fromProtobuf(item)
            WorkerQueryServer.logger.info(queryPlanItem.toString)

            return queryPlanItem.execute(store)
        }

        override def getPartitionsForTable(request : worker_query.GetPartitionsForTableRequest, responseObserver : StreamObserver[table_model.StreamedTableResult]) : Unit = {
            val table = Table.fromProtobuf(request.table.get)

            val runnable = responseObserverToDelayedRunnable(responseObserver)

            store.ask(ref => TableStore.GetAllResults(table, ref)) onComplete {
                    case Success(Some(result : TableResult)) => runnable.setData(result)
                    case _ => 
                        val status = Status.newBuilder()
                            .setCode(Code.NOT_FOUND.getNumber)
                            .setMessage("Table not found in table store")
                            .build()
                        responseObserver.onError(StatusProto.toStatusRuntimeException(status))
            }
        }

        override def getHashedPartitionData(request : worker_query.GetHashedPartitionDataRequest, responseObserver : StreamObserver[table_model.StreamedTableResult]) : Unit = {
            val table = Table.fromProtobuf(request.table.get)

            val runnable = responseObserverToDelayedRunnable(responseObserver)

            store.ask(ref => TableStore.GetHash(table, request.totalPartitions, request.partitionNum, ref)) onComplete {
                case Success(Some(result : TableResult)) => runnable.setData(result)
                case _ => 
                    val status = Status.newBuilder()
                        .setCode(Code.NOT_FOUND.getNumber)
                        .setMessage("Partition not found in table store")
                        .build()
                    responseObserver.onError(StatusProto.toStatusRuntimeException(status))
            }
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
