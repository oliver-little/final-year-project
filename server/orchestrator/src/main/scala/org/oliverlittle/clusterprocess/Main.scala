package org.oliverlittle.clusterprocess

import io.grpc.ManagedChannelBuilder

import org.oliverlittle.clusterprocess.model.table.SelectTransformation
import org.oliverlittle.clusterprocess.model.field.expressions._
import org.oliverlittle.clusterprocess.cassandra_source._
import org.oliverlittle.clusterprocess.worker_query._
import org.oliverlittle.clusterprocess.table_model._

@main def main: Unit = {
    val source = CassandraDataSource("localhost", "test", "test_table")
    val tokenRange = CassandraTokenRange(1, 1000)
    val table = Table(Seq(SelectTransformation(FieldOperations.AddInt(V(1), V(2)).as("Test_Field")).protobuf))
    val computePartialResultCassandraRequest = ComputePartialResultCassandraRequest(Some(table), Some(source), Some(tokenRange))

    val channel = ManagedChannelBuilder.forAddress("localhost", 50051).usePlaintext.build;
    val blockingStub = WorkerComputeServiceGrpc.blockingStub(channel);
    val res = blockingStub.computePartialResultCassandra(computePartialResultCassandraRequest);
}