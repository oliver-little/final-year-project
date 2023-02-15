package org.oliverlittle.clusterprocess.model.table.sources

import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.data_source
import org.oliverlittle.clusterprocess.model.table._
import org.oliverlittle.clusterprocess.model.field.expressions._
import org.oliverlittle.clusterprocess.connector.grpc.{WorkerHandler, ChannelManager}

import akka.actor.typed.ActorRef
import scala.util.Try

case class GroupByDataSource(source : Table, uniqueFields : Seq[NamedFieldExpression], aggregates : Seq[AggregateExpression]) extends DataSource:
    // Same as in DataSource definition
    lazy val getHeaders : TableResultHeader = TableResultHeader(uniqueFields.map(_.outputTableField(source.outputHeaders)) ++ aggregates.flatMap(_.outputTableFields(source.outputHeaders)))
    override val getDependencies: Seq[Table] = Seq(source)
    lazy val isValid = source.isValid && Try{uniqueFields.map(_.resolve(source.outputHeaders))}.isSuccess


    // New function, gives it a list of workers and their channels and requests some partitions/partition data back
    // Partitions will need to be an interface of some kind to handle both Cassandra and internal representations
    // -- Partitions able to calculate themselves? (given the correct dependencies)
    def getPartitions(workerHandler : WorkerHandler) : Seq[(Seq[ChannelManager], Seq[PartialDataSource])]  = Seq((Seq(), Seq(PartialGroupByDataSource(this, 1, 1))))

    lazy val groupByProtobuf : data_source.GroupByDataSource = data_source.GroupByDataSource(uniqueFields.map(_.protobuf), aggregates.map(_.protobuf))
    // Same as in DataSource definition, this will just become a new kind of data source
    lazy val protobuf : data_source.DataSource = data_source.DataSource().withGroupBy(groupByProtobuf)

case class PartialGroupByDataSource(parent : GroupByDataSource, partitionNum : Int, totalPartitions : Int) extends PartialDataSource:
    lazy val protobuf : data_source.PartialDataSource = data_source.PartialDataSource().withGroupBy(data_source.PartialGroupByDataSource(
        Some(parent.groupByProtobuf), 
        Some(data_source.PartitionInformation(partitionNum, totalPartitions))
    ))

    def getPartialData(workerChannels : Seq[ChannelManager]) : TableResult = {
        // Check for cache hit - something like
        // Once we've got the data from the workers, we need to actually run the group by       
        return getDataFromWorkers(workerChannels)
    }

    def getDataFromWorkers(workerChannels : Seq[ChannelManager]) : TableResult = {
        // Negotiate with all other workers to get their data

        // Placeholder
        return LazyTableResult(TableResultHeader(Seq()), Seq())
    }