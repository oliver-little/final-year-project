package org.oliverlittle.clusterprocess.model.table.sources

import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.data_source
import org.oliverlittle.clusterprocess.model.table._
import org.oliverlittle.clusterprocess.model.field.expressions._
import org.oliverlittle.clusterprocess.connector.grpc.{WorkerHandler, ChannelManager}

import akka.actor.typed.ActorRef

case class GroupByDataSource(source : Table, uniqueColumns : Seq[NamedFieldExpression], aggregates : Seq[AggregateExpression]) extends DataSource:
    // Same as in DataSource definition
    lazy val getHeaders : TableResultHeader = TableResultHeader(uniqueColumns.map(_.outputTableField(source.outputHeaders)) ++ aggregates.flatMap(_.outputTableFields(source.outputHeaders)))

    // New function, gives it a list of workers and their channels and requests some partitions/partition data back
    // Partitions will need to be an interface of some kind to handle both Cassandra and internal representations
    // -- Partitions able to calculate themselves? (given the correct dependencies)
    def getPartitions(workerHandler : WorkerHandler) : Seq[(Seq[ChannelManager], Seq[PartialDataSource])]  = Seq((Seq(), Seq(PartialGroupByDataSource(this, 1, 1))))

    override val getDependencies: Seq[Table] = Seq(source)

    // Same as in DataSource definition, this will just become a new kind of data source
    def protobuf : data_source.DataSource = data_source.DataSource()

case class PartialGroupByDataSource(parent : GroupByDataSource, partitionNum : Int, totalPartitions : Int) extends PartialDataSource:
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