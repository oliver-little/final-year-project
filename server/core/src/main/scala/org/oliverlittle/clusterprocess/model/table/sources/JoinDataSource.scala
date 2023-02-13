package org.oliverlittle.clusterprocess.model.table.sources

import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.data_source

abstract case class JoinDataSource(joinType : table_model.Join.JoinType, left : Table, right : Table) extends DataSource:
    // Same as in DataSource definition
    def getHeaders : TableResultHeader

    // New function, gives it a list of workers and their channels and requests some partitions/partition data back
    // Partitions will need to be an interface of some kind to handle both Cassandra and internal representations
    // -- Partitions able to calculate themselves? (given the correct dependencies)
    def getPartitions : Any // will be partitions

    def getData : TableResult

    // Same as in DataSource definition, this will just become a new kind of data source
    def protobuf : data_source.DataSource

