package org.oliverlittle.clusterprocess.connector.cassandra.size_estimation

import org.oliverlittle.clusterprocess.connector.cassandra.token._

import com.datastax.oss.driver.api.core.CqlSession
import scala.jdk.CollectionConverters._

object TableSizeEstimation {
    def estimateTableSize(session : CqlSession, keyspace : String, table : String) : TableSizeEstimation = {
        val res = session.execute("SELECT keyspace_name, table_name, range_start, range_end, mean_partition_size, partitions_count FROM system.size_estimates WHERE keyspace_name=? AND table_name=?", keyspace, table)
        return TableSizeEstimation(res.iterator.asScala.map(row => SizeEstimatesRow(CassandraTokenRange.fromString(row.getString("range_start"), row.getString("range_end")), row.getLong("mean_partition_size").toLong, row.getLong("partitions_count"))).toSeq)
    }
}

case class TableSizeEstimation(estimateRows : Seq[SizeEstimatesRow]) {
    lazy val percentageOfFullRing : Double = estimateRows.map(_.range.percentageOfFullRing).sum
    lazy val allRowsSize : Long = estimateRows.map(_.size).sum
    // Estimated size of this table, in bytes
    lazy val estimatedTableSize : Double = allRowsSize / percentageOfFullRing
    lazy val estimatedTableSizeMB : Double = estimatedTableSize / 1000000
}

case class SizeEstimatesRow(range : CassandraTokenRange, meanPartitionSize : Long, partitionsCount : Long) {
    val size = meanPartitionSize * partitionsCount
}