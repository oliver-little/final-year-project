package org.oliverlittle.clusterprocess.connector.cassandra.size_estimation

import org.oliverlittle.clusterprocess.connector.cassandra.token._
import org.oliverlittle.clusterprocess.UnitSpec

import scala.math.pow

class TableSizeEstimationTest extends UnitSpec {
    "A TableSizeEstimation" should "calculate the percentage of the full ring based on the row data" in {
        val rows = (1 to 5).map(i => SizeEstimatesRow(CassandraTokenRange(CassandraToken(0), CassandraToken(pow(10, 18).toLong)), 100, 100))
        TableSizeEstimation(rows).percentageOfFullRing should be (0.271 +- 0.01)
    }

    it should "calculate the size of all rows in bytes correctly" in {
        val rows = (1 to 5).map(i =>SizeEstimatesRow(CassandraTokenRange(CassandraToken(0), CassandraToken(pow(10, 18).toLong)), 100, 100))
        TableSizeEstimation(rows).allRowsSize should be (50000)
    }

    it should "calculate the estimated table size correctly" in {
        val rows = (1 to 5).map(i => SizeEstimatesRow(CassandraTokenRange(CassandraToken(0), CassandraToken(pow(10, 18).toLong)), 100, 100))
        val estimatedSize = 50000 / 0.27105
        val estimatedSizeMB = estimatedSize / 1000000
        val estimation = TableSizeEstimation(rows)
        estimation.estimatedTableSize should be ((estimatedSize * TableSizeEstimation.memoryOverheadMultiplier) +- 10)
        estimation.estimatedTableSizeMB should be ((estimatedSizeMB * TableSizeEstimation.memoryOverheadMultiplier) +- 0.0001)
    }
}