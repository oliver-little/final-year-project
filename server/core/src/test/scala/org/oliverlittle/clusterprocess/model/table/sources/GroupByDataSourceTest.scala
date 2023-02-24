package org.oliverlittle.clusterprocess.model.table.sources

import org.oliverlittle.clusterprocess.UnitSpec

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar
import org.oliverlittle.clusterprocess.table_model.PartialGroupByDataSource

class GroupByDataSourceTest extends UnitSpec with MockitoSugar {
    "A GroupByDataSource" should "return the correct headers" in {
        fail()
    }

    it should "get the Table above as a dependency" in {
        fail()
    }

    it should "be valid if the table above is valid, as well as the unique and aggregate fields" in {
        fail()
    }

    it should "be invalid if any dependency is invalid" in {
        fail()
    }

    it should "get partitions based on the stored data size" in {
        fail()
    }

    it should "hash the data based on the unique column" in {
        fail()
    }

    it should "convert to GroupBy protobuf correctly" in {
        fail()
    }

    it should "convert to DataSource protobuf correctly" in {
        fail()
    }
}

class PartialGroupByDataSourceTest extends UnitSpec with MockitoSugar {
    "A PartialGroupByDataSource" should "convert to protobuf correctly" in {
        fail()
    }

    it should "get partial data from the other workers and data store" in {
        fail()
    }

    it should "return an empty table if no hashed data was received" in {
        fail()
    }

    it should "get a TableResult of hashed partition data from a single worker" in {
        fail()
    }

    it should "perform a group by without aggregate functions correctly" in {
        fail()
    }

    it should "perform a group by with aggregate functions correctly" in {
        fail()
    }
}