package org.oliverlittle.clusterprocess.query

import org.oliverlittle.clusterprocess.UnitSpec

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar

class PrepareResultTest extends UnitSpec {
    "A PrepareResult" should "throw an IllegalStateException if executed" in {
        fail()
    }

    it should "return PrepareResultWithPartitions when provided with PartialDataSource partitions" in {
        fail()
    }
}

class PrepareResultWithPartitionsTest extends UnitSpec {
    "A PrepareResultWithPartitions" should "calculate protobuf queries correctly" in {
        fail()
    }

    it should "calculate the partitions count correctly" in {
        fail()
    }

    it should "start consumers and a counter" in {
        fail()
    }

    it should "send a message when completed" in {
        fail()
    }

    it should "send an error message if an error occurred" in {
        fail()
    }
}

class DeleteResultTest extends UnitSpec {
    "A DeleteResult" should "send DeleteResult messages to workers" in {
        fail()
    }

    it should "forward partitions if it has any" in {
        fail()
    }

    it should "send an error message if an error occurs" in {
        fail()
    }
}

class GetPartitionTest extends UnitSpec {
    "A GetPartition" should "start a GetPartitionExecutor worker" in {
        fail()
    }
}

class GetPartitionExecutor extends UnitSpec {
    "A GetPartitionExecutor" should "fetch the partitions from the data source" in {
        fail()
    }
    
    it should "create producers, consumers and a counter with the correct data" in {
        fail()
    }

    it should "send prepare hashes and delete hashes messages if the data source has dependencies" in {
        fail()
    }

    it should "send a message with Data Source partitions when it completes" in {
        fail()
    }

    it should "send an error message if an error occurs at any point" in {
        fail()
    }
}

class DeletePartitionTest extends UnitSpec {
    "A DeletePartition" should "send DeletePartition messages to all workers" in {
        fail()
    }

    it should "send an error message if an error occurs" in {
        fail()
    }
}