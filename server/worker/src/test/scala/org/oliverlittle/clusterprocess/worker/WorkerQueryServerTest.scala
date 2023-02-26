package org.oliverlittle.clusterprocess.worker

import org.oliverlittle.clusterprocess.UnitSpec

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar

class WorkerQueryServerTest extends UnitSpec with MockitoSugar {
    "A WorkerQueryServer" should "return the local Cassandra node from CassandraConnector" in {
        fail()
    }

    it should "process a single PartialQueryPlanItem" in {
        fail()
    }

    it should "return an unknown error if the PartialQueryPlanItem computation fails for any reason" in {
        fail()
    }

    it should "return the table data as a stream" in {
        fail()
    }

    it should "return an empty table if the table does not exist" in {
        fail()
    }

    it should "return an error if an error occurs when fetching the table data" in {
        fail()
    }

    it should "return the hashed table data as a stream" in {
        fail()
    }

    it should "return an empty table if the hashed table does not exist" in {
        fail()
    }

    it should "return an error if an error occurs when fetching the hashed table data" in {
        fail()
    }

    it should "return the TableStoreData as a protobuf" in {
        fail()
    }

    it should "apply cache push requests" in {
        fail()
    }

    it should "apply cache pop requests" in {
        fail()
    }

    it should "return the estimated table size for a table" in {
        fail()
    }

    it should "return 0 for the estimated table size if the table doesn't exist, or an error occurs" in {
        fail()
    }
}