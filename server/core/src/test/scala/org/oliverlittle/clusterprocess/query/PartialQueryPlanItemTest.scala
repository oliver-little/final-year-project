package org.oliverlittle.clusterprocess.query

import org.oliverlittle.clusterprocess.UnitSpec

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar

class PartialQueryPlanItemTest extends UnitSpec {
    "PartialQueryPlanItem" should "deserialise from protobuf" in {
        fail()
    }
}

class PartialPrepareResultTest extends UnitSpec {
    "A PartialPrepareResult" should "add a PartialTable to the TableStore" in {
        fail()
    }

    it should "throw an error when the dependent PartialDataSource is missing" in {
        fail()
    }
}

class PartialDeleteResultTest extends UnitSpec {
    "A PartialDeleteResult" should "remove all instances of a table from the TableStore" in {
        fail()
    }
}

class PartialPrepareHashesTest extends UnitSpec {
    "A PartialPrepareHashes" should "calculate the hashes of the dependencies of a data source" in {
        fail()
    }

    it should "fail if an error occurs" in {
        fail()
    }
}

class PartialDeletePreparedHashesTest extends UnitSpec {
    "A PartialDeletePreparedHashes" should "remove all calculated hashes for a DependentDataSource" in {
        fail()
    }

    it should "fail if an error occurs" in {
        fail()
    }
}

class PartialGetPartition extends UnitSpec {
    "A PartialGetPartition" should "add a PartialDataSource to the TableStore" in {
        fail()
    }

    it should "fail if an error occurs" in {
        fail()
    }
}

class PartialDeletePartition extends UnitSpec {
    "A PartialDeletePartition" should "delete all instances of a DataSource from the TableStore" in {
        
    }
}