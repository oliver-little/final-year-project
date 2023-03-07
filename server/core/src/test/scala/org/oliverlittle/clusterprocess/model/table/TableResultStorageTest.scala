package org.oliverlittle.clusterprocess.model.table

import org.oliverlittle.clusterprocess.UnitSpec

class InMemoryPartialTableTest extends UnitSpec {
    "An InMemoryPartialTable" should "spill the result to disk, and update the TableStoreData" in {
        fail()
    }
}

class InMemoryPartialDataSourceTest extends UnitSpec {
    "An InMemoryPartialDataSource" should "spill the result to disk, and update the TableStoreData" in {
        fail()
    }
}

class InMemoryDependencyTest extends UnitSpec {
    "An InMemoryDependency" should "spill the result to disk, and update the TableStoreData" in {
        fail()
    }
}

class ProtobufTableResultTest extends UnitSpec {
    "A ProtobufTableResult" should "resolve the correct path from the environment variable, and the source" in {
        fail()
    }

    it should "store a TableResult at the path location" in {
        fail()
        // Remember to cleanup file data
    }

    it should "retrieve a TableResult from the path location" in {
        fail()
    }

    it should "delete a stored file on cleanup" in {
        fail()
    }
}