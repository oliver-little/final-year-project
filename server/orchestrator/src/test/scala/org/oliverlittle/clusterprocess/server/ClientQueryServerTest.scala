package org.oliverlittle.clusterprocess.server

import org.oliverlittle.clusterprocess.UnitSpec

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar

class ClientQueryServerTest extends UnitSpec with MockitoSugar {
    "A ClientQueryServer" should "compute a table if the table is valid" in {
        fail()
    }

    it should "return an invalid argument error if the table is invalid" in {
        fail()
    }

    it should "return an unknown error if the query fails for any reason" in {
        fail()
    }

    it should "push the cache, and pop it if the query was successful" in {
        fail()
    }

    it should "push the cache, and pop it if the query fails after generating the table" in {
        fail()
    }
}