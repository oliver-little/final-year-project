package org.oliverlittle.clusterprocess.util

import org.oliverlittle.clusterprocess.UnitSpec

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar

class MemoryUsageTest extends UnitSpec with MockitoSugar {
    "MemoryUsage" should "calculate the amount of memory that is in use" in {
        val runtimeMock = mock[Runtime]
        when(runtimeMock.maxMemory).thenReturn(200L)
        when(runtimeMock.totalMemory).thenReturn(100L)
        when(runtimeMock.freeMemory).thenReturn(40L)
        MemoryUsage.getUsedMemory(runtimeMock) should be (60)
    }

    it should "calculate the total amount of free memory" in {
        val runtimeMock = mock[Runtime]
        when(runtimeMock.maxMemory).thenReturn(200L)
        when(runtimeMock.totalMemory).thenReturn(100L)
        when(runtimeMock.freeMemory).thenReturn(40L)
        MemoryUsage.getTotalFreeMemory(runtimeMock) should be (140)
    }

    it should "calculate the percentage of total memory in use" in {
        val runtimeMock = mock[Runtime]
        when(runtimeMock.maxMemory).thenReturn(200L)
        when(runtimeMock.totalMemory).thenReturn(100L)
        when(runtimeMock.freeMemory).thenReturn(40L)
        MemoryUsage.getMemoryUsagePercentage(runtimeMock) should be (0.3 +- 0.01)
    }

    it should "calculate an estimate of the memory in use" in {
        val threshold = 0.25
        val runtimeMock = mock[Runtime]
        when(runtimeMock.maxMemory).thenReturn(200L)
        when(runtimeMock.totalMemory).thenReturn(100L)
        when(runtimeMock.freeMemory).thenReturn(40L)
        MemoryUsage.getMemoryUsageAboveThreshold(runtimeMock, threshold) should be (10L +- 1L)
    }
}