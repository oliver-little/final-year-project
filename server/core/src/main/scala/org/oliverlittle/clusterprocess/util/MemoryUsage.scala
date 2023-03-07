package org.oliverlittle.clusterprocess.util

// Adapted from: https://stackoverflow.com/questions/3571203/what-are-runtime-getruntime-totalmemory-and-freememory
object MemoryUsage:
    def getMemoryUsagePercentage(runtime : Runtime) : Double = MemoryUsage.getUsedMemory(runtime).toDouble / runtime.maxMemory.toDouble

    def getMemoryUsageAboveThreshold(runtime : Runtime, threshold : Double) : Long = {
        val usedPercentage = getMemoryUsagePercentage(runtime)
        if usedPercentage > threshold
        then ((usedPercentage - threshold) * runtime.maxMemory).toLong
        else 0
    }

    def getTotalFreeMemory(runtime : Runtime) : Long = runtime.maxMemory - MemoryUsage.getUsedMemory(runtime).toLong

    def getUsedMemory(runtime : Runtime) : Long = runtime.totalMemory - runtime.freeMemory