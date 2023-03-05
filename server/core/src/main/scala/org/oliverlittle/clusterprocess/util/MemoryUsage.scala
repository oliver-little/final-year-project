package org.oliverlittle.clusterprocess.util

// Adapted from: https://stackoverflow.com/questions/3571203/what-are-runtime-getruntime-totalmemory-and-freememory
object MemoryUsage:
    def getMemoryUsagePercentage(runtime : Runtime) : Double = MemoryUsage.getUsedMemory(runtime) / runtime.maxMemory

    def getTotalFreeMemory(runtime : Runtime) : Double = runtime.maxMemory - MemoryUsage.getUsedMemory(runtime)

    def getUsedMemory(runtime : Runtime) : Double = runtime.totalMemory - runtime.freeMemory