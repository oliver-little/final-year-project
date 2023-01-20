package org.oliverlittle.clusterprocess.util

import java.nio.ByteBuffer;

object ByteBufferOperations {
    def toByteBuffer(input : String) : ByteBuffer = ByteBuffer.wrap(input.getBytes)
}