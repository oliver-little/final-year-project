package org.oliverlittle.clusterprocess.connector

import java.net.InetSocketAddress
import java.net.SocketTimeoutException
import java.net.Socket
import java.io.IOException
import List.fill

// Adapted from: https://stackoverflow.com/questions/29906290/short-code-to-test-http-latency
object PingLatency {
    /**
      * Estimates the latency to a particular address and port
      *
      * @param address Server URL
      * @param port Server Port
      * @param timeoutMillis How long to timeout a connection failure
      * @param attempts The number of attempts to average
      * @return The average latency, or None if an error occurred 
      */
    def getLatencyToURL(address : String, port : Int = 80, timeoutMillis : Int = 2000, attempts : Int = 5) : Option[Double] = {
        val socketAddress = InetSocketAddress(address, port)
        
        // Run getLatencyOneShot attempts times, then remove any failures
        val results = fill(attempts) {getLatencyOneShot(socketAddress, timeoutMillis)}.flatten
        
        // If no elements, return None as all attempts failed
        // If some elements, average them 
        results.length match {
            case 0 => return None
            case v => return Some(results.foldLeft(0.0)(_ + _) / v)
        }
    }

    def getLatencyOneShot(socketAddress : InetSocketAddress, timeoutMillis : Int = 2000) : Option[Double] = {
        val socket = new Socket()
        val start = System.currentTimeMillis()
        try {
            socket.connect(socketAddress, timeoutMillis);
        } 
        catch { // Timed out and not reachable
            case e : SocketTimeoutException => return None
            case e : IOException => return None
        } 
        
        val stop = System.currentTimeMillis();
        
        try {
            socket.close()
        } 
        catch {
            case e : IOException => println(e)
        }

        return Some(stop.toDouble - start.toDouble)
    }
}