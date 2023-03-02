package org.oliverlittle.clusterprocess

import org.oliverlittle.clusterprocess.worker.WorkerQueryServer
import org.oliverlittle.clusterprocess.connector.cassandra.CassandraConnector

object Main {
    // Note: if running from sbt from terminal, wrap the run command in "" to pass arguments
    // E.G: sbt "run 50053"
    def main(args : Array[String]) : Unit = {
        // Verify connection to force a failure at startup if Cassandra is unavailable
        CassandraConnector.verifyConnection
        WorkerQueryServer.start(args)
    }
}