package org.oliverlittle.clusterprocess

import org.oliverlittle.clusterprocess.server.ClientQueryServer
import org.oliverlittle.clusterprocess.cassandra.CassandraConnector

@main def main: Unit = {
    // Verify connection to force a failure at startup if Cassandra is unavailable
    CassandraConnector.verifyConnection
    ClientQueryServer.main()
}