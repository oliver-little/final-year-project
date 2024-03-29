package org.oliverlittle.clusterprocess

import org.oliverlittle.clusterprocess.server.ClientQueryServer
import org.oliverlittle.clusterprocess.connector.cassandra.CassandraConnector

@main def main: Unit = {
    // Verify connection to force a failure at startup if Cassandra is unavailable
    CassandraConnector.verifyConnection
    ClientQueryServer.main()
}