package org.oliverlittle.clusterprocess

import org.oliverlittle.clusterprocess.worker.WorkerQueryServer
import org.oliverlittle.clusterprocess.cassandra.CassandraConnector

@main def main: Unit = {
    CassandraConnector.verifyConnection()
    WorkerQueryServer.main()
}