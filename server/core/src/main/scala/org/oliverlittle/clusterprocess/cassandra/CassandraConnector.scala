package org.oliverlittle.clusterprocess.cassandra

import com.datastax.oss.driver.api.core.CqlSession;
import java.net.InetSocketAddress

object CassandraConnector {
    var host : String = "localhost"
    var datacenter : String = "datacenter1"
    private var session : CqlSession = CqlSession.builder().addContactPoint(new InetSocketAddress(host, 9042)).withLocalDatacenter(datacenter).build()

    def getSession : CqlSession = session
}