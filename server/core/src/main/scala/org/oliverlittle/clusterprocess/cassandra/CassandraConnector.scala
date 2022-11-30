package org.oliverlittle.clusterprocess.cassandra

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata
import java.net.InetSocketAddress

object CassandraConnector {
    var host : String = "localhost"
    var datacenter : String = "datacenter1"
    private var session : CqlSession = CqlSession.builder().addContactPoint(new InetSocketAddress(host, 9042)).withLocalDatacenter(datacenter).build()

    def getSession : CqlSession = session

    def getTableMetadata(keyspace : String, table : String) : TableMetadata = {
        val ksObj = CassandraConnector.getSession.getMetadata.getKeyspace(keyspace)
        if ksObj.isPresent then
            val tableObj = ksObj.get.getTable(table)
            if tableObj.isPresent then
                return tableObj.get
            throw new IllegalArgumentException("Table " + keyspace + "." + table + " not found.")
        throw new IllegalArgumentException("Keyspace " + keyspace + " not found.")
    }

    def hasTable(keyspace : String, table : String) : Boolean = {
        val metadata = CassandraConnector.getSession.getMetadata;
        val ksObj = metadata.getKeyspace(keyspace)
        return if ksObj.isPresent then return ksObj.get.getTable(table).isPresent else false
    }
}