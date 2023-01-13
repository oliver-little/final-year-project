package org.oliverlittle.clusterprocess.cassandra

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata
import com.datastax.oss.driver.api.core.auth.{AuthProvider, ProgrammaticPlainTextAuthProvider}
import java.net.InetSocketAddress
import scala.io.Source.fromFile
import scala.util.Properties.{envOrElse, envOrNone}

object CassandraConnector {
    var host : String = envOrElse("CASSANDRA_URL", "localhost")
    var port : Int = envOrElse("CASSANDRA_PORT", "9042").toInt
    var datacenter : String = envOrElse("CASSANDRA_DATACENTER", "datacenter1")

    var usernameFile : Option[String] = envOrNone("CASSANDRA_USERNAME_FILE")
    var passwordFile : Option[String] = envOrNone("CASSANDRA_PASSWORD_FILE")
    // Get username and password from file if they exist
    var username : Option[String] = usernameFile.flatMap(f => Some(fromFile(f).getLines.mkString))
    var password : Option[String] = passwordFile.flatMap(f => Some(fromFile(f).getLines.mkString))
    var authProvider : Option[AuthProvider] = if username.isDefined && password.isDefined then Some(new ProgrammaticPlainTextAuthProvider(username.get, password.get)) else None

    // Procedure style syntax to put the auth provider in if it exists
    private var sessionBuilder = CqlSession.builder().addContactPoint(new InetSocketAddress(host, port)).withLocalDatacenter(datacenter)
    if authProvider.isDefined then sessionBuilder.withAuthProvider(authProvider.get)
    private var session : CqlSession = sessionBuilder.build()

    def getSession : CqlSession = session

    def verifyConnection : Boolean = !session.isClosed

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