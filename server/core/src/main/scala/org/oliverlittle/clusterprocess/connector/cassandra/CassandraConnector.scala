package org.oliverlittle.clusterprocess.connector.cassandra

import org.oliverlittle.clusterprocess.connector.PingLatency

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata
import com.datastax.oss.driver.api.core.auth.{AuthProvider, ProgrammaticPlainTextAuthProvider}
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.net.InetSocketAddress
import scala.io.Source.fromFile
import scala.util.Properties.{envOrElse, envOrNone}

object CassandraConfig:
    lazy val connector = CassandraConnector()

    def apply() : CassandraConfig = ConfigHolder(connector)

    case class ConfigHolder(connector : CassandraConnector) extends CassandraConfig

trait CassandraConfig extends Selectable:
    def connector : CassandraConnector

object CassandraConnector:
    def getHost(port : Int) : String = {
        val url = envOrNone("CASSANDRA_URL")
        val baseURL = envOrNone("CASSANDRA_BASE_URL")
        if url.isDefined then return url.get
        else if baseURL.isDefined then return selectClosestNodeURL(baseURL.get, envOrElse("CASSANDRA_NODE_NAME", "demo-dc1-default-sts"), envOrElse("NUM_CASSANDRA_NODES", "1").toInt, port).getOrElse("localhost")
        else return "localhost"
    }

    def selectClosestNodeURL(baseURL : String, nodeName : String, numNodes : Int, port : Int) : Option[String] = 
        (0 to numNodes - 1).map(nodeName + "-" + _.toString + "." + baseURL) // For each possible URL, generate the full string
            .map((url : String) => (url, PingLatency.getLatencyToURL(url, port))) // Then get the corresponding latency for that URL
            .filter(_._2.nonEmpty).sortBy(_._2) // Remove any failed connections and sort by latency ascending
            .headOption // Get the first element if it exists
            .flatMap((url : String, _) => Some(url)) // Get the URL portion of the tuple
    
    def verifyConnection : Boolean = !CassandraConnector().getSession.isClosed

case class CassandraConnector(
    socket : InetSocketAddress = new InetSocketAddress(getHost(envOrElse("CASSANDRA_PORT", "9042").toInt), envOrElse("CASSANDRA_PORT", "9042").toInt),
    datacenter : String = envOrElse("CASSANDRA_DATACENTER", "datacenter1"),
    usernameFile : Option[String] = envOrNone("CASSANDRA_USERNAME_FILE"),
    passwordFile : Option[String] = envOrNone("CASSANDRA_PASSWORD_FILE"),
        ) {

    private val logger = LoggerFactory.getLogger("CassandraConnector")

    // Get username and password from file if they exist
    var username : Option[String] = usernameFile.flatMap(f => Some(fromFile(f).getLines.mkString))
    var password : Option[String] = passwordFile.flatMap(f => Some(fromFile(f).getLines.mkString))
    var authProvider : Option[AuthProvider] = if username.isDefined && password.isDefined then Some(new ProgrammaticPlainTextAuthProvider(username.get, password.get)) else None

    // Procedure style syntax to put the auth provider in if it exists
    private var sessionBuilder = CqlSession.builder().addContactPoint(socket).withLocalDatacenter(datacenter)
    if authProvider.isDefined then sessionBuilder.withAuthProvider(authProvider.get)
    private var session : CqlSession = sessionBuilder.build()

    logger.info(f"Connected to ${socket.getHostName}%s:${socket.getPort}%s, datacenter $datacenter%s.")

    def getSession : CqlSession = session

    def getTableMetadata(keyspace : String, table : String) : TableMetadata = {
        val ksObj = getSession.getMetadata.getKeyspace(keyspace)
        if ksObj.isPresent then
            val tableObj = ksObj.get.getTable(table)
            if tableObj.isPresent then
                return tableObj.get
            throw new IllegalArgumentException("Table " + keyspace + "." + table + " not found.")
        throw new IllegalArgumentException("Keyspace " + keyspace + " not found.")
    }

    def hasTable(keyspace : String, table : String) : Boolean = {
        val metadata = getSession.getMetadata;
        val ksObj = metadata.getKeyspace(keyspace)
        return if ksObj.isPresent then return ksObj.get.getTable(table).isPresent else false
    }
}