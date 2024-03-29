package org.oliverlittle.clusterprocess.model.table.sources.cassandra

import org.oliverlittle.clusterprocess.UnitSpec
import org.oliverlittle.clusterprocess.connector.grpc.{WorkerHandler, ChannelManager}
import org.oliverlittle.clusterprocess.connector.cassandra.{CassandraConfig, CassandraConnector}
import org.oliverlittle.clusterprocess.model.table.TableResultHeader
import org.oliverlittle.clusterprocess.model.table.field._
import org.oliverlittle.clusterprocess.model.table.sources.cassandra.PartialCassandraDataSource
import org.oliverlittle.clusterprocess.connector.cassandra.token._
import org.oliverlittle.clusterprocess.table_model

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar

import com.datastax.oss.driver.api.core.metadata.schema.{TableMetadata, ColumnMetadata}
import com.datastax.oss.driver.api.core.{CqlIdentifier}
import com.datastax.oss.driver.api.core.`type`.{DataTypes, DataType}
import com.datastax.oss.driver.api.core.cql.Row
import akka.actor.ActorRef
import com.datastax.oss.driver.api.core.CqlSession
import java.util.{Arrays, Collections}

object MockCassandraConfig:
    def apply() : CassandraConfig = MockConfigHolder(mock(classOf[CassandraConnector]))

    case class MockConfigHolder(connector : CassandraConnector) extends CassandraConfig

class CassandraDataSourceTest extends UnitSpec with MockitoSugar {
    "A CassandraDataSource" should "infer from Cassandra correctly" in {
        val mockConfig = MockCassandraConfig()
        val mockConnector = mockConfig.connector
        val mockTableMetadata = mock[TableMetadata]
        when(mockConnector.getTableMetadata("test", "test_table")).thenReturn(mockTableMetadata)
        val mockCqlIdentifier = mock[CqlIdentifier]
        val mockColumnMetadata = mock[ColumnMetadata]
        when(mockTableMetadata.getColumns).thenReturn(Collections.singletonMap(mockCqlIdentifier, mockColumnMetadata))
        when(mockTableMetadata.getPartitionKey).thenReturn(Arrays.asList(mockColumnMetadata))
        when(mockTableMetadata.getPrimaryKey).thenReturn(Arrays.asList(mockColumnMetadata))
        when(mockCqlIdentifier.asInternal).thenReturn("fieldName")
        when(mockColumnMetadata.getType).thenReturn(DataTypes.BIGINT)
        when(mockColumnMetadata.getName).thenReturn(mockCqlIdentifier)

        val table = CassandraDataSource.inferDataSourceFromCassandra("test", "test_table")(using mockConfig)       

        table should be (CassandraDataSource(mockConfig, "test", "test_table", Seq(CassandraIntField("fieldName")), Seq("fieldName"), Seq("fieldName")))
    }

    it should "generate field names based on field definitions" in {
        val mockConfig = MockCassandraConfig()
        val mockConnector = mockConfig.connector
        CassandraDataSource(mockConfig, "test", "test_table", Seq(CassandraIntField("field1"), CassandraBoolField("field2")), Seq("field1"), Seq("field1")).names should be (Set("field1", "field2"))
    }

    it should "throw an error if there are any duplicate field names" in {
        val mockConfig = MockCassandraConfig()
        val mockConnector = mockConfig.connector
        assertThrows[IllegalArgumentException] {
            CassandraDataSource(mockConfig, "test", "test_table", Seq(CassandraIntField("field1"), CassandraBoolField("field1")), Seq("field1"), Seq("field1"))
        }
    }

    it should "throw an error if there are 0 partition keys" in {
        val mockConfig = MockCassandraConfig()
        val mockConnector = mockConfig.connector
        assertThrows[IllegalArgumentException] {
            CassandraDataSource(mockConfig, "test", "test_table", Seq(CassandraIntField("field1"), CassandraBoolField("field2")), Seq(), Seq())
        }
    }

    it should "throw an error if any partition keys do not also appear as primary keys" in {
        val mockConfig = MockCassandraConfig()
        val mockConnector = mockConfig.connector
        assertThrows[IllegalArgumentException] {
            CassandraDataSource(mockConfig, "test", "test_table", Seq(CassandraIntField("field1"), CassandraBoolField("field2")), Seq("field1"), Seq())
        }
    }

    it should "throw an error if a partition key is not in the field list" in {
        val mockConfig = MockCassandraConfig()
        val mockConnector = mockConfig.connector
        assertThrows[IllegalArgumentException] {
            CassandraDataSource(mockConfig, "test", "test_table", Seq(CassandraIntField("field1"), CassandraBoolField("field2")), Seq("field3"), Seq("field3"))
        }
    }

    it should "throw an error if a primary key is not in the field list" in {
        val mockConfig = MockCassandraConfig()
        val mockConnector = mockConfig.connector
        assertThrows[IllegalArgumentException] {
            CassandraDataSource(mockConfig, "test", "test_table", Seq(CassandraIntField("field1"), CassandraBoolField("field2")), Seq("field1"), Seq("field1", "field3"))
        }
    }

    it should "correctly calculate the clustering keys from the partition and primary key list" in {
        val mockConfig = MockCassandraConfig()
        val mockConnector = mockConfig.connector
        CassandraDataSource(mockConfig, "test", "test_table", Seq(CassandraIntField("field1"), CassandraBoolField("field2")), Seq("field1"), Seq("field1")).clusterKeys should be (Seq())
        CassandraDataSource(mockConfig, "test", "test_table", Seq(CassandraIntField("field1"), CassandraBoolField("field2")), Seq("field1"), Seq("field1", "field2")).clusterKeys should be (Seq("field2"))
        CassandraDataSource(mockConfig, "test", "test_table", Seq(CassandraIntField("field1"), CassandraBoolField("field2")), Seq("field1", "field2"), Seq("field1", "field2")).clusterKeys should be (Seq())
    
    }

    it should "generate the correct TableResultHeader" in {
        val mockConfig = MockCassandraConfig()
        val mockConnector = mockConfig.connector
        CassandraDataSource(mockConfig, "test", "test_table", Seq(CassandraIntField("field1"), CassandraBoolField("field2")), Seq("field1"), Seq("field1")).getHeaders should be (TableResultHeader(Seq(CassandraIntField("field1"), CassandraBoolField("field2"))))
    }

    it should "convert to a DataSource protobuf correctly" in {
        val mockConfig = MockCassandraConfig()
        val mockConnector = mockConfig.connector
        CassandraDataSource(mockConfig, "test", "test_table", Seq(CassandraIntField("field1"), CassandraBoolField("field2")), Seq("field1"), Seq("field1")).protobuf should be (table_model.DataSource().withCassandra(table_model.CassandraDataSource("test", "test_table")))
    }

    it should "convert to a CQL create table statement" in {
        val mockConfig = MockCassandraConfig()
        val mockConnector = mockConfig.connector
        CassandraDataSource(mockConfig, "test", "test_table", Seq(CassandraIntField("field1"), CassandraBoolField("field2")), Seq("field1"), Seq("field1")).toCql() should be ("CREATE TABLE IF NOT EXISTS test.test_table (field1 bigint,field2 boolean, PRIMARY KEY (field1));")
        CassandraDataSource(mockConfig, "test", "test_table", Seq(CassandraIntField("field1"), CassandraBoolField("field2")), Seq("field1"), Seq("field1")).toCql(false) should be ("CREATE TABLE test.test_table (field1 bigint,field2 boolean, PRIMARY KEY (field1));")
        CassandraDataSource(mockConfig, "test", "test_table", Seq(CassandraIntField("field1"), CassandraBoolField("field2")), Seq("field1"), Seq("field1", "field2")).toCql() should be ("CREATE TABLE IF NOT EXISTS test.test_table (field1 bigint,field2 boolean, PRIMARY KEY (field1,field2));")
        CassandraDataSource(mockConfig, "test", "test_table", Seq(CassandraIntField("field1"), CassandraBoolField("field2")), Seq("field1", "field2"), Seq("field1", "field2")).toCql() should be ("CREATE TABLE IF NOT EXISTS test.test_table (field1 bigint,field2 boolean, PRIMARY KEY ((field1,field2)));")
    }
}

class PartialCassandraDataSourceTest extends UnitSpec with MockitoSugar {

    val partition = CassandraPartition(Seq(CassandraTokenRange.fromLong(0, 1)))

    "A PartialCassandraDataSource" should "convert to protobuf correctly" in {
        val mockConfig = MockCassandraConfig()
        val mockConnector = mockConfig.connector
        val cassandraDataSource = CassandraDataSource(mockConfig, "test", "test_table", Seq(CassandraIntField("field1"), CassandraBoolField("field2")), Seq("field1"), Seq("field1"))
        PartialCassandraDataSource(cassandraDataSource, partition).protobuf should be (table_model.PartialDataSource().withCassandra(table_model.PartialCassandraDataSource("test", "test_table", partition.protobuf)))
    }

    it should "produce a number of data queries for each TokenRange" in {
        val mockConfig = MockCassandraConfig()
        val mockConnector = mockConfig.connector
        val cassandraDataSource = CassandraDataSource(mockConfig, "test", "test_table", Seq(CassandraIntField("field1"), CassandraBoolField("field2")), Seq("field1"), Seq("field1"))
        PartialCassandraDataSource(cassandraDataSource, partition).getDataQueries should be (Seq("SELECT * FROM test.test_table WHERE token(field1) > 0 AND token(field1) <= 1;"))
    }
}

class CassandraFieldTest extends UnitSpec with MockitoSugar {
    "A CassandraField" should "convert to CQL identifier" in {
        CassandraIntField("fieldName").toCql should be ("fieldName bigint")
    }

    it should "extract non-null values" in {
        val mockRow = mock[Row]
        when(mockRow.isNull("fieldName")).thenReturn(false)
        when(mockRow.getLong("fieldName")).thenReturn(1L)
        CassandraIntField("fieldName").getTableValue(mockRow) should be (Some(IntValue(1)))
    }

    it should "extract null values as None" in {
        val mockRow = mock[Row]
        when(mockRow.isNull("fieldName")).thenReturn(true)
        CassandraIntField("fieldName").getTableValue(mockRow) should be (None)
    }
}