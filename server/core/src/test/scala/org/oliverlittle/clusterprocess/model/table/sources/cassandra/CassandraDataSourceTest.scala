package org.oliverlittle.clusterprocess.model.table.sources.cassandra

import org.oliverlittle.clusterprocess.UnitSpec
import org.oliverlittle.clusterprocess.connector.cassandra.CassandraConnector
import org.oliverlittle.clusterprocess.model.table.TableResultHeader
import org.oliverlittle.clusterprocess.model.table.field._
import org.oliverlittle.clusterprocess.connector.cassandra.token._
import org.oliverlittle.clusterprocess.data_source

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar

import com.datastax.oss.driver.api.core.metadata.schema.{TableMetadata, ColumnMetadata}
import com.datastax.oss.driver.api.core.{CqlIdentifier}
import com.datastax.oss.driver.api.core.`type`.{DataTypes, DataType}
import com.datastax.oss.driver.api.core.cql.Row

import java.util.{Arrays, Collections}

class CassandraDataSourceTest extends UnitSpec with MockitoSugar {
    "A CassandraDataSource" should "infer from Cassandra correctly" in {
        val mockConnector = mock[CassandraConnector]
        val mockTableMetadata = mock[TableMetadata]
        when(mockConnector.getTableMetadata("test", "test_table")).thenReturn(mockTableMetadata)
        val mockCqlIdentifier = mock[CqlIdentifier]
        val mockColumnMetadata = mock[ColumnMetadata]
        when(mockTableMetadata.getColumns).thenReturn(Collections.singletonMap(mockCqlIdentifier, mockColumnMetadata))
        when(mockTableMetadata.getPartitionKey).thenReturn(Arrays.asList(mockColumnMetadata))
        when(mockTableMetadata.getPrimaryKey).thenReturn(Arrays.asList())
        when(mockCqlIdentifier.asInternal).thenReturn("fieldName")
        when(mockColumnMetadata.getType).thenReturn(DataTypes.BIGINT)
        when(mockColumnMetadata.getName).thenReturn(mockCqlIdentifier)

        val table = CassandraDataSource.inferDataSourceFromCassandra(mockConnector, "test", "test_table")       

        table should be (CassandraDataSource(mockConnector, "test", "test_table", Seq(CassandraIntField("fieldName")), Seq("fieldName"), Seq(), None))
    }

    it should "generate field names based on field definitions" in {
        val mockConnector = mock[CassandraConnector]
        CassandraDataSource(mockConnector, "test", "test_table", Seq(CassandraIntField("field1"), CassandraBoolField("field2")), Seq("field1"), Seq(), None).names should be (Set("field1", "field2"))
    }

    it should "throw an error if there are any duplicate field names" in {
        val mockConnector = mock[CassandraConnector]
        assertThrows[IllegalArgumentException] {
            CassandraDataSource(mockConnector, "test", "test_table", Seq(CassandraIntField("field1"), CassandraBoolField("field1")), Seq("field1"), Seq(), None)
        }
    }

    it should "throw an error if there are 0 partition keys" in {
        val mockConnector = mock[CassandraConnector]
        assertThrows[IllegalArgumentException] {
            CassandraDataSource(mockConnector, "test", "test_table", Seq(CassandraIntField("field1"), CassandraBoolField("field2")), Seq(), Seq(), None)
        }
    }

    it should "throw an error if a partition key also appears as a primary key" in {
        val mockConnector = mock[CassandraConnector]
        assertThrows[IllegalArgumentException] {
            CassandraDataSource(mockConnector, "test", "test_table", Seq(CassandraIntField("field1"), CassandraBoolField("field2")), Seq("field1"), Seq("field1"), None)
        }
    }

    it should "throw an error if a partition key is not in the field list" in {
        val mockConnector = mock[CassandraConnector]
        assertThrows[IllegalArgumentException] {
            CassandraDataSource(mockConnector, "test", "test_table", Seq(CassandraIntField("field1"), CassandraBoolField("field2")), Seq("field3"), Seq(), None)
        }
    }

    it should "throw an error if a primary key is not in the field list" in {
        val mockConnector = mock[CassandraConnector]
        assertThrows[IllegalArgumentException] {
            CassandraDataSource(mockConnector, "test", "test_table", Seq(CassandraIntField("field1"), CassandraBoolField("field2")), Seq("field1"), Seq("field3"), None)
        }
    }

    it should "generate the correct TableResultHeader" in {
        val mockConnector = mock[CassandraConnector]
        CassandraDataSource(mockConnector, "test", "test_table", Seq(CassandraIntField("field1"), CassandraBoolField("field2")), Seq("field1"), Seq(), None).getHeaders should be (TableResultHeader(Seq(CassandraIntField("field1"), CassandraBoolField("field2"))))
    }

    it should "convert to a DataSource protobuf correctly" in {
        val mockConnector = mock[CassandraConnector]
        CassandraDataSource(mockConnector, "test", "test_table", Seq(CassandraIntField("field1"), CassandraBoolField("field2")), Seq("field1"), Seq(), None).protobuf should be (data_source.DataSource().withCassandra(data_source.CassandraDataSource("test", "test_table")))
    }

    it should "convert to a protobuf correctly" in {
        val mockConnector = mock[CassandraConnector]
        CassandraDataSource(mockConnector, "test", "test_table", Seq(CassandraIntField("field1"), CassandraBoolField("field2")), Seq("field1"), Seq(), None).getCassandraProtobuf should be (Some(data_source.CassandraDataSource("test", "test_table")))
    }

    it should "convert to a CQL query" in {
        val mockConnector = mock[CassandraConnector]
        CassandraDataSource(mockConnector, "test", "test_table", Seq(CassandraIntField("field1"), CassandraBoolField("field2")), Seq("field1"), Seq(), None).getDataQuery should be ("SELECT * FROM test.test_table;")
        CassandraDataSource(mockConnector, "test", "test_table", Seq(CassandraIntField("field1"), CassandraBoolField("field2")), Seq("field1"), Seq(), Some(CassandraTokenRange(CassandraToken(0), CassandraToken(100)))).getDataQuery should be ("SELECT * FROM test.test_table WHERE token(field1) > 0 AND token(field1) <= 100;")
    }

    it should "convert to a CQL create table statement" in {
        val mockConnector = mock[CassandraConnector]
        CassandraDataSource(mockConnector, "test", "test_table", Seq(CassandraIntField("field1"), CassandraBoolField("field2")), Seq("field1"), Seq(), None).toCql() should be ("CREATE TABLE IF NOT EXISTS test.test_table (field1 bigint,field2 boolean, PRIMARY KEY (field1));")
        CassandraDataSource(mockConnector, "test", "test_table", Seq(CassandraIntField("field1"), CassandraBoolField("field2")), Seq("field1"), Seq(), None).toCql(false) should be ("CREATE TABLE test.test_table (field1 bigint,field2 boolean, PRIMARY KEY (field1));")
        CassandraDataSource(mockConnector, "test", "test_table", Seq(CassandraIntField("field1"), CassandraBoolField("field2")), Seq("field1"), Seq("field2"), None).toCql() should be ("CREATE TABLE IF NOT EXISTS test.test_table (field1 bigint,field2 boolean, PRIMARY KEY (field1,field2));")
        CassandraDataSource(mockConnector, "test", "test_table", Seq(CassandraIntField("field1"), CassandraBoolField("field2")), Seq("field1", "field2"), Seq(), None).toCql() should be ("CREATE TABLE IF NOT EXISTS test.test_table (field1 bigint,field2 boolean, PRIMARY KEY ((field1,field2)));")
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