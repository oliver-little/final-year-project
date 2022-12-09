from __future__ import annotations

import protobuf.cassandra_source_pb2 as cassandra_source 
from .data_source import DataSource

class CassandraDataSource(DataSource):
    def __init__(self, keyspace : str, table : str, server_url : str = ""):
        self.keyspace = keyspace
        self.table = table
        self.server_url = server_url

    def to_protobuf(self) -> cassandra_source.CassandraDataSource:
        return cassandra_source.CassandraDataSource(server_url = self.server_url, table = self.table, keyspace = self.keyspace)