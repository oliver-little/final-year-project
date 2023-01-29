from __future__ import annotations

import cluster_client.protobuf.data_source_pb2 as data_source_pb2

class DataSource():
    def to_protobuf() -> data_source_pb2.DataSource:
        raise NotImplementedError("DataSource is an abstract class, this must be implemented by children")
    
class CassandraDataSource(DataSource):
    def __init__(self, keyspace : str, table : str, server_url : str = ""):
        self.keyspace = keyspace
        self.table = table
        self.server_url = server_url

    def to_protobuf(self) -> data_source_pb2.DataSource:
        return data_source_pb2.DataSource(cassandra=data_source_pb2.CassandraDataSource(table = self.table, keyspace = self.keyspace))