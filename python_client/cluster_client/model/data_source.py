from __future__ import annotations

import cluster_client.protobuf.table_model_pb2 as table_model_pb2

class DataSource():
    def to_protobuf() -> table_model_pb2.DataSource:
        raise NotImplementedError("DataSource is an abstract class, this must be implemented by children")
    
class CassandraDataSource(DataSource):
    def __init__(self, keyspace : str, table : str, server_url : str = ""):
        self.keyspace = keyspace
        self.table = table
        self.server_url = server_url

    def to_protobuf(self) -> table_model_pb2.DataSource:
        return table_model_pb2.DataSource(cassandra=table_model_pb2.CassandraDataSource(table = self.table, keyspace = self.keyspace))