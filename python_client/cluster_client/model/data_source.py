from __future__ import annotations
from typing import List

import cluster_client.model.table
from .aggregate_expressions import AggregateExpression
from .field_expressions import NamedFieldExpression, FieldExpression
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
    
class GroupByDataSource(DataSource):
    def __init__(self, table : cluster_client.model.table.Table, unique_fields : List[NamedFieldExpression], aggregate_fields : List[AggregateExpression]):
        self.table = table
        self.unique_fields = unique_fields
        self.aggregate_fields = aggregate_fields

    def to_protobuf(self) -> table_model_pb2.DataSource:
        return table_model_pb2.DataSource(
            group_by=table_model_pb2.GroupByDataSource(
                table=self.table.to_protobuf(), 
                unique_fields=[unique_field.to_named_protobuf() for unique_field in self.unique_fields],
                aggregate_fields=[aggregate_field.to_protobuf() for aggregate_field in self.aggregate_fields]
            )
        )