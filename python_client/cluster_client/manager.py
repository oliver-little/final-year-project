from __future__ import annotations
from typing import List

import grpc

from cluster_client.connector.cluster import ClusterConnector
from cluster_client.model.data_source import DataSource, CassandraDataSource
from cluster_client.model.table_transformation import *
import cluster_client.protobuf.client_query_pb2 as client_query_pb2
import cluster_client.protobuf.data_source_pb2 as data_source_pb2

# ClusterManager("address").cassandraTable().

class ClusterManager():
    def __init__(self, address : str, port : int = 50051, credentials : grpc.ChannelCredentials = None) -> None:
        self.address = address
        self.port = port
        self.connector = ClusterConnector(address, str(port), credentials)
        self.connector.open()

    def cassandra_table(self, keyspace : str, table : str):
        return TableManager(self, CassandraDataSource(keyspace, table, self.address), [])

class TableManager():
    def __init__(self, cluster_manager : ClusterManager, data_source : DataSource, transformations : List[TableTransformation]) -> None:
        self.cluster_manager = cluster_manager
        self.data_source = data_source
        self.transformations = transformations

    def evaluate(self) -> None:
        """Evaluates this table on the cluster"""
        header = None
        rows = []
        for result in self.cluster_manager.connector.table_client_service.ComputeTable(client_query_pb2.ComputeTableRequest(data_source=self.data_source.to_protobuf(), table=self.transformations_to_protobuf())):
            if result.HasField("header"):
                if header is not None:
                    raise ValueError("Header sent twice by remote gRPC server.")
                else:
                    header = result.header
            else:
                rows.append(result.row)
        print(header)
        print(len(rows))
        print("Done.")

    def select(self, *select_columns : FieldExpression) -> TableManager:
        """Creates a new table, applying a select operation:
        E.G: `table.select(F("col1"), F("col2"))`
            or `table.select(Name(F("col1") + F("col2"), "newCol"), F("col3") + " hello")`"""

        return TableManager(self.cluster_manager, self.data_source, self.transformations + [SelectTransformation(*select_columns)])

    def filter(self, *filters : FieldComparison) -> TableManager:
        """Creates a new table, applying a filter operation:
        E.G: `table.filter(F("col1") < F("col2"))`
            or `table.filter((F("col1").contains("hello"))`"""

        return TableManager(self.transformations + [FilterTransformation(*filters)])

    def join(self, join_type : str, join_table_name : str) -> TableManager:
        """Creates a new table by joining this table to another by name
        E.G: `table.join("INNER_JOIN", "table_name")`"""

        return TableManager(self.transformations + [JoinTransformation(join_type, join_table_name)])

    def full_outer_join(self, join_table_name : str) -> TableManager:
        """Shortcut to `.join("FULL_OUTER_JOIN", "table_name")`"""

        return TableManager(self.transformations + [JoinTransformation("FULL_OUTER_JOIN", join_table_name)])

    def inner_join(self, join_table_name : str) -> TableManager:
        """Shortcut to `.join("INNER_JOIN", "table_name")`"""

        return TableManager(self.transformations + [JoinTransformation("INNER_JOIN", join_table_name)])

    def left_join(self, join_table_name : str) -> TableManager:
        """Shortcut to `.join("LEFT_JOIN", "table_name")`"""

        return TableManager(self.transformations + [JoinTransformation("LEFT_JOIN", join_table_name)])

    def right_join(self, join_table_name : str) -> TableManager:
        """Shortcut to `.join("RIGHT_JOIN", "table_name")`"""

        return TableManager(self.transformations + [JoinTransformation("RIGHT_JOIN", join_table_name)])

    def cross_join(self, join_table_name : str) -> TableManager:
        """Shortcut to `.join("CROSS_JOIN", "table_name")`"""

        return TableManager(self.transformations + [JoinTransformation("CROSS_JOIN", join_table_name)])

    def group_by(self, *group_by_columns : FieldExpression) -> TableManager:
        """Creates a new table, grouping any results in future by the provided columns"""

        return TableManager(self.transformations + [GroupByTransformation(*group_by_columns)])

    def aggregate(self, *aggregate_columns : FieldExpression) -> TableManager:
        """Creates a new table, aggregating the table data by the provided columns"""

        return TableManager(self.transformations + [AggregateTransformation(*aggregate_columns)])

    def agg(self, *aggregate_columns : FieldExpression) -> TableManager:
        """Creates a new table, aggregating the table data by the provided columns.
        Identical to calling `.aggregate()`"""

        self.aggregate(*aggregate_columns)

    def order_by(self, *order_by_columns : Tuple(FieldExpression, str)) -> TableManager:
        """Creates a new table, ordering the results by the provided columns"""

        return TableManager(self.transformations + [OrderByTransformation(*order_by_columns)])

    def window(self, window_functions : List[FieldExpression], partition_fields : List[str], order_by : List(Tuple(FieldExpression, str)) = None) -> TableManager:
        """Creates a new table, calculating aggregate functions over a window of fields, optionally ordered by the specified columns"""

        return TableManager(self.transformations + [WindowTransformation(window_functions, partition_fields, order_by)])

    def transformations_to_protobuf(self) -> protobuf_model.Table:
        """Converts this table's Python representation to a Protobuf representation"""
        return protobuf_model.Table(transformations = [transformation.to_protobuf() for transformation in self.transformations])

    def __str__(self):
        transformation_data = ""
        for transformation in self.transformations:
            transformation_data += str(transformation) + "\n"
        transformation_data = transformation_data[:-1]

        if transformation_data == "":
            transformation_data = "No transformations."
        return f"Table:\n{transformation_data}"