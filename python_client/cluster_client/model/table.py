from __future__ import annotations
from typing import List

from cluster_client.connector.cluster import ClusterConnector
from .aggregate_expressions import AggregateExpression
from .field_expressions import FieldExpression, FieldComparison
from .table_transformation import *
from .data_source import *
from .result_builder import StreamedTableResultBuilder
import cluster_client.protobuf.table_model_pb2 as table_model_pb2
import cluster_client.protobuf.client_query_pb2 as client_query_pb2


class Table():
    def __init__(self, connector : ClusterConnector, data_source : DataSource, transformations : List[TableTransformation]):
        self.connector = connector
        self.data_source = data_source
        self.transformations = transformations

    def evaluate(self) -> StreamedTableResultBuilder:
        """Evaluates this table on the cluster"""
        iterator = self.connector.table_client_service.ComputeTable(client_query_pb2.ComputeTableRequest(table=self.to_protobuf()))
        return StreamedTableResultBuilder(iterator)

    def select(self, *select_columns : FieldExpression) -> Table:
        """Creates a new table, applying a select operation:
        E.G: `table.select(F("col1"), F("col2"))`
            or `table.select(Name(F("col1") + F("col2"), "newCol"), F("col3") + " hello")`"""

        return Table(self.connector, self.data_source, self.transformations + [SelectTransformation(*select_columns)])

    def filter(self, filter : FieldComparison) -> Table:
        """Creates a new table, applying a filter operation:
        E.G: `table.filter(F("col1") < F("col2"))`
            or `table.filter((F("col1").contains("hello"))`"""

        return Table(self.connector, self.data_source, self.transformations + [FilterTransformation(filters)])

    def join(self, join_type : str, join_table_name : str) -> Table:
        """Creates a new table by joining this table to another by name
        E.G: `table.join("INNER_JOIN", "table_name")`"""

        return Table(self.connector, self.data_source, self.transformations + [JoinTransformation(join_type, join_table_name)])

    def full_outer_join(self, join_table_name : str) -> Table:
        """Shortcut to `.join("FULL_OUTER_JOIN", "table_name")`"""

        return Table(self.connector, self.data_source, self.transformations + [JoinTransformation("FULL_OUTER_JOIN", join_table_name)])

    def inner_join(self, join_table_name : str) -> Table:
        """Shortcut to `.join("INNER_JOIN", "table_name")`"""

        return Table(self.connector, self.data_source, self.transformations + [JoinTransformation("INNER_JOIN", join_table_name)])

    def left_join(self, join_table_name : str) -> Table:
        """Shortcut to `.join("LEFT_JOIN", "table_name")`"""

        return Table(self.connector, self.data_source, self.transformations + [JoinTransformation("LEFT_JOIN", join_table_name)])

    def right_join(self, join_table_name : str) -> Table:
        """Shortcut to `.join("RIGHT_JOIN", "table_name")`"""

        return Table(self.connector, self.data_source, self.transformations + [JoinTransformation("RIGHT_JOIN", join_table_name)])

    def cross_join(self, join_table_name : str) -> Table:
        """Shortcut to `.join("CROSS_JOIN", "table_name")`"""

        return Table(self.connector, self.data_source, self.transformations + [JoinTransformation("CROSS_JOIN", join_table_name)])

    def group_by(self, unique_fields : List[NamedFieldExpression], aggregate_fields : List[AggregateExpression]) -> Table:
        """Creates a new table, grouping any results in future by the provided columns"""

        return Table(self.connector, GroupByDataSource(self, unique_fields, aggregate_fields), [])

    def aggregate(self, *aggregate_columns : AggregateExpression) -> Table:
        """Creates a new table, aggregating the table data by the provided columns"""

        return Table(self.connector, self.data_source, self.transformations + [AggregateTransformation(*aggregate_columns)])

    def agg(self, *aggregate_columns : AggregateExpression) -> Table:
        """Creates a new table, aggregating the table data by the provided columns.
        Identical to calling `.aggregate()`"""

        self.aggregate(*aggregate_columns)

    def order_by(self, *order_by_columns : Tuple(FieldExpression, str)) -> Table:
        """Creates a new table, ordering the results by the provided columns"""

        return Table(self.connector, self.data_source, self.transformations + [OrderByTransformation(*order_by_columns)])

    def window(self, window_functions : List[FieldExpression], partition_fields : List[str], order_by : List(Tuple(FieldExpression, str)) = None) -> Table:
        """Creates a new table, calculating aggregate functions over a window of fields, optionally ordered by the specified columns"""

        return Table(self.connector, self.data_source, self.transformations + [WindowTransformation(window_functions, partition_fields, order_by)])

    def to_protobuf(self) -> table_model_pb2.Table:
        """Converts this table's Python representation to a Protobuf representation"""
        return table_model_pb2.Table(data_source = self.data_source.to_protobuf(), transformations = [transformation.to_protobuf() for transformation in self.transformations])

    def __str__(self):
        transformation_data = ""
        for transformation in self.transformations:
            transformation_data += str(transformation) + "\n"
        transformation_data = transformation_data[:-1]

        if transformation_data == "":
            transformation_data = "No transformations."
        return f"Table:\n{transformation_data}"