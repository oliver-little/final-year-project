from __future__ import annotations
from typing import List, Tuple

from .field_expressions import *
import cluster_client.protobuf.table_model_pb2 as protobuf_model

class TableTransformation():
    """Base class defining all transformations on a table"""
    def __init__(self):
        if type(self) == TableTransformation:
            raise NotImplementedError("TableTransformation is an abstract class and cannot be instantiated directly.")
    
    def to_protobuf() -> protobuf_model.Table.TableTransformation: 
        raise NotImplementedError("TableTransformation abstract class cannot be converted to protobuf.")

class SelectTransformation(TableTransformation):
    """Selects any number of columns from the tables, and applies any given field expressions"""
    def __init__(self, *select_columns : FieldExpression):
        self.select_columns = select_columns

    def to_protobuf(self) -> protobuf_model.Table.TableTransformation:
        return protobuf_model.Table.TableTransformation(select=protobuf_model.Select(fields=[col.to_protobuf() for col in self.select_columns]))

    def __str__(self):
        col_data = ""
        for col in self.select_columns:
            col_data += str(col) + ", "
        col_data = col_data[:-2]
        return f"Select({col_data})"

class FilterTransformation(TableTransformation):
    """Filters the table according to some Filter criteria"""
    def __init__(self, *filters: FieldComparison):
        self.filters = filters
        print(filters)
    
    def to_protobuf(self) -> protobuf_model.Table.TableTransformation:
        return protobuf_model.Table.TableTransformation(filter=protobuf_model.Filter(filters=[filter.to_protobuf() for filter in self.filters]))

    def __str__(self):
        filter_data = ""

        for filter in self.filters:
            filter_data += str(filter) + ", "
        filter_data = filter_data[:-2]
        return f"Filter({filter_data})"

class JoinTransformation(TableTransformation):
    def __init__(self, join_type : str,  join_table_name : str):
        self.join_table_name = join_table_name

        try:
            self.join_type = protobuf_model.Join.JoinType.Value(join_type.upper())
        except ValueError:
            raise ValueError(f"Invalid join type provided {join_type}")

    def to_protobuf(self) -> protobuf_model.Table.TableTransformation:
        return protobuf_model.Table.TableTransformation(join=
            protobuf_model.Join(join_type=self.join_type, table_name=self.join_table_name)
        )

    def __str__(self):
        return f"Join({self.join_type}, {self.join_table_name})"

class GroupByTransformation(TableTransformation):
    """Groups future calculations by the provided expressions"""
    def __init__(self, *group_by_columns : FieldExpression):
        self.group_by_columns = group_by_columns

    def to_protobuf(self) -> protobuf_model.Table.TableTransformation:
        return protobuf_model.Table.TableTransformation(group_by=protobuf_model.GroupBy(fields=[col.to_protobuf() for col in self.group_by_columns]))

    def __str__(self):
        group_data = ""
        for group_by_column in self.group_by_columns:
            group_data += str(group_by_column) + ", "
        group_data = group_data[:-2]
        return f"Filter({group_data})"

class AggregateTransformation(TableTransformation):
    """Aggregates the data in the table by the provided expressions"""
    def __init__(self, *aggregate_columns : FieldExpression):
        self.aggregate_columns = aggregate_columns

    def to_protobuf(self) -> protobuf_model.Table.TableTransformation:
        return protobuf_model.Table.TableTransformation(aggregate=protobuf_model.Aggregate(fields=[col.to_protobuf() for col in self.aggregate_columns]))

    def __str__(self):
        aggregate_data = ""
        for aggregate_column in self.aggregate_columns:
            aggregate_data += str(aggregate_column) + ", "
        aggregate_data = aggregate_data[:-2]
        return f"Aggregate({aggregate_data})"

class OrderByTransformation(TableTransformation):
    """Orders the output results by the provided expressions"""
    def __init__(self, *order_by_columns : Tuple(FieldExpression, str)):
        converted_pairs = []
        for expression, order_by_type in order_by_columns:
            try:
                converted_pairs.append((expression, protobuf_model.OrderBy.OrderByType.Value(order_by_type)))
            except ValueError:
                raise ValueError(f"Invalid order by type provided {order_by_type}")

        self.order_by_columns = converted_pairs

    def to_protobuf(self) -> protobuf_model.Table.TableTransformation:
        return protobuf_model.Table.TableTransformation(order_by=protobuf_model.Order(order_by_fields=[
            protobuf_model.Order.OrderByField(field=expression.to_protobuf(), order_by_type=order_by_type) for expression, order_by_type in self.order_by_columns
        ]))

    def __str__(self):
        order_by_data = ""
        for order_by_column in self.order_by_columns:
            order_by_data += str(order_by_column) + ", "
        order_by_data = order_by_data[:-2]
        return f"OrderBy({order_by_data})"

class WindowTransformation(TableTransformation):
    """Performs a calculation over a window function"""
    def __init__(self, window_functions : List[FieldExpression], partition_fields : List[str], order_by : List(Tuple(FieldExpression, str)) = None):
        self.window_functions = window_functions
        self.partition_fields = partition_fields
        if order_by is not None:
            self.order_by = OrderByTransformation(order_by)

    def to_protobuf(self) -> protobuf_model.Table.TableTransformation:
        return protobuf_model.Table.TableTransformation(window=
            protobuf_model.Window(
                window_function=[window_function.to_protobuf() for window_function in self.window_functions],
                partition_fields=self.partition_fields,
                order_by_fields=self.order_by
            )
        )

    def __str__(self):
        window_data = ""
        for window_function in self.window_functions:
            window_data += str(window_function) + ", "
        window_data = window_data[:-2]

        partition_data = ""
        for partition_fields in self.partition_fields:
            partition_data += str(partition_fields) + ", "
        partition_data = partition_data[:-2]

        order_by_data = ""
        if self.order_by is not None:
            order_by_data = str(self.order_by)

        return f"Window({window_data}) over ({partition_data}) {order_by_data}"


class Table():
    """Base Representation of a table in the system. All operations are non-destructive, creating a new instance"""
    def __init__(self, transformations : List[TableTransformation] = []):
        super().__init__()
        self.transformations = transformations
    
    def select(self, *select_columns : FieldExpression) -> Table:
        """Creates a new table, applying a select operation:
        E.G: `table.select(F("col1"), F("col2"))`
            or `table.select(Name(F("col1") + F("col2"), "newCol"), F("col3") + " hello")`"""

        return Table(self.transformations + [SelectTransformation(*select_columns)])

    def filter(self, *filters : FieldComparison) -> Table:
        """Creates a new table, applying a filter operation:
        E.G: `table.filter(F("col1") < F("col2"))`
            or `table.filter((F("col1").contains("hello"))`"""

        return Table(self.transformations + [FilterTransformation(*filters)])

    def join(self, join_type : str, join_table_name : str) -> Table:
        """Creates a new table by joining this table to another by name
        E.G: `table.join("INNER_JOIN", "table_name")`"""

        return Table(self.transformations + [JoinTransformation(join_type, join_table_name)])

    def full_outer_join(self, join_table_name : str) -> Table:
        """Shortcut to `.join("FULL_OUTER_JOIN", "table_name")`"""

        return Table(self.transformations + [JoinTransformation("FULL_OUTER_JOIN", join_table_name)])

    def inner_join(self, join_table_name : str) -> Table:
        """Shortcut to `.join("INNER_JOIN", "table_name")`"""

        return Table(self.transformations + [JoinTransformation("INNER_JOIN", join_table_name)])

    def left_join(self, join_table_name : str) -> Table:
        """Shortcut to `.join("LEFT_JOIN", "table_name")`"""

        return Table(self.transformations + [JoinTransformation("LEFT_JOIN", join_table_name)])

    def right_join(self, join_table_name : str) -> Table:
        """Shortcut to `.join("RIGHT_JOIN", "table_name")`"""

        return Table(self.transformations + [JoinTransformation("RIGHT_JOIN", join_table_name)])

    def cross_join(self, join_table_name : str) -> Table:
        """Shortcut to `.join("CROSS_JOIN", "table_name")`"""

        return Table(self.transformations + [JoinTransformation("CROSS_JOIN", join_table_name)])

    def group_by(self, *group_by_columns : FieldExpression) -> Table:
        """Creates a new table, grouping any results in future by the provided columns"""

        return Table(self.transformations + [GroupByTransformation(*group_by_columns)])

    def aggregate(self, *aggregate_columns : FieldExpression) -> Table:
        """Creates a new table, aggregating the table data by the provided columns"""

        return Table(self.transformations + [AggregateTransformation(*aggregate_columns)])

    def agg(self, *aggregate_columns : FieldExpression) -> Table:
        """Creates a new table, aggregating the table data by the provided columns.
        Identical to calling `.aggregate()`"""

        self.aggregate(*aggregate_columns)

    def order_by(self, *order_by_columns : Tuple(FieldExpression, str)) -> Table:
        """Creates a new table, ordering the results by the provided columns"""

        return Table(self.transformations + [OrderByTransformation(*order_by_columns)])

    def window(self, window_functions : List[FieldExpression], partition_fields : List[str], order_by : List(Tuple(FieldExpression, str)) = None) -> Table:
        """Creates a new table, calculating aggregate functions over a window of fields, optionally ordered by the specified columns"""

        return Table(self.transformations + [WindowTransformation(window_functions, partition_fields, order_by)])

    def to_protobuf(self) -> protobuf_model.Table:
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