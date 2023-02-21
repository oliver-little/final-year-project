from .field_expressions import *

import cluster_client.protobuf.table_model_pb2 as protobuf_model
from .field_expressions import *

class AggregateExpression():
    def __init__(self, field_expression : NamedFieldExpression):
        self.field_expression = field_expression

    def get_aggregate_type(self) -> protobuf_model.AggregateExpression.AggregateType:
        raise NotImplementedError("AggregateExpression abstract class has no defined aggregate type.")

    def get_optional_arg(self) -> str:
        return None

    def to_protobuf(self) -> protobuf_model.AggregateExpression:
        optional_arg = self.get_optional_arg()
        if optional_arg is None:
            return protobuf_model.AggregateExpression(aggregate_type=self.get_aggregate_type(), expr=self.field_expression.to_named_protobuf())
        else:
            return protobuf_model.AggregateExpression(aggregate_type=self.get_aggregate_type(), expr=self.field_expression.to_named_protobuf(), arg=optional_arg)

class Max(AggregateExpression):
    def __init__(self, field_expression : NamedFieldExpression):
        super().__init__(field_expression)
    
    def get_aggregate_type(self) -> protobuf_model.AggregateExpression.AggregateType:
        return protobuf_model.AggregateExpression.AggregateType.MAX
    
class Min(AggregateExpression):
    def __init__(self, field_expression : NamedFieldExpression):
        super().__init__(field_expression)
    
    def get_aggregate_type(self) -> protobuf_model.AggregateExpression.AggregateType:
        return protobuf_model.AggregateExpression.AggregateType.MIN

class Sum(AggregateExpression):
    def __init__(self, field_expression : NamedFieldExpression):
        super().__init__(field_expression)
    
    def get_aggregate_type(self) -> protobuf_model.AggregateExpression.AggregateType:
        return protobuf_model.AggregateExpression.AggregateType.SUM

class Avg(AggregateExpression):
    def __init__(self, field_expression : NamedFieldExpression):
        super().__init__(field_expression)
    
    def get_aggregate_type(self) -> protobuf_model.AggregateExpression.AggregateType:
        return protobuf_model.AggregateExpression.AggregateType.AVG

class Count(AggregateExpression):
    def __init__(self, field_expression : NamedFieldExpression):
        super().__init__(field_expression)
    
    def get_aggregate_type(self) -> protobuf_model.AggregateExpression.AggregateType:
        return protobuf_model.AggregateExpression.AggregateType.COUNT

class DistinctCount(AggregateExpression):
    def __init__(self, field_expression : NamedFieldExpression):
        super().__init__(field_expression)
    
    def get_aggregate_type(self) -> protobuf_model.AggregateExpression.AggregateType:
        return protobuf_model.AggregateExpression.AggregateType.COUNT_DISTINCT

class StringConcat(AggregateExpression):
    def __init__(self, field_expression : NamedFieldExpression, delimiter : str):
        super().__init__(field_expression)

        self.delimiter = delimiter
    
    def get_aggregate_type(self) -> protobuf_model.AggregateExpression.AggregateType:
        return protobuf_model.AggregateExpression.AggregateType.STRING_CONCAT

    def get_optional_arg(self) -> str:
        return self.delimiter

class DistinctStringConcat(AggregateExpression):
    def __init__(self, field_expression : NamedFieldExpression, delimiter : str):
        super().__init__(field_expression)

        self.delimiter = delimiter
    
    def get_aggregate_type(self) -> protobuf_model.AggregateExpression.AggregateType:
        return protobuf_model.AggregateExpression.AggregateType.STRING_CONCAT_DISTINCT

    def get_optional_arg(self) -> str:
        return self.delimiter