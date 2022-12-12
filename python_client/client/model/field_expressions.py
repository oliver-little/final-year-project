from __future__ import annotations
from datetime import datetime
from types import NoneType
from typing import Any, Union

import protobuf.table_model_pb2 as protobuf_model

VALID_LITERAL_TYPES = {int, float, str, bool, datetime, NoneType}

def try_convert_model_value(value) -> FieldExpression:
    """Helper function to check the type of a value and convert it to a model Value if not a FieldExpression"""
    if not isinstance(value, FieldExpression):
        return V(value)
    else:
        return value

class FieldExpression():
    """Base class for all FieldExpressions"""
    def __init__(self):
        if type(self) is FieldExpression:
            raise NotImplementedError("FieldExpression is an abstract class and cannot be instantiated directly.")

    def to_protobuf(self) -> protobuf_model.Expression:
        raise NotImplementedError("FieldExpression abstract class cannot be converted to protobuf.")

    # Various dunder methods to override +-*/ etc
    def __add__(self, other) -> FieldExpression:
        return Function.Add(self, other)

    def __radd__(self, other) -> FieldExpression:
        return Function.Add(other, self)

    def __sub__(self, other) -> FieldExpression:
        return Function.Sub(self, other)

    def __rsub__(self, other) -> FieldExpression:
        return Function.Sub(other, self)

    def __mul__(self, other) -> FieldExpression:
        return Function.Mul(self, other)

    def __rmul__(self, other) -> FieldExpression:
        return Function.Mul(other, self)

    def __truediv__(self, other) -> FieldExpression:
        return Function.Div(self, other)

    def __rtruediv__(self, other) -> FieldExpression:
        return Function.Div(other, self)

    def __mod__(self, other) -> FieldExpression:
        return Function.Mul(self, other)

    def __rmod__(self, other) -> FieldExpression:
        return Function.Mul(other, self)  

    def __pow__(self, other) -> FieldExpression:
        return Function.Pow(self, other)

    def __rpow__(self, other) -> FieldExpression:
        return Function.Pow(other, self)

    def __eq__(self, other) -> FieldComparison:
        return BinaryFieldComparison("EQ", self, other)

    def __ne__(self, other) -> FieldComparison:
        return BinaryFieldComparison("NE", self, other)

    def __lt__(self, other) -> FieldComparison:
        return BinaryFieldComparison("LT", self, other)

    def __le__(self, other) -> FieldComparison:
        return BinaryFieldComparison("LTE", self, other)

    def __gt__(self, other) -> FieldComparison:
        return BinaryFieldComparison("GT", self, other)

    def __ge__(self, other) -> FieldComparison:
        return BinaryFieldComparison("GTE", self, other)

    def contains(self, operand : Union[V, str, int, float, bool, datetime, NoneType]) -> FieldComparison:
        return BinaryFieldComparison("CONTAINS", self, operand)

    def icontains(self, operand : Union[V, str, int, float, bool, datetime, NoneType]) -> FieldComparison:
        return BinaryFieldComparison("ICONTAINS", self, operand)

    def starts_with(self, operand : Union[V, str, int, float, bool, datetime, NoneType]) -> FieldComparison:
        return BinaryFieldComparison("STARTS_WITH", self, operand)

    def istarts_with(self, operand : Union[V, str, int, float, bool, datetime, NoneType]) -> FieldComparison:
        return BinaryFieldComparison("ISTARTS_WITH", self, operand)

    def contains(self, operand : Union[V, str, int, float, bool, datetime, NoneType]) -> FieldComparison:
        return BinaryFieldComparison("ENDS_WITH", self, operand)

    def icontains(self, operand : Union[V, str, int, float, bool, datetime, NoneType]) -> FieldComparison:
        return BinaryFieldComparison("IENDS_WITH", self, operand)

    def is_null(self) -> FieldComparison:
        return UnaryFieldComparison("IS_NULL", self)

    def is_not_null(self) -> FieldComparison:
        return UnaryFieldComparison("IS_NOT_NULL", self)

class V(FieldExpression):
    """Class definition for a literal argument. Automatically parses argument into an accepted type by the system or throws an error."""
    def __init__(self, value):
        super().__init__()
        self.value = value
        if not self.check_type():
            raise ValueError(f"Invalid type ({type(value)} provided for value '{value}'.")

    def check_type(self) -> bool:
        """Returns a bool representing if the type of this Value is acceptable for the system"""
        return type(self.value) in VALID_LITERAL_TYPES
    
    def to_protobuf(self) -> protobuf_model.Expression:
        value_type = type(self.value)
        value_to_encode = self.value
        if value_type == str:
            value_field_name = "string"
        elif value_type == int:
            value_field_name = "int"
        elif value_type == float:
            value_field_name = "float"
        elif value_type == bool:
            value_field_name = "bool"
        elif value_type == datetime:
            value_field_name = "datetime"
        elif value_type == NoneType:
            value_field_name = "null"
            value_to_encode = True
        else:
            raise ValueError(f"Invalid type ({type(self.value)} provided for value '{self.value}'.")

        return protobuf_model.Expression(value=
            protobuf_model.Value(
                **{value_field_name: value_to_encode} # This passes the string in value_field_name as a kwarg
            )
        )

    def __str__(self):
        return f"V({repr(self.value)})"

    def __repr__(self):
        return self.__str__()

class F(FieldExpression):
    """Class definition for an expression representing a Field in the system"""
    def __init__(self, field_name):
        super().__init__()
        self.field_name = field_name

    def to_protobuf(self) -> protobuf_model.Expression:
        return protobuf_model.Expression(value=protobuf_model.Value(field=self.field_name))

    def __str__(self):
        return f"F({repr(self.field_name)})"

    def __repr__(self):
        return self.__str__()

def function_builder(function_name : str, num_args : int) -> Function:
    """Takes a defined function name and a number of arguments, and returns a builder to construct that Function object when provided with the specified arguments"""
    def builder(*args : Any) -> Function:
        if len(args) != num_args:
            raise ValueError(f"Invalid number of arguments provided to {function_name}: provided {len(args)}, expected {num_args}, arguments: {args}")
        return Function(function_name, *args)

    return builder

class Function(FieldExpression):
    """Implements generic function calls, enforcing type checking on the arguments"""

    Add = function_builder("Add", 2)
    Sub = function_builder("Sub", 2)
    Mul = function_builder("Mul", 2)
    Div = function_builder("Div", 2)
    Pow = function_builder("Pow", 2)
    Mod = function_builder("Mod", 2)
    IsNull = function_builder("IsNull", 1)

    def __init__(self, function_name : str, *args):
        super().__init__()
        self.function_name = function_name

        # Convert any literal Values to FieldExpression 
        self.args = [try_convert_model_value(arg) for arg in args]
    
    def to_protobuf(self):
        return protobuf_model.Expression(function=
            protobuf_model.Expression.FunctionCall(
                function_name=self.function_name, 
                arguments = [arg.to_protobuf() for arg in self.args]
            )
        )

    def __str__(self):
        printed_args = ""
        for arg in self.args:
            printed_args += str(arg) + ", "

        printed_args = printed_args[:-2]
        return f"{self.function_name}({printed_args})"

    def __repr__(self):
        printed_args = ""
        for arg in self.args:
            printed_args += repr(arg) + ", "

        printed_args = printed_args[:-2]
        return f"{self.function_name}({printed_args})"


class FieldComparison():
    """Base class for representing field comparisons"""
    def __init__(self):
        if type(self) is FieldComparison:
            raise NotImplementedError("FieldComparison is an abstract class and cannot be instantiated directly.")

    def to_protobuf(self):
        raise NotImplementedError("FieldComparison abstract class can't be converted to protobuf.")

class UnaryFieldComparison(FieldComparison):
    VALID_COMPARATORS = {
        protobuf_model.Filter.FilterType.IS_NULL,
        protobuf_model.Filter.FilterType.NULL,
        protobuf_model.Filter.FilterType.IS_NOT_NULL,
        protobuf_model.Filter.FilterType.NOT_NULL
    }

    def __init__(self, comparator : str, field_exp):
        self.field_exp = try_convert_model_value(field_exp)
        try:
            self.filter_type = protobuf_model.Filter.FilterType.Value(comparator.upper())
        except ValueError:
            raise ValueError(f"Invalid comparator provided {comparator}")

        if self.filter_type not in UnaryFieldComparison.VALID_COMPARATORS:
            raise ValueError(f"Invalid number of arguments provided for comparator {comparator}")

    def to_protobuf(self) -> protobuf_model.FilterExpression:
        return protobuf_model.Filter.FilterExpression(
            left_value=self.field_exp.to_protobuf(),
            filter_type=self.filter_type
        )

    def __str__(self):
        return f"({str(self.field_exp)} {protobuf_model.Filter.FilterType.Name(self.filter_type)} )"
    
    def __repr__(self):
        return f"UnaryFieldComparison({protobuf_model.Filter.FilterType.Name(self.filter_type)}, {repr(self.field_exp)})"


# FieldComparison code
# In the same file to avoid circular references

class BinaryFieldComparison(FieldComparison):
    """Takes two FieldExpressions and an operator to represent a comparison used in a filter.
    Comparator should be one of:
        EQUAL (EQ), NOT_EQUAL (NE), LESS_THAN (LT), LESS_THAN_EQUAL (LTE), GREATER_THAN (GT), GREATER_THAN_EQUAL (GTE)
    """
    # Defines the comparators that are valid with this comparison type
    VALID_COMPARATORS = {
        protobuf_model.Filter.FilterType.EQUAL,
        protobuf_model.Filter.FilterType.EQ,
        protobuf_model.Filter.FilterType.NOT_EQUAL,
        protobuf_model.Filter.FilterType.NE,
        protobuf_model.Filter.FilterType.LESS_THAN,
        protobuf_model.Filter.FilterType.LT,
        protobuf_model.Filter.FilterType.LESS_THAN_EQUAL,
        protobuf_model.Filter.FilterType.LTE,
        protobuf_model.Filter.FilterType.GREATER_THAN,
        protobuf_model.Filter.FilterType.GT,
        protobuf_model.Filter.FilterType.GREATER_THAN_EQUAL,
        protobuf_model.Filter.FilterType.GTE,
        protobuf_model.Filter.FilterType.CONTAINS,
        protobuf_model.Filter.FilterType.ICONTAINS,
        protobuf_model.Filter.FilterType.STARTS_WITH,
        protobuf_model.Filter.FilterType.ISTARTS_WITH,
        protobuf_model.Filter.FilterType.ENDS_WITH,
        protobuf_model.Filter.FilterType.IENDS_WITH
    }
    
    # Defines the comparators which MUST have a Value literal on the right side of the expression
    RIGHT_VALUE_COMPARATORS = {
        protobuf_model.Filter.FilterType.CONTAINS,
        protobuf_model.Filter.FilterType.ICONTAINS,
        protobuf_model.Filter.FilterType.STARTS_WITH,
        protobuf_model.Filter.FilterType.ISTARTS_WITH,
        protobuf_model.Filter.FilterType.ENDS_WITH,
        protobuf_model.Filter.FilterType.IENDS_WITH
    }
    
    def __init__(self, comparator, left_field_exp, right_field_exp):
        super().__init__()
        self.left_field_exp = try_convert_model_value(left_field_exp)
        self.right_field_exp = try_convert_model_value(right_field_exp)
        try:
            self.filter_type = protobuf_model.Filter.FilterType.Value(comparator.upper())
        except ValueError:
            raise ValueError(f"Invalid comparator provided {comparator}")

        if self.filter_type not in BinaryFieldComparison.VALID_COMPARATORS:
            raise ValueError(f"Invalid number of arguments provided for comparator {comparator}")
        if self.filter_type in BinaryFieldComparison.RIGHT_VALUE_COMPARATORS and not isinstance(self.right_field_exp, V):
            raise ValueError(f"Right operand {right_field_exp} must be a literal Value in order to use comparator {comparator}")
        
    def to_protobuf(self) -> protobuf_model.Filter:
        return protobuf_model.Filter.FilterExpression(
            left_value=self.left_field_exp.to_protobuf(),
            filter_type=self.filter_type,
            right_value=self.right_field_exp.to_protobuf()
        )

    def __str__(self):
        return f"({str(self.left_field_exp)} {protobuf_model.Filter.FilterType.Name(self.filter_type)} {str(self.right_field_exp)})"
    
    def __repr__(self):
        return f"BinaryFieldComparison({protobuf_model.Filter.FilterType.Name(self.filter_type)}, {str(self.left_field_exp)}, {str(self.right_field_exp)})"

def generate_field_comparison(comparator : str, *operands):
    """Performs a lookup on the provided comparator to determine which FieldComparison class to place it in"""

    operands = operands
    comparator = comparator.upper()
    if protobuf_model.Filter.FilterType.Value(comparator) in BinaryFieldComparison.VALID_COMPARATORS:
        if len(operands) != 2:
            raise ValueError(f"Incorrect amount of arguments for comparator {comparator}, got {len(operands)}, expected 2.")
        return BinaryFieldComparison(comparator, operands[0], operands[1])
    elif protobuf_model.Filter.FilterType.Value(comparator) in UnaryFieldComparison.VALID_COMPARATORS:
        if len(operands) != 1:
            raise ValueError(f"Incorrect amount of arguments for comparator {comparator}, got {len(operands)}, expected 1.")
        return UnaryFieldComparison(comparator, operands[0])
    else:
        raise ValueError(f"Comparator {comparator} is invalid.")