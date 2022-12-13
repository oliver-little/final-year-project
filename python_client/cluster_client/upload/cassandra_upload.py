from __future__ import annotations
from typing import List, Dict, Callable

from datetime import datetime
import csv

from cluster_client.connector.cassandra import CassandraConnector

def check_conversion(item : str, func : Callable) -> bool:
    try:
        func(item)
        return True
    except ValueError:
        return False

def is_float(item : str) -> bool:
    return check_conversion(item, float)

def is_bool(item : str) -> bool:
    if item.lower() == "true" or item.lower() == "false":
        return True
    else:
        return False

def is_iso_datetime(item : str) -> bool:
    return check_conversion(item, datetime.fromisoformat)

class CassandraDatatype():
    def __init__(self, validator, datatype_string : str) -> None:
        self.validator = validator
        self.datatype_string = datatype_string

# Stores inferred datatypes, from most restrictive to least restrictive
DATATYPES = [
    CassandraDatatype(lambda x: is_iso_datetime(x), "timestamp"),
    CassandraDatatype(lambda x: x.isdigit(), "bigint"),
    CassandraDatatype(lambda x: is_float(x), "double"),
    CassandraDatatype(lambda x: is_bool(x), "boolean"),
    CassandraDatatype(lambda _: True, "text")
]

class CassandraUploadHandler():
    def __init__(self, connector : CassandraConnector):
        self.connector = connector

    def create_from_csv(self, file_name : str, keyspace : str, table : str, partition_keys : List[str], primary_keys : List[str] = [], column_names : List[str] = None, column_types : List[str] = None, row_converters : Dict[str, Callable] = {}, start_row : int = 1, delimiter : str = ",", quotechar : str = '"', row_delimiter : str ="\n") -> None:
        if column_types is None:
            if column_names is None:
                column_names, column_types = infer_columns_from_csv(file_name, delimiter, quotechar, row_delimiter, detect_headers=False)
            else:
                column_types = infer_columns_from_csv(file_name, delimiter, quotechar, row_delimiter, detect_headers=True)

        self.create_table(keyspace, table, column_names, column_types, partition_keys, primary_keys)
        self.insert_from_csv(file_name, keyspace, table, column_names, row_converters, start_row, delimiter, quotechar, row_delimiter)

    def create_table(self, keyspace : str, table : str, column_names : List[str], column_types : List[str], partition_keys : List[str], primary_keys : List[str]) -> None:
        self.connector.create_keyspace(keyspace)
        if self.connector.has_table(keyspace, table):
            raise ValueError(f"Table already exists: {keyspace}.{table}")

        # Validation
        if len(column_names) == 0 or len(column_types) == 0 or len(column_names) != len(column_types):
            raise ValueError("Invalid number of column names or column types (must have more than 1, and must have the same number of names and types")
        if len(partition_keys) == 0:
            raise ValueError("Must have at least 1 partition key")

        # Validation and preparing strings (can't use prepared statements here)

        if not all((x.isalpha() or x.isdigit()) for x in column_names):
            raise ValueError(f"Invalid column names (must be alphanumeric)")
        elif not all(x.isalpha() for x in column_types):
            raise ValueError(f"Invalid column types (must be alpha)")
        
        column_check = lambda x: x.isalpha() and x in column_names
        if not all(column_check(x) for x in partition_keys):
            raise ValueError("Invalid partition keys")
        elif not all(column_check(x) for x in primary_keys):
            raise ValueError("Invalid partition keys")

        if len(primary_keys) == 0:
            key_string = "(" + ", ".join(partition_keys) + ")"
        else:
            key_string = "((" + ", ".join(partition_keys) + "), " + ", ".join(primary_keys) + ")"
        
        column_string = ", ".join(" ".join(i) for i in zip(column_names, column_types))

        self.connector.get_session().execute(f"CREATE TABLE {keyspace}.{table} ({column_string} PRIMARY KEY {key_string});")

    def insert_from_csv(self, file_name : str, keyspace : str, table : str, column_names : List[str], row_converters : Dict[str, Callable] = {}, start_row : int = 1, delimiter = ",", quotechar = '"', row_delimiter="\n") -> None:
        column_string = ", ".join(column_names)
        value_string = ", ".join(["?"] * len(column_names))

        # Prepare statement
        prep = self.connector.get_session().prepare(f"INSERT INTO {keyspace}.{table} ({column_string}) VALUES ({value_string})")
        
        # Generate row_converter mapping
        row_converter_indices = {column_names.index(k) : v for k, v in row_converters.items()}

        with open(file_name, "r", newline=row_delimiter) as file:
            reader = csv.reader(file, delimiter=delimiter, quotechar=quotechar)

            # Skip start_row rows
            for x in range(start_row - 1):
                next(reader)

            for row in reader:
                # Convert anything that needs to be converted (usually dates/datetimes)
                for index, converter in  row_converter_indices.items():
                    row[index] = converter(row[index])
                # Execute prepared statement for this row
                self.connector.get_session().execute(prep, row)

def infer_columns_from_csv(file_name : str, delimiter = ",", quotechar = '"', row_delimiter="\n", detect_headers : bool = True) -> List[str]:
    """Scans the entire file to determine the type of each column.
    If possible, provide the column definitions instead, as this will be much faster.
    NOTE: this does not handle datetime objects except where they are in ISO8601 format. 
    If custom date/datetime formats are required, you must provide the column definitions.
    When detect_headers is true (default), return type is (column_names, column_types). Otherwise, return type is column_types
    """

    # Stores the index of the current inferred datatype of the column
    column_checker_index = []

    with open(file_name, "r", newline=row_delimiter) as file:
        reader = csv.reader(file, delimiter=delimiter, quotechar=quotechar)

        if detect_headers:
            try:
                column_names = list(map(lambda x: x.strip(), next(reader)))
            except StopIteration:
                raise ValueError("csv file has no content")
            
        for row in reader:
            if len(column_checker_index) == 0:
                column_checker_index = [0] * len(row)
            # Iterate over all cells in the row
            for cell_index in range(len(row)):
                # Iterate over all checkers, starting from the one we are currently using
                for y in range(column_checker_index[cell_index], len(DATATYPES)):
                    # Check if this checker fits this columns
                    if DATATYPES[y].validator(row[cell_index].strip()):
                        # If it does, continue to the next cell
                        break
                    else:
                        # Otherwise, increment the checker we are using and try again
                        column_checker_index[cell_index] += 1

    if len(column_checker_index) == 0:
        raise ValueError("csv file has no content")

    try:
        return column_names, [DATATYPES[index].datatype_string for index in column_checker_index]
    except NameError:
        return [DATATYPES[index].datatype_string for index in column_checker_index]