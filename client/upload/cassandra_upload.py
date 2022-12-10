from __future__ import annotations
from typing import List, Dict, Callable

from datetime import datetime
import csv

from connector.cassandra import CassandraConnector

def test_conversion(item : str, func : Callable) -> bool:
    try:
        func(item)
        return True
    except ValueError:
        return False

def is_float(item : str) -> bool:
    return test_conversion(item, float)

def is_bool(item : str) -> bool:
    return test_conversion(item, bool)

def is_iso_datetime(item : str) -> bool:
    return test_conversion(item, datetime.fromisoformat)

DATATYPE_CHECKERS = [
    lambda x: is_iso_datetime(x),
    lambda x: x.isdigit(),
    lambda x: is_float(x),
    lambda x: is_bool(x),
    lambda _: True
]

DATATYPE_ORDER = [
    "timestamp",
    "bigint",
    "double",
    "boolean",
    "text"
]   

class CassandraUploadHandler():
    def __init__(self, connector : CassandraConnector):
        self.connector = connector

    def create_from_csv(self, file_name : str, keyspace : str, table : str, column_names : List[str], partition_keys = List[str], primary_keys = List[str], column_types : List[str] = None, date_converters = Dict[str, Callable], delimiter : str = ",", quotechar : str = '"', row_delimiter : str ="\n") -> None:
        if column_types is None:
            column_types = self.infer_columns_from_csv(file_name, delimiter, quotechar, row_delimiter)
        self.create_table(keyspace, table, column_names, column_types, partition_keys, primary_keys)
        self.insert_from_csv(file_name, keyspace, table, delimiter, quotechar, row_delimiter)

    def create_table(self, keyspace : str, table : str, column_names : List[str], column_types : List[str], partition_keys = List[str], primary_keys = List[str]) -> None:
        self.connector.create_keyspace(keyspace)
        if self.connector.has_table(keyspace, table):
            raise ValueError(f"Table already exists: {keyspace}.{table}")

        # Validation
        if len(column_names) == 0 or len(column_types) == 0 or len(column_names) != len(column_types):
            raise ValueError("Invalid number of column names or column types (must have more than 1, and must have the same number of names and types")
        if len(partition_keys) == 0:
            raise ValueError("Must have at least 1 partition key")

        # Validation and preparing strings (can't use prepared statements here)
        column_string = ""
        for name, type in zip(column_names, column_types):
            if not name.isalpha():
                raise ValueError(f"Invalid column name {name}")
            elif not type.isalpha():
                raise ValueError(f"Invalid column type {type}")
            column_string += f"{name} {type}, "

        partition_key_string = ""
        for partition_key in partition_keys:
            if not (partition_key.isalpha() and partition_key in column_names):
                raise ValueError(f"Invalid partition key {partition_key}")
            partition_key_string += partition_key + ", "

        partition_key_string = partition_key_string[:-2]

        primary_key_string = ""
        for primary_key in primary_keys:
            if not (primary_key.isalpha() and primary_key in column_names):
                raise ValueError(f"Invalid primary key {primary_key}")
            primary_key_string += primary_key + ", "

        primary_key_string = primary_key_string[:-2]

        if primary_key_string != "":
            key_string = "(" + partition_key_string + ")"
        else:
            key_string = "((" + partition_key_string + "), " + primary_key_string + ")"
        
        self.connector.get_session().execute(f"CREATE TABLE {keyspace}.{table} {key_string};")

    def infer_columns_from_csv(self, file_name : str, delimiter = ",", quotechar = '"', row_delimiter="\n") -> List[str]:
        """Scans the entire file to determine the type of each column.
        If possible, provide the column definitions instead, as this will be much faster.
        NOTE: this does not handle datetime objects except where they are in ISO8601 format. 
        If custom date/datetime formats are required, you must provide the column definitions
        """

        # Stores the index of the 
        column_checker_index = []

        with open(file_name, "r", newline=row_delimiter) as file:
            reader = csv.reader(file, delimiter=delimiter, quotechar=quotechar)
            for row in reader:
                if len(column_checker_index) == 0:
                    column_checker_index = [0] * len(row)
                # Iterate over all cells in the row
                for cell_index in range(len(row)):
                    # Iterate over all checkers, starting from the one we are currently using
                    for y in range(DATATYPE_CHECKERS[column_checker_index[cell_index]], len(DATATYPE_CHECKERS)):
                        # Check if this checker fits this columns
                        if DATATYPE_CHECKERS[y](row[cell_index]):
                            # If it does, continue to the next cell
                            break
                        else:
                            # Otherwise, increment the checker we are using and try again
                            column_checker_index[cell_index] += 1

        return [DATATYPE_ORDER[index] for index in column_checker_index]

    def insert_from_csv(self, file_name : str, keyspace : str, table : str, delimiter = ",", quotechar = '"', row_delimiter="\n") -> None:
        pass

    