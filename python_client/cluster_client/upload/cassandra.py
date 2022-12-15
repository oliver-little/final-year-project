from __future__ import annotations
from typing import List, Dict, Callable, Any
from datetime import datetime
import csv
import re
import logging
import ctypes
import multiprocessing
from queue import Empty
from cassandra.cluster import PreparedStatement

from cluster_client.connector.cassandra import CassandraConnector
from cluster_client.config import NAME_REGEX, VALID_COLUMN_TYPES, NUM_CASSANDRA_UPLOAD_PROCESSES, MAX_WAITING_READ_ROWS

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
        self.logger = logging.getLogger("cluster_client")

    def create_from_csv(self, file_name : str, keyspace : str, table : str, partition_keys : List[str], primary_keys : List[str] = [], column_names : List[str] = None, column_types : List[str] = None, row_converters : Dict[str, Callable] = {}, start_row : int = 1, delimiter : str = ",", quotechar : str = '"', row_delimiter : str ="\n") -> None:
        if column_types is None:
            if column_names is None:
                column_names, column_types = infer_columns_from_csv(file_name, delimiter, quotechar, row_delimiter, start_row, detect_headers=True)
                # Increment start_row, as we are detecting the headers
                start_row += 1
            else:
                column_types = infer_columns_from_csv(file_name, delimiter, quotechar, row_delimiter, start_row, detect_headers=False)    
        # Add a default converter for each column if we don't have one already, as the driver requires that they be in the correct type already
        for c_name, c_type in zip(column_names, column_types):
            if c_name not in row_converters:
                match c_type:
                    case "bigint":
                        row_converters[c_name] = lambda x: int(x)
                    case "double":
                        row_converters[c_name] = lambda x: float(x)
                    case "boolean":
                        row_converters[c_name] = lambda x: True if x.lower() == "true" else False
                    case "timestamp":
                        row_converters[c_name] = lambda x: datetime.fromisoformat(x)

        if not self.connector.has_table(keyspace, table):
            self.create_table(keyspace, table, column_names, column_types, partition_keys, primary_keys)
        self.insert_from_csv(file_name, keyspace, table, column_names, row_converters, start_row, delimiter, quotechar, row_delimiter)

    def create_table(self, keyspace : str, table : str, column_names : List[str], column_types : List[str], partition_keys : List[str], primary_keys : List[str]) -> None:
        self.connector.create_keyspace(keyspace)

        # Validation
        if len(column_names) == 0 or len(column_types) == 0 or len(column_names) != len(column_types):
            raise ValueError("Invalid number of column names or column types (must have more than 1, and must have the same number of names and types")
        if len(partition_keys) == 0:
            raise ValueError("Must have at least 1 partition key")

        # Validation and preparing strings (can't use prepared statements here)

        if not all(re.match(NAME_REGEX, x) for x in column_names):
            raise ValueError(f"Invalid column names (must be alphanumeric)")
        elif not all(x in VALID_COLUMN_TYPES for x in column_types):
            raise ValueError(f"Invalid column types (must be alpha)")
        
        if not all(x in column_names for x in partition_keys):
            raise ValueError("Invalid partition keys")
        elif not all(x in column_names for x in primary_keys):
            raise ValueError("Invalid partition keys")
        elif any(key in partition_keys for key in primary_keys):
            raise ValueError("Partition Key cannot also be a Primary Key")

        if len(primary_keys) == 0:
            key_string = "(" + ", ".join(partition_keys) + ")"
        else:
            key_string = "((" + ", ".join(partition_keys) + "), " + ", ".join(primary_keys) + ")"
        
        column_string = ", ".join(" ".join(i) for i in zip(column_names, column_types))

        query_string = f"CREATE TABLE {keyspace}.{table} ({column_string}, PRIMARY KEY {key_string});"
        self.logger.debug(f"Executing {query_string}")
        self.connector.get_session().execute(query_string)

    def insert_from_csv(self, file_name : str, keyspace : str, table : str, column_names : List[str] = None, row_converters : Dict[str, Callable] = {}, start_row : int = 1, delimiter = ",", quotechar = '"', row_delimiter="\n") -> None:  
        """Performs a multithreaded insert into the database"""

        # If we don't have column names, detect them from the first row after start_row
        if column_names is None:
            with open(file_name, "r", newline=row_delimiter) as file:
                reader = csv.reader(file, delimiter=delimiter, quotechar=quotechar)
                
                # Skip start_row rows
                for _ in range(start_row - 1):
                    next(reader)

                try:
                    column_names = list(map(lambda x: x.strip(), next(reader)))
                except StopIteration:
                    raise ValueError("csv file has no content")
            # Increment start_row, because we inferred the column names from the first row
            start_row += 1
            
        # Generate row_converter mapping
        row_converter_indices = {column_names.index(k) : v for k, v in row_converters.items()}

        column_string = ", ".join(column_names)
        value_string = ", ".join(["?"] * len(column_names))

        # Prepare statement
        prep = self.connector.get_session().prepare(f"INSERT INTO {keyspace}.{table} ({column_string}) VALUES ({value_string});")

        # Perform a multiprocess read/upload - the main process reads from the file, then a number of other processes actually perform the inserts
        queue = multiprocessing.Queue()
        # Requests that writers quit at the earliest opportunity
        request_stop_event = multiprocessing.Event()
        # Notifies writers that there are no more items to read, so when the queue is empty they can quit
        finished_reading_event = multiprocessing.Event()
        # Stores the exception if an insert failure occurs
        insert_failure_event = multiprocessing.Event()
        insert_failure_queue = multiprocessing.Queue(maxsize=MAX_WAITING_READ_ROWS)
        
        # Start writer processes
        self.logger.debug("Starting insert processes")
        writers = [multiprocessing.Process(target=insert_from_queue, args=(queue, self.connector.server_url, self.connector.port, prep, finished_reading_event, request_stop_event, insert_failure_event, insert_failure_queue)) for _ in range(NUM_CASSANDRA_UPLOAD_PROCESSES)]
        
        try:
            for writer in writers:
                writer.start()
                self.logger.debug(f"Created insert process {writer.pid}")
            self.logger.debug("Created insert processes")

            # Read the entire file
            self.logger.debug("Starting file read")
            with open(file_name, "r", newline=row_delimiter) as file:
                reader = csv.reader(file, delimiter=delimiter, quotechar=quotechar)

                # Skip start_row rows
                for _ in range(start_row - 1):
                    next(reader)

                # Read all rows into the read queue
                for row in reader:
                    row = apply_row_converters(row, row_converter_indices)
                    queue.put(row)

                    # Check if any failures have occurred, and quit early if that has happened
                    if insert_failure_event.is_set():
                        request_stop_event.set()
                        exception = insert_failure_queue.get()
                        raise ValueError(f"Upload failed: {exception}")

                # Set the event when reading has finished
                finished_reading_event.set()
            self.logger.debug("Finished file read")
    
            # Poll the failure status until the queue is empty (would just do a join here, but we need to track if a failure occurs)
            while not queue.empty():
                if insert_failure_event.is_set():
                    request_stop_event.set()
                    exception = insert_failure_queue.get()
                    raise ValueError(f"Upload failed: {exception}")

            request_stop_event.set()
            self.logger.debug("Read queue empty")
        finally:
            # Flush the queue (threads will not exit if there are items waiting to be queued, this mostly affects the read queue)
            if not queue.empty():
                queue.cancel_join_thread()

            # Attempt to join (close) all writers
            for writer in writers:
                writer.join(timeout=1)
                # If they don't close within the timeout, kill them
                if writer.is_alive():
                    self.logger.debug(f"Killing leftover writer {writer}")
                    writer.kill()
            self.logger.debug("Insert processes closed")

def apply_row_converters(row : List[str], converters : Dict[int, Callable]) -> List[Any]:
    return [converters.get(index, lambda x: x)(cell) for index, cell in enumerate(row)]

def handle_insert_failure(insert_failure_event : multiprocessing.Event, insert_failure_queue : multiprocessing.Queue, exception):
    if not insert_failure_event.is_set():
        insert_failure_queue.put(str(exception))
        insert_failure_event.set()

def insert_from_queue(queue : multiprocessing.Queue, server_url : str, port : int, prep : PreparedStatement, finished_reading : multiprocessing.Event, request_stop : multiprocessing.Event, insert_failure_event : multiprocessing.Event, insert_failure_queue : multiprocessing.Queue) -> None:    
    connector = CassandraConnector(server_url, port)
    err_callback = lambda exception: handle_insert_failure(insert_failure_event, insert_failure_queue, exception)
    try:
        while not request_stop.is_set():
            try:
                row = queue.get(block=True, timeout=0.5)
                future = connector.get_session().execute_async(prep, row)
                future.add_errback(err_callback)
            except Empty:
                # If we couldn't get an element from the queue, check if we are expecting new elements from the queue
                if finished_reading.is_set():
                    # If not, quit the loop
                    break
    finally:
        # No matter what happens, shutdown the connection to cassandra as it will block the thread from exiting
        connector.cluster.shutdown()

def infer_columns_from_csv(file_name : str, delimiter = ",", quotechar = '"', row_delimiter="\n", start_row : int = 1, detect_headers : bool = True) -> List[str]:
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

        # Skip start_row rows
        for _ in range(start_row - 1):
            next(reader)

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