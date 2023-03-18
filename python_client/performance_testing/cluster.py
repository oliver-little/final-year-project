import time
from cluster_client.manager import ClusterManager
from cluster_client.model.field_expressions import *
from cluster_client.model.aggregate_expressions import *

connection_string = ""

def execute(callable, times):
    results = []
    for x in range(times):
        t0 = time.time()
        callable()
        t1 = time.time()
        diff = t1 - t0
        results.append(diff)
        print(f'{t1 - t0:.5f} seconds')
    print(f"All times: {results}")
    average = sum(results) / len(results)
    print(f"Average times: {average}")


def select_simple(manager, table):
    def inner():
        manager.cassandra_table("origination", table).evaluate()
    return inner

def select_with_operations(manager, table):
    def inner():
        manager.cassandra_table("origination", table).select(
            (F("loan_id") + 1).as_name("loan_id_inc"), 
            (F("interest_rate") + 1).as_name("interest_rate_inc"),
            Function.Pow(Function.ToDouble(F("duration")), 2.0).as_name("duration_pow"),
            Function.Substring(Function.ToString(F("origination_date")), 0, 10).as_name("orignation_date_str")
        ).evaluate()
    return inner

def select_tests(ranges, manager=ClusterManager("localhost")):
    times = 5
    for table in ranges:
        print(f"Table: origination.{table}")
        execute(select_simple(manager, table), times)
        execute(select_with_operations(manager, table), times)

def filter_simple(manager, table):
    def inner():
        manager.cassandra_table("origination", table).filter(F("duration") == 30).evaluate()
    return inner

def filter_complex(manager, table):
    def inner():
        manager.cassandra_table("origination", table).filter(
            (
                (F("duration") == 30) 
                & 
                (F("amount") > 500000.0)
            )
            | 
            (F("loan_ID") == 1)
        ).evaluate()
    return inner

def filter_tests(ranges, manager=ClusterManager("localhost")):
    times = 5
    for table in ranges:
        print(f"Table: origination.{table}")
        execute(filter_simple(manager, table), times)
        execute(filter_complex(manager, table), times)

def group_by_simple(manager, table):
    def inner():
        manager.cassandra_table("origination", table).group_by([F("duration")]).evaluate()
    return inner

def group_by_with_aggregate(manager, table):
    def inner():
        manager.cassandra_table("origination", table).group_by(
            [F("duration")],
            [Max(F("origination_date")), Avg(F("interest_rate")), Min(F("amount"))]
        ).evaluate()
    return inner

def group_by_tests(ranges, manager=ClusterManager("localhost")):
    times = 5
    for table in ranges:
        print(f"Table: origination.{table}")
        execute(group_by_simple(manager, table), times)
        execute(group_by_with_aggregate(manager, table), times)

