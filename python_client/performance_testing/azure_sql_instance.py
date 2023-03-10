from cluster_client.test_data.generate_test_loan_data import loan_origination_generator
import pyodbc
import time

connection_string = ""
conn = pyodbc.connect(connection_string)
cursor = conn.cursor()

generator = loan_origination_generator(20000000)


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


def select_simple(cursor, table):
    def inner():
        cursor.execute(f"SELECT * FROM {table}")
        rows = cursor.fetchall()
        return rows
    return inner

def select_with_operations(cursor, table):
    def inner():
        cursor.execute(f"SELECT Loan_ID + 1 as Loan_ID_Inc, interest_rate + 1 as Interest_rate_Inc, power(duration, 2) as Duration_Pow, substring(cast(origination_date as nvarchar(300)), 0, 11) as origination_date_str FROM {table}")
        rows = cursor.fetchall()
        return rows
    return inner

def select_tests():
    ranges = [1000, 10000, 100000, 1000000]
    times = 5
    for range in ranges:
        table = f"[dbo].[origination_data-{range}]"
        print(f"Table: {table}")
        execute(select_simple(cursor, table), times)
        execute(select_with_operations(cursor, table), times)

def filter_simple(cursor, table):
    def inner():
        cursor.execute(f"SELECT * FROM {table} WHERE duration = 30")
        rows = cursor.fetchall()
        return rows
    return inner

def filter_complex(cursor, table):
    def inner():
        cursor.execute(f"SELECT * FROM {table} WHERE (duration = 30 and amount > 500000) or loan_id = 1")
        rows = cursor.fetchall()
        return rows
    return inner

def filter_tests():
    ranges = [1000, 10000, 100000, 1000000, 10000000]
    times = 5
    for range in ranges:
        table = f"[dbo].[origination_data-{range}]"
        print(f"Table: {table}")
        execute(filter_simple(cursor, table), times)
        execute(filter_complex(cursor, table), times)

def group_by_simple(cursor, table):
    def inner():
        cursor.execute(f"SELECT duration FROM {table} GROUP BY duration")
        rows = cursor.fetchall()
        return rows
    return inner

def group_by_with_aggregate(cursor, table):
    def inner():
        cursor.execute(f"SELECT duration, MAX(origination_date) as Max_origination_date, AVG(interest_rate) as Avg_interest_rate, Min(amount) as Min_amount FROM {table} GROUP BY duration")
        rows = cursor.fetchall()
        return rows
    return inner

def group_by_tests():
    ranges = [1000, 10000, 100000, 1000000, 10000000]
    times = 5
    for range in ranges:
        table = f"[dbo].[origination_data-{range}]"
        print(f"Table: {table}")
        execute(group_by_simple(cursor, table), times)
        execute(group_by_with_aggregate(cursor, table), times)

