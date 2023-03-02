import pandas as pd
import numpy as np
import numpy_financial as npf
import random
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta
from scipy.stats import halfnorm
from tqdm import tqdm

# Questions for Kirill:
# What sort of random effects do we see on loans data?/where should I be introducting randomness / error

DISTRIBUTION_FUNCTION = halfnorm.rvs

NUM_EXISTING_LOANS = 1000
NUM_ORIGINATIONS = 100

# Start and end dates for amortisation data (exclusive)
START_DATA_DATE = "2020-12-31"
END_DATA_DATE = "2022-01-01"

# Start and end dates for existing loan generation
START_EXISTING_DATE = "1/1/2000 12:00 AM"
END_EXISTING_DATE = "1/1/2021 12:00 AM"

# Start and end dates for random origination date generation
START_ORIGINATION_DATE = "1/1/2021 12:00 AM"
END_ORIGINATION_DATE = "1/1/2022 12:00 AM"

# Interest Rates
INTEREST_RATE_MIN = 0.005
INTEREST_RATE_MAX = 0.1

MONTHLY_RATE = 30/360

# Loan duration in years
DURATION_MIN = 20
DURATION_MAX = 30

# Origination amount
ORIGINATION_MIN = 100000
ORIGINATION_MAX = 1500000

# Repayment date shifting 
REPAYMENT_SHIFT_PROBABILITY = 1/10000
# Repayment shift amount (+-days)
REPAYMENT_SHIFT_AMOUNT = 5

# Repayment dropping
REPAYMENT_DROP_PROBABILITY = 1/100000

# Files to generate
# Loan tape - balance of loan per month and interest paid on loan per momth 
# Loan tape repayments
# Loan interest rate file?

# START: Random date generation code between two dates
# Source: https://stackoverflow.com/questions/553303/generate-a-random-date-between-two-other-dates
def str_time_prop(start, end, time_format, prop):
    """Get a time at a proportion of a range of two formatted times.

    start and end should be strings specifying times formatted in the
    given format (strftime-style), giving an interval [start, end].
    prop specifies how a proportion of the interval to be taken after
    start.  The returned time will be in the specified format.
    """

    stime = time.mktime(time.strptime(start, time_format))
    etime = time.mktime(time.strptime(end, time_format))

    ptime = stime + prop * (etime - stime)

    return datetime.strptime(time.strftime(time_format, time.localtime(ptime)), time_format)


def random_date(start, end, prop):
    return str_time_prop(start, end, '%m/%d/%Y %I:%M %p', prop)

# Usage: random_date("1/1/2021 12:00 AM", "1/1/2022 12:00 AM", random.random())
# END: Random date generation code between two dates

# START: from https://stackoverflow.com/questions/51855922/pandas-date-range-for-specific-day-of-month
def month_range_day(start, periods):
    start_date = pd.Timestamp(start).date()
    month_range = pd.date_range(start=start_date, periods=periods, freq='M')
    month_day = month_range.day.values
    month_day[start_date.day < month_day] = start_date.day
    return pd.to_datetime(month_range.year*10000+month_range.month*100+month_day, format='%Y%m%d')
# END

def generate_origination(start, end):
    amount = random.randint(ORIGINATION_MIN, ORIGINATION_MAX)

    interest_rate = random.uniform(INTEREST_RATE_MIN, INTEREST_RATE_MAX)

    duration = random.randint(DURATION_MIN, DURATION_MAX)

    origination_date = random_date(start, end, random.random())

    return amount, interest_rate, duration, origination_date

def generate_amortisation_schedule(amount, interest_rate, duration, origination_date):
    current_amount = amount
    
    amortisation_schedule = pd.DataFrame({"date": month_range_day(origination_date + relativedelta(month=1), duration *12)})
    per = np.arange(duration * 12) + 1
    amortisation_schedule["principal_repayment"] = npf.ppmt(interest_rate * MONTHLY_RATE, per, duration * 12, amount) * -1
    amortisation_schedule["interest_repayment"] = npf.ipmt(interest_rate * MONTHLY_RATE, per, duration * 12, amount) * -1
    amortisation_schedule["total_repayment"] = npf.pmt(interest_rate * MONTHLY_RATE, duration * 12, amount) * -1

    # Check principal + interest = total
    np.allclose(amortisation_schedule["principal_repayment"] + amortisation_schedule["interest_repayment"], amortisation_schedule["total_repayment"])
    amortisation_schedule["principal_repaid"] = 0
    amortisation_schedule["interest_repaid"] = 0
    amortisation_schedule["total_repaid"] = 0

    # Generate repaid amounts
    amortisation_schedule.loc[0, "principal_repaid"] = amortisation_schedule.loc[0, "principal_repayment"]
    amortisation_schedule.loc[0, "interest_repaid"] = amortisation_schedule.loc[0, "interest_repayment"]
    amortisation_schedule.loc[0, "total_repaid"] = amortisation_schedule.loc[0, "total_repayment"]

    for x in range(1, len(amortisation_schedule)):
        amortisation_schedule.loc[x, "principal_repaid"] = amortisation_schedule.loc[x - 1, "principal_repaid"] + amortisation_schedule.loc[x, "principal_repayment"]
        amortisation_schedule.loc[x, "interest_repaid"] = amortisation_schedule.loc[x - 1, "interest_repaid"] + amortisation_schedule.loc[x, "interest_repayment"]
        amortisation_schedule.loc[x, "total_repaid"] =  amortisation_schedule.loc[x - 1, "total_repaid"] + amortisation_schedule.loc[x, "total_repayment"]

    return amortisation_schedule

if __name__ == "__main__":
    amortisation_data = []
    origination_data = []

    # Generate existing loans
    print("Generating existing loans:")

    for loan_id in tqdm(range(NUM_EXISTING_LOANS)):
        amount, interest_rate, duration, origination_date = generate_origination(START_EXISTING_DATE, END_EXISTING_DATE)
        schedule = generate_amortisation_schedule(amount, interest_rate, duration, origination_date)
        schedule = schedule[(schedule["date"] > START_DATA_DATE) & (schedule["date"] < END_DATA_DATE)]
        schedule["Loan_ID"] = loan_id
        origination_data.append([loan_id, amount, interest_rate, duration, origination_date.strftime("%Y-%m-%d")])
        amortisation_data.append(schedule)

    print("Generating new originations:")

    # Generate originations
    for loan_id in tqdm(range(NUM_EXISTING_LOANS, NUM_EXISTING_LOANS + NUM_ORIGINATIONS)):
        amount, interest_rate, duration, origination_date = generate_origination(START_ORIGINATION_DATE, END_ORIGINATION_DATE)
        schedule = generate_amortisation_schedule(amount, interest_rate, duration, origination_date)
        schedule = schedule[(schedule["date"] > START_DATA_DATE) & (schedule["date"] < END_DATA_DATE)]
        schedule["Loan_ID"] = loan_id
        origination_data.append([loan_id, amount, interest_rate, duration, origination_date.strftime("%Y-%m-%d")])
        amortisation_data.append(schedule)


    print("Outputting data.")

    origination_data = pd.DataFrame(data=origination_data, columns=["Loan_ID", "Origination_Amount", "Interest_Rate", "Duration", "Origination_Date"])
    origination_data.to_csv("Origination_data.csv", index=False)

    full_data = pd.concat(amortisation_data)
    full_data.to_csv("Loan_data.csv", index=False)