import pandas as pd
import random
import time
from datetime import datetime
from scipy.stats import halfnorm

PATH_TO_CURRENCY_DATA = "../../forex.csv"

# Number of transactions to generate (most transactions will have multiple rows)
NUM_TRANSACTIONS = 100

# Start and end dates for random generation
START_DATE = "1/1/2021 12:00 AM"
END_DATE = "1/1/2022 12:00 AM"

DISTRIBUTION_FUNCTION = halfnorm.rvs

# Various probabilities for the generation
## Revenue
MAX_REVENUE = 50
MIN_REVENUE = 0

# Probability that the revenue will be changed from the amount that would be recalculated
REVENUE_FLUCTUATION_CHANCE = 1/1000
# Maximum percentage amount to modify the revenue by
REVENUE_FLUCTUATION_AMOUNT = 0.1

## Amounts
# Minimum amount
MIN_TRANSACTION_AMOUNT = 0.1
# Maximum amount
MAX_TRANSACTION_AMOUNT = 10000

## Splits
# Threshold for enabling cash item splitting (only transactions above this amount will be eligible)
SPLIT_CASH_THRESHOLD = 1000
# Probability that a cash item (in or out) will be split across multiple entries, rather than 1 entry
SPLIT_CASH_CHANCE = 1/100
# Maximum number of splits
MAX_SPLITS = 100

# Probability that a line item (part of a transaction) will be entirely dropped.
LINE_ITEM_DROP_CHANCE = 1/100000
# Probability that a fee will be charged if it is an applicable currency
FEE_CHANCE = 0.95

# Currencies for which a fee is involved - this is for source or destination currency
# Maps the currency amount, to the percentage of fee to charge
FEES = {
    "BRL": 0.05,
    "IDR": 0.1,
    "MYR": 0.075
}

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

def test_random(threshold):
    """Generates a random number between 0 and 1, and tests it against a provided threshold, returning a boolean if the number is below the threshold"""
    return random.uniform(0, 1) <= threshold

def create_splits(amount):
    """Creates n random splits (between 1 and MAX_SPLITS) that add to amount provided"""
    num_splits = random.randint(1, MAX_SPLITS)
    # Naive way of generating n random splits: generate n random numbers, then multiply by (amount/total)
    split_amounts = [DISTRIBUTION_FUNCTION(0, MAX_TRANSACTION_AMOUNT / 2.5) + MIN_TRANSACTION_AMOUNT for x in range(num_splits)]
    scale_factor = amount / sum(split_amounts)
    return [split_amount * scale_factor for split_amount in split_amounts]

def generate_random_splits(amount):
    if amount > SPLIT_CASH_THRESHOLD and test_random(SPLIT_CASH_CHANCE):
        return create_splits(amount)
    else:
        return [amount]

def calculate_fee(amount, currency):
    return amount * FEES[currency] 

def generate_rows(amounts, currency, identifier):
    return [[amount, currency, identifier] for amount in amounts]

def generate_transaction(transfer_id, currency_pair, currency_data):
    # Overall calculation: Cash in + cash out - fees (if applicable) = revenue after converting all to same currency
    # To do this in reverse, generate a load of values in the base currency, then do any conversions in reverse to the other currency
    source, dest = currency_pair.split("/")
    transfer_date = random_date("1/1/2021 12:00 AM", "1/1/2022 12:00 AM", random.random())
    currency_data_for_pair = currency_data.loc[(currency_data["slug"] == currency_pair) & (currency_data["date"] <= transfer_date.strftime("%Y-%m-%d"))].sort_values("date", ascending=False)

    ## Generate in amount
    # Scale down by 2.5 as that's roughly how big the distribution is
    in_amount = generate_random_splits(DISTRIBUTION_FUNCTION(0, MAX_TRANSACTION_AMOUNT / 2.5) + MIN_TRANSACTION_AMOUNT)
    in_currency = source

    ## Generate revenue
    revenue = min(DISTRIBUTION_FUNCTION(0, min(MAX_REVENUE, sum(in_amount)) / 2.5) + MIN_REVENUE, sum(in_amount))
    revenue_currency = source

    ## Calculate out amount (source currency) from in + revenue
    out_amount_source = generate_random_splits(sum(in_amount) - revenue)

    # FX Conversion
    fx_rate = currency_data_for_pair["open"].iloc[0]

    out_amount_local = [amount * fx_rate for amount in out_amount_source]
    out_currency = dest

    ## Fees
    fee_amount = 0
    fee_currency = None
    if revenue_currency in FEES and test_random(FEE_CHANCE):
        fee_currency = source
        fee_amount = calculate_fee(revenue, revenue_currency)
          
    # Also drop the revenue by the fee amount, if applicable
    revenue = revenue - fee_amount

    ## Randomly fluctuate revenue (to introduce error)
    if test_random(REVENUE_FLUCTUATION_CHANCE):
        revenue = revenue * (1 + random.uniform(-REVENUE_FLUCTUATION_AMOUNT, REVENUE_FLUCTUATION_AMOUNT))

    # Generate data for this transaction
    data = generate_rows(in_amount, in_currency, "CASH_IN") + \
        generate_rows([revenue], revenue_currency, "REVENUE") + \
        [[sum(in_amount) - revenue, in_currency, "FX_CONVERSION"]] + \
        [[sum(out_amount_local), out_currency, "FX_CONVERSION"]] + \
        generate_rows(out_amount_local, out_currency, "CASH_OUT")

    if fee_amount != 0:
        data += [[fee_amount, fee_currency, "TRANSFER_FEE_" + fee_currency]]

    transfer_df = pd.DataFrame(data, columns=["Amount", "Currency", "Identifier"])
    transfer_df["Transfer_ID"] = transfer_id
    transfer_df["Date"] = transfer_date.strftime("%Y-%m-%d")


    return transfer_df


if __name__ == "__main__":
    # Cols: slug (source / destination - read as 1 source = {rate} destination), date, open (FX rate), high (FX rate), low (FX rate), close (FX rate), currency (destination currency, second of slug pair)
    currency_data = pd.read_csv(PATH_TO_CURRENCY_DATA)
    currency_data[["Source", "Dest"]] = currency_data["slug"].str.split("/", expand=True)
    unique_pairs = pd.unique(currency_data["slug"]).tolist()
    
    transfers = [generate_transaction(x, random.choice(unique_pairs), currency_data) for x in range(1, NUM_TRANSACTIONS + 1)]

    output = pd.concat(transfers)

    to_drop = output.sample(frac=LINE_ITEM_DROP_CHANCE)
    print("Dropped " + str(len(to_drop)) + " items.")
    output = output.drop(to_drop.index)
    output = output.round(2)
    output.index.name = "Line_ID"
    output.to_csv("./transaction_data.csv")

    print(f"Done. Output {len(output)} items.")