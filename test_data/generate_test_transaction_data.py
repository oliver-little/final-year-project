import pandas as pd

PATH_TO_CURRENCY_DATA = "../../forex.csv"

# Number of transactions to generate (most transactions will have multiple rows)
NUM_TRANSACTIONS = 1000000

# Various probabilities for the generation
REVENUE_FLUCTUATION_CHANCE = 1/1000
REVENUE_FLUCTUATION_AMOUNT = 0.1
# Probability that a cash item (in or out) will be split across multiple entries, rather than 1 entry
SPLIT_CASH_CHANCE = 1/100
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

def generate_transfer():
    # Overall calculation: Cash in + cash out - fees (if applicable) = revenue after converting all to same currency
    # To do this in reverse, generate a load of values in the base currency, then do any conversions in reverse to the other currency
    x=1

if __name__ == "__main__":
    # Cols: slug (source / destination - read as 1 source = {rate} destination), date, open (FX rate), high (FX rate), low (FX rate), close (FX rate), currency (destination currency, second of slug pair)
    currency_data = pd.read_csv(PATH_TO_CURRENCY_DATA)
    currencies = currency_data["slug"].split("/")[0]