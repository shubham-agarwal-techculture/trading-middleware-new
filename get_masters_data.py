from xts_api_client.xts_connect import XTSConnect
import pandas as pd

# XTS credentials
API_KEY = "797186c437b71e16887889"
API_SECRET = "Dnfl104#2l"
SOURCE = "WEBAPI"

# Exchage Segment
# EXCHANGE_SEGMENT = "MCXFO"
EXCHANGE_SEGMENT = "NSEFO"

# Base URL from your broker
BASE_URL = "https://eztrade.wealthdiscovery.in/apimarketdata"

# Create XTS object
xt = XTSConnect(API_KEY, API_SECRET, SOURCE, root=BASE_URL, disable_ssl=True)

# Login for market data
response = xt.marketdata_login()

print("Login Response:")
print(response)

# Fetch master data
# master = xt.get_master(exchangeSegmentList=["NSECM"])
# master = xt.get_master(exchangeSegmentList=["NSEFO"])
master = xt.get_master(exchangeSegmentList=[EXCHANGE_SEGMENT])


# Save raw master data
raw_data = master["result"]


# =========================
# Column Names for NSECM
# =========================
columns = [
    "ExchangeSegment",
    "ExchangeInstrumentID",
    "InstrumentType",
    "Name",
    "Description",
    "Series",
    "NameWithSeries",
    "InstrumentID",
    "PriceBandHigh",
    "PriceBandLow",
    "FreezeQty",
    "TickSize",
    "LotSize",
    "Multiplier",
    "UnderlyingInstrumentId",
    "UnderlyingIndexName",
    "ContractExpiration",
    "StrikePrice",
    "OptionType",
]

# =========================================
# Column Names for MCXFO (if needed, you can adjust based on actual data)
# =========================================

# columns = [
#     "ExchangeSegment",
#     "ExchangeInstrumentId",
#     "InstrumentType",
#     "Name",
#     "Description",
#     "Series",
#     "NameWithSeries",
#     "InstrumentId",
#     "PriceBandHigh",
#     "PriceBandLow",
#     "FreezeQty",
#     "TickSize",
#     "LotSize",
#     "Multiplier",
#     "UnderlyingInstrumentId",
#     "UnderlyingIndexName",
#     "ContractExpiration",
#     "StrikePrice",
#     "OptionType",
# ]

# =========================
# Convert to DataFrame
# =========================
rows = []

print(raw_data[:500])  # Print the first 500 characters to understand the structure

for line in raw_data.split("\n"):
    if line.strip():
        values = line.split("|")[:19]
        rows.append(values)

df = pd.DataFrame(rows, columns=columns)

# =========================
# Save CSV
# =========================
df.to_csv(f"master_data\{EXCHANGE_SEGMENT}.csv", index=False)

print(df.head())
# print("Saved as nsecm_master.csv")

print(f"Master data saved to {EXCHANGE_SEGMENT}.csv")
