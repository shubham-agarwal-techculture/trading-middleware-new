from pathlib import Path

import pandas as pd
from xts_api_client.xts_connect import XTSConnect

# XTS credentials (market data API)
API_KEY = "797186c437b71e16887889"
API_SECRET = "Dnfl104#2l"
SOURCE = "WEBAPI"
BASE_URL = "https://eztrade.wealthdiscovery.in/apimarketdata"

# All exchange segments supported by the OMS
EXCHANGE_SEGMENTS = [
    "NSECM",   # NSE Cash Market
    "NSEFO",   # NSE Futures & Options
    "BSECM",   # BSE Cash Market
    "BSEFO",   # BSE Futures & Options
    "MCXFO",   # MCX Futures
]

COLUMNS = [
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

OUTPUT_DIR = Path("master_data")


def parse_master_result(raw_data: str) -> pd.DataFrame:
    rows = []
    for line in raw_data.split("\n"):
        if line.strip():
            values = line.split("|")[:19]
            # Pad short rows so DataFrame columns always align
            if len(values) < len(COLUMNS):
                values.extend([""] * (len(COLUMNS) - len(values)))
            rows.append(values)
    return pd.DataFrame(rows, columns=COLUMNS)


def fetch_and_save_segment(xt: XTSConnect, segment: str) -> None:
    print(f"\nFetching master data for {segment}...")
    master = xt.get_master(exchangeSegmentList=[segment])

    if not isinstance(master, dict) or "result" not in master:
        print(f"  FAILED {segment}: unexpected response -> {master}")
        return

    raw_data = master["result"]
    if not raw_data or not str(raw_data).strip():
        print(f"  SKIPPED {segment}: empty result")
        return

    df = parse_master_result(str(raw_data))
    out_path = OUTPUT_DIR / f"{segment}.csv"
    df.to_csv(out_path, index=False)
    print(f"  Saved {len(df)} rows -> {out_path}")
    print(df.head(2).to_string(index=False))


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    xt = XTSConnect(API_KEY, API_SECRET, SOURCE, root=BASE_URL, disable_ssl=True)
    response = xt.marketdata_login()
    print("Login Response:")
    print(response)

    for segment in EXCHANGE_SEGMENTS:
        try:
            fetch_and_save_segment(xt, segment)
        except Exception as exc:
            print(f"  ERROR {segment}: {exc}")

    print("\nDone. Master CSVs are in:", OUTPUT_DIR.resolve())


if __name__ == "__main__":
    main()
