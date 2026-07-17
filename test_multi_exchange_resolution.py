"""Offline test: multi-exchange contract resolution (no orders placed)."""

import asyncio

from nifty_signal_bridge import (
    resolve_contract_by_ticker,
    find_contract_by_instrument_id,
    parse_tv_option_ticker,
)


async def main():
    cases = [
        ("NIFTY260721C25150", None),      # NSE weekly option (TV format)
        ("BSX260723C81100", None),        # SENSEX via BSX alias -> BSEFO
        ("SENSEX2672373400PE", None),     # BSE description exact match
        ("CRUDEOILM17AUG20265350CE", None),  # MCX option description
        ("RELIANCE", None),               # plain equity -> NSECM
        ("RELIANCE", "BSECM"),            # equity with segment hint -> BSECM
        ("RELIANCE-A", None),             # BSE description
    ]

    for ticker, hint in cases:
        c = await resolve_contract_by_ticker(ticker, segment_hint=hint)
        if c:
            print(
                f"OK   {ticker!r:32} hint={hint or '-':6} -> "
                f"{c['ExchangeSegment']} id={c['ExchangeInstrumentID']} "
                f"desc={c['Description']} lot={c['LotSize']}"
            )
        else:
            print(f"FAIL {ticker!r:32} hint={hint or '-':6} -> not found")

    row = find_contract_by_instrument_id("BSEFO", 833321)
    print("\nBy instrument id (BSEFO 833321):", row["Description"] if row else "NOT FOUND")

    print("\nParse checks:")
    print(" BSX260723C81100 ->", parse_tv_option_ticker("BSX260723C81100"))
    print(" NIFTY260625P25000 ->", parse_tv_option_ticker("NIFTY260625P25000"))
    print(" RELIANCE (not option) ->", parse_tv_option_ticker("RELIANCE"))


if __name__ == "__main__":
    asyncio.run(main())
