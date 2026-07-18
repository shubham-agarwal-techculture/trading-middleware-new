"""Contract CSV loading and XTS exchange-segment codes."""

from __future__ import annotations

import csv
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

log = logging.getLogger("NIFTY_ATM_LTP")

NIFTY_NAME = "NIFTY"
STRIKE_INTERVAL = 50

SEGMENT_CODES = {
    "NSECM": 1,
    "NSEFO": 2,
    "NSECDS": 3,
    "NSECO": 4,
    "BSECM": 11,
    "BSEFO": 12,
    "BSECDS": 13,
    "NCDEX": 21,
    "MCXFO": 51,
}


def segment_to_code(exchange_segment) -> int:
    """Convert a segment name like 'BSEFO' (or a numeric code) to the XTS numeric code."""
    if isinstance(exchange_segment, int):
        return exchange_segment
    seg = str(exchange_segment).strip().upper()
    if seg.isdigit():
        return int(seg)
    return SEGMENT_CODES.get(seg, 2)


class ContractLoader:
    """Loads and indexes contracts from a master CSV file."""

    def __init__(self, csv_path: Path):
        self.csv_path = csv_path
        self.contracts: list = []
        self._load_csv()

    def _load_csv(self) -> None:
        log.info("Loading CSV file: %s", self.csv_path)
        if not self.csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {self.csv_path}")
        with open(self.csv_path, "r", encoding="utf-8") as f:
            self.contracts = list(csv.DictReader(f))
        log.info("CSV loaded. Total contracts: %d", len(self.contracts))

    def get_nearest_expiry_futures(
        self, name: str = NIFTY_NAME
    ) -> Optional[Dict[str, Any]]:
        today = datetime.now().date()
        valid_contracts = []

        for contract in self.contracts:
            try:
                if contract["Name"] != name:
                    continue
                if contract["Series"] != "FUTIDX":
                    continue
                if contract["InstrumentType"] != "1":
                    continue
                expiry_date = datetime.fromisoformat(
                    contract["ContractExpiration"]
                ).date()
                if expiry_date >= today:
                    valid_contracts.append((contract, expiry_date))
            except (ValueError, KeyError):
                continue

        if not valid_contracts:
            log.warning("No valid futures contracts found for %s", name)
            return None

        valid_contracts.sort(key=lambda x: x[1])
        selected_contract, selected_expiry = valid_contracts[0]
        log.info(
            "Nearest expiry futures: %s | Expiry: %s | InstrumentID: %s",
            selected_contract["Description"],
            selected_expiry.isoformat(),
            selected_contract["ExchangeInstrumentID"],
        )
        return selected_contract

    def get_atm_options(
        self, underlying_price: float, name: str = NIFTY_NAME, option_type: str = "CE"
    ) -> Optional[Dict[str, Any]]:
        atm_strike = round(underlying_price / STRIKE_INTERVAL) * STRIKE_INTERVAL
        log.info(
            "Underlying price: %.2f | Calculated ATM strike: %d (%s)",
            underlying_price,
            atm_strike,
            option_type,
        )

        today = datetime.now().date()
        valid_contracts = []
        option_type_map = {"CE": "3", "PE": "4"}
        csv_option_type = option_type_map.get(option_type.upper(), option_type)

        for contract in self.contracts:
            try:
                if contract["Name"] != name:
                    continue
                if contract.get("OptionType") != csv_option_type:
                    continue
                strike = int(float(contract["StrikePrice"]))
                if strike != atm_strike:
                    continue
                expiry_date = datetime.fromisoformat(
                    contract["ContractExpiration"]
                ).date()
                if expiry_date >= today:
                    valid_contracts.append((contract, expiry_date))
            except (ValueError, KeyError):
                continue

        if not valid_contracts:
            log.warning(
                "No valid %s options found for strike %d", option_type, atm_strike
            )
            return None

        valid_contracts.sort(key=lambda x: x[1])
        selected_contract, selected_expiry = valid_contracts[0]
        log.info(
            "ATM %s option: %s | Expiry: %s | InstrumentID: %s",
            option_type,
            selected_contract["Description"],
            selected_expiry.isoformat(),
            selected_contract["ExchangeInstrumentID"],
        )
        return selected_contract
