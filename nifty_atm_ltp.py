"""
Nifty ATM Option LTP Fetcher

This script:
1. Gets the close price of the most recent expiry Nifty50 futures using XTS marketdata API
2. Calculates the ATM (At-The-Money) strike
3. Fetches the LTP for both CE and PE ATM options using XTS marketdata API

Usage:
    python nifty_atm_ltp.py
"""

import asyncio
import csv
import json
import logging
import sys
import aiohttp
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# Configuration
CSV_PATH = Path("master_data/NSEFO.csv")
XTS_API_KEY = "797186c437b71e16887889"  # Set your XTS API key
XTS_API_SECRET = "Dnfl104#2l"  # Set your XTS API secret
XTS_MARKETDATA_URL = "wss://eztrade.wealthdiscovery.in/apimarketdata"  # Replace with actual XTS marketdata WebSocket URL
XTS_REST_URL = "https://eztrade.wealthdiscovery.in/apimarketdata"  # Replace with actual XTS REST API URL

# Nifty50 specific settings
NIFTY_NAME = "NIFTY"
STRIKE_INTERVAL = 50  # Nifty options typically have 50-point intervals

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("NIFTY_ATM_LTP")


class XTSMarketDataClient:
    """Simple XTS Marketdata API client for fetching LTP via HTTP REST API."""

    def __init__(self, api_key: str, api_secret: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self.access_token: Optional[str] = None
        self.session: Optional[aiohttp.ClientSession] = None

    async def connect(self) -> None:
        """Connect to XTS marketdata REST API and authenticate."""
        if not self.api_key or not self.api_secret:
            raise ValueError("XTS_API_KEY and XTS_API_SECRET must be set")

        log.info("Connecting to XTS marketdata REST API...")
        try:
            self.session = aiohttp.ClientSession()

            # Login via HTTP
            login_url = f"{XTS_REST_URL}/auth/login"
            login_payload = {
                "secretKey": self.api_secret,
                "appKey": self.api_key,
            }

            async with self.session.post(
                login_url, json=login_payload, verify_ssl=False
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    # log.info("Login response: %s", json.dumps(data, indent=2))
                    if data.get("type") == "success":
                        # Try different possible token locations
                        self.access_token = (
                            data.get("result", {}).get("token")
                            or data.get("token")
                            or data.get("result", {}).get("accessToken")
                            or data.get("accessToken")
                        )
                        if self.access_token:
                            log.info(
                                "Successfully authenticated with XTS marketdata API"
                            )
                            log.info(
                                "Token (first 20 chars): %s...", self.access_token[:20]
                            )
                        else:
                            raise Exception(
                                f"Could not extract token from response: {data}"
                            )
                    else:
                        raise Exception(f"Authentication failed: {data}")
                else:
                    error_text = await response.text()
                    raise Exception(f"HTTP {response.status}: {error_text}")

        except Exception as e:
            log.error("Failed to connect to XTS marketdata: %s", e)
            raise

    async def get_ltp(
        self, exchange_instrument_id: int, exchange_segment: str = "NSEFO"
    ) -> Optional[float]:
        """
        Get Last Traded Price for a specific instrument via HTTP REST API.

        Args:
            exchange_instrument_id: The instrument ID
            exchange_segment: Exchange segment (default: NSEFO)

        Returns:
            LTP as float, or None if failed
        """
        if not self.session or not self.access_token:
            raise Exception("Not connected to XTS marketdata")

        try:
            # Get quote via HTTP - try different authentication approaches
            quote_url = f"{XTS_REST_URL}/instruments/quotes"

            # Try with token in headers (different formats)
            headers_variants = [
                # {
                #     "Authorization": f"Bearer {self.access_token}",
                #     "Content-Type": "application/json"
                # },
                {
                    "xts-api-key": self.api_key,
                    "Authorization": self.access_token,
                    "Content-Type": "application/json",
                },
                # {
                #     "token": self.access_token,
                #     "Content-Type": "application/json"
                # }
            ]

            payload = {
                # "instruments": [
                #     {
                #         "exchangeSegment": exchange_segment,
                #         "exchangeInstrumentID": exchange_instrument_id,
                #     }
                # ],
                "instruments": [
                    {
                        "exchangeSegment": 2,
                        "exchangeInstrumentID": exchange_instrument_id,
                    }
                ],
                "xtsMessageCode": 1501,
                "publishFormat": "JSON",
            }

            for i, headers in enumerate(headers_variants):
                # log.info("Trying authentication variant %d with headers: %s", i+1, headers)
                async with self.session.post(
                    quote_url, json=payload, headers=headers, verify_ssl=False
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        # print(data)
                        # log.info("Quote response: %s", json.dumps(data, indent=2))
                        # if data.get("result", {}).get("quotesResponseList"):
                        list_quotes = data["result"]["listQuotes"]
                        if list_quotes:
                            quote = json.loads(list_quotes[0])  # parse JSON string
                            ltp = quote["LastTradedPrice"]
                            # print("LTP:", ltp)
                            if ltp is not None:
                                return float(ltp)
                            else:
                                log.warning("No LTP in quote response")
                                return None
                        else:
                            log.warning("No quotes in response: %s", data)
                            return None
                    else:
                        error_text = await response.text()
                        log.warning(
                            "HTTP %d with variant %d: %s",
                            response.status,
                            i + 1,
                            error_text,
                        )
                        if i == len(headers_variants) - 1:  # Last variant failed
                            log.error("All authentication variants failed")
                            return None

        except Exception as e:
            log.error(
                "Error getting LTP for instrument ID %d: %s", exchange_instrument_id, e
            )
            return None

    async def disconnect(self) -> None:
        """Disconnect from XTS marketdata REST API."""
        if self.session:
            await self.session.close()
            log.info("Disconnected from XTS marketdata")


class ContractLoader:
    """Loads and indexes contracts from the CSV file."""

    def __init__(self, csv_path: Path):
        self.csv_path = csv_path
        self.contracts: list = []
        self._load_csv()

    def _load_csv(self) -> None:
        """Load and index the CSV file."""
        log.info("Loading CSV file: %s", self.csv_path)

        if not self.csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {self.csv_path}")

        with open(self.csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            self.contracts = list(reader)

        log.info("CSV loaded. Total contracts: %d", len(self.contracts))

    def get_nearest_expiry_futures(
        self, name: str = NIFTY_NAME
    ) -> Optional[Dict[str, Any]]:
        """
        Get the nearest expiry futures contract for the given name.

        Args:
            name: Instrument name (default: NIFTY)

        Returns:
            Contract dict or None if not found
        """
        today = datetime.now().date()
        valid_contracts = []

        for contract in self.contracts:
            try:
                # Filter for the specified name and futures (no option type)
                if contract["Name"] != name:
                    continue
                # if contract.get("OptionType"):  # Skip options
                #     continue
                if contract["Series"] != "FUTIDX":
                    continue

                # print("hi")
                expiry_str = contract["ContractExpiration"]
                expiry_date = datetime.fromisoformat(expiry_str).date()

                # print(expiry_date)

                if expiry_date >= today:
                    valid_contracts.append((contract, expiry_date))

            except (ValueError, KeyError):
                continue

        if not valid_contracts:
            log.warning("No valid futures contracts found for %s", name)
            return None

        # Sort by expiry date ascending and pick the first
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
        """
        Get the ATM option contract for the given underlying price.

        Args:
            underlying_price: Current underlying price
            name: Instrument name (default: NIFTY)
            option_type: Option type (CE or PE)

        Returns:
            Contract dict or None if not found
        """
        # Calculate ATM strike (round to nearest strike interval)
        atm_strike = round(underlying_price / STRIKE_INTERVAL) * STRIKE_INTERVAL

        log.info(
            "Underlying price: %.2f | Calculated ATM strike: %d (%s)",
            underlying_price,
            atm_strike,
            option_type,
        )

        today = datetime.now().date()
        valid_contracts = []

        # Map option type strings to CSV values
        option_type_map = {"CE": "3", "PE": "4"}
        csv_option_type = option_type_map.get(option_type.upper(), option_type)

        for contract in self.contracts:
            try:
                # Filter for the specified name, strike, and option type
                if contract["Name"] != name:
                    continue
                if contract.get("OptionType") != csv_option_type:
                    continue

                strike = int(float(contract["StrikePrice"]))
                if strike != atm_strike:
                    continue

                expiry_str = contract["ContractExpiration"]
                expiry_date = datetime.fromisoformat(expiry_str).date()

                if expiry_date >= today:
                    valid_contracts.append((contract, expiry_date))
            except (ValueError, KeyError):
                continue

        if not valid_contracts:
            log.warning(
                "No valid %s options found for strike %d", option_type, atm_strike
            )
            return None

        # Sort by expiry date ascending and pick the first
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


async def main():
    """Main entry point."""
    log.info("Starting Nifty ATM LTP Fetcher...")

    # Check if API credentials are set
    if not XTS_API_KEY or not XTS_API_SECRET:
        log.error(
            "XTS_API_KEY and XTS_API_SECRET must be set in the script. "
            "Please edit the file and add your credentials."
        )
        return

    try:
        # Load contracts from CSV
        loader = ContractLoader(CSV_PATH)

        # Get nearest expiry futures contract
        futures_contract = loader.get_nearest_expiry_futures()
        if not futures_contract:
            log.error("Could not find nearest expiry futures contract")
            return

        # Connect to XTS marketdata
        client = XTSMarketDataClient(XTS_API_KEY, XTS_API_SECRET)
        await client.connect()

        # Get LTP for futures (as proxy for underlying close)
        futures_id = int(futures_contract["ExchangeInstrumentID"])
        futures_ltp = await client.get_ltp(
            futures_id, futures_contract["ExchangeSegment"]
        )

        ###########################################################

        if futures_ltp is None:
            log.error("Could not get futures LTP")
            await client.disconnect()
            return

        log.info("Nifty50 Futures LTP: %.2f", futures_ltp)

        # Get ATM CE option
        ce_contract = loader.get_atm_options(futures_ltp, option_type="CE")
        ce_ltp = None
        if ce_contract:
            ce_id = int(ce_contract["ExchangeInstrumentID"])
            ce_ltp = await client.get_ltp(ce_id, ce_contract["ExchangeSegment"])
            if ce_ltp is not None:
                log.info("ATM CE LTP: %.2f", ce_ltp)

        # Get ATM PE option
        pe_contract = loader.get_atm_options(futures_ltp, option_type="PE")
        pe_ltp = None
        if pe_contract:
            pe_id = int(pe_contract["ExchangeInstrumentID"])
            pe_ltp = await client.get_ltp(pe_id, pe_contract["ExchangeSegment"])
            if pe_ltp is not None:
                log.info("ATM PE LTP: %.2f", pe_ltp)

        # Summary
        print("\n" + "=" * 60)
        print("SUMMARY")
        print("=" * 60)
        print(f"Underlying (Futures) LTP: {futures_ltp:.2f}")
        print(f"ATM Strike: {round(futures_ltp / STRIKE_INTERVAL) * STRIKE_INTERVAL}")
        if ce_ltp is not None:
            print(f"ATM CE LTP: {ce_ltp:.2f}")
        else:
            print("ATM CE LTP: N/A")
        if pe_ltp is not None:
            print(f"ATM PE LTP: {pe_ltp:.2f}")
        else:
            print("ATM PE LTP: N/A")
        print("=" * 60)

        # Disconnect
        await client.disconnect()

    except Exception as e:
        log.exception("Error in main:")
        return


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Script stopped by user")
