"""XTS Marketdata REST client for live LTP quotes."""

from __future__ import annotations

import json
import logging
from typing import Optional

import aiohttp

from market_data.contracts import segment_to_code
from oms.utils.env import env

log = logging.getLogger("NIFTY_ATM_LTP")

XTS_REST_URL = env(
    "XTS_MD_REST_URL", "https://eztrade.wealthdiscovery.in/apimarketdata"
)


class XTSMarketDataClient:
    """Async XTS Marketdata API client for fetching LTP via HTTP REST."""

    def __init__(self, api_key: str, api_secret: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self.access_token: Optional[str] = None
        self.session: Optional[aiohttp.ClientSession] = None
        self.rest_url = XTS_REST_URL

    async def connect(self) -> None:
        if not self.api_key or not self.api_secret:
            raise ValueError("XTS_API_KEY and XTS_API_SECRET must be set")

        log.info("Connecting to XTS marketdata REST API...")
        try:
            self.session = aiohttp.ClientSession()
            login_url = f"{self.rest_url}/auth/login"
            login_payload = {
                "secretKey": self.api_secret,
                "appKey": self.api_key,
            }

            async with self.session.post(
                login_url, json=login_payload, verify_ssl=False
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("type") == "success":
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
        if not self.session or not self.access_token:
            raise Exception("Not connected to XTS marketdata")

        try:
            quote_url = f"{self.rest_url}/instruments/quotes"
            headers = {
                "xts-api-key": self.api_key,
                "Authorization": self.access_token,
                "Content-Type": "application/json",
            }
            # 1501 = touchline snapshot, 1512 = LTP event. Some servers have no
            # touchline cached for an instrument (e.g. after a restart outside
            # market hours) but still answer the LTP message code.
            message_codes = [1501, 1512]

            for message_code in message_codes:
                payload = {
                    "instruments": [
                        {
                            "exchangeSegment": segment_to_code(exchange_segment),
                            "exchangeInstrumentID": exchange_instrument_id,
                        }
                    ],
                    "xtsMessageCode": message_code,
                    "publishFormat": "JSON",
                }
                async with self.session.post(
                    quote_url, json=payload, headers=headers, verify_ssl=False
                ) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        log.warning(
                            "HTTP %d for message code %d: %s",
                            response.status,
                            message_code,
                            error_text,
                        )
                        continue

                    data = await response.json()
                    list_quotes = data["result"]["listQuotes"]
                    if not list_quotes:
                        log.warning(
                            "No quotes for message code %d: %s", message_code, data
                        )
                        continue

                    quote = json.loads(list_quotes[0])
                    ltp = quote.get("LastTradedPrice")
                    if ltp is not None:
                        return float(ltp)
                    close = quote.get("Close")
                    if close is not None:
                        log.warning(
                            "No LTP for instrument %d; using Close price %s",
                            exchange_instrument_id,
                            close,
                        )
                        return float(close)
                    log.warning("No LTP in quote response: %s", quote)

            log.error(
                "No quote data available for instrument %d (tried message codes %s)",
                exchange_instrument_id,
                message_codes,
            )
            return None
        except Exception as e:
            log.error(
                "Error getting LTP for instrument ID %d: %s", exchange_instrument_id, e
            )
            return None

    async def disconnect(self) -> None:
        if self.session:
            await self.session.close()
            log.info("Disconnected from XTS marketdata")
