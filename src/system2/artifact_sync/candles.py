"""Candle sources for the live regime detector (EXEC-002).

``OandaCandleSource`` reads completed H1/H4 candles via OANDA v20
``GET /v3/instruments/{instrument}/candles``. The full broker adapter (EXEC-006) is a
separate concern; this is a read-only candle reader scoped to regime inference. The
oandapyV20 SDK is imported lazily so dev/tests run without it.
"""

from __future__ import annotations

import pandas as pd

from system2.common.secrets import Secrets, get_secrets


class OandaCandleSource:
    """Fetches recent completed candles from OANDA practice/live (read-only)."""

    def __init__(self, secrets: Secrets | None = None) -> None:
        self.secrets = secrets or get_secrets()
        env = (self.secrets.get("OANDA_ENV", "practice") or "practice").lower()
        if env == "live":
            self._token = self.secrets.require("OANDA_LIVE_API_KEY")
        else:
            self._token = self.secrets.require("OANDA_PRACTICE_API_KEY")
        self._env = env

    def fetch_candles(self, instrument: str, granularity: str, count: int) -> pd.DataFrame:
        from oandapyV20 import API  # lazy import
        from oandapyV20.endpoints.instruments import InstrumentsCandles

        api = API(access_token=self._token, environment="live" if self._env == "live" else "practice")
        params = {"granularity": granularity, "count": count, "price": "M", "smooth": False}
        req = InstrumentsCandles(instrument=instrument, params=params)
        api.request(req)
        rows = []
        for c in req.response.get("candles", []):
            if not c.get("complete", False):
                continue  # never act on an unclosed candle
            mid = c["mid"]
            rows.append(
                {
                    "bar_time_utc": c["time"],
                    "open": float(mid["o"]),
                    "high": float(mid["h"]),
                    "low": float(mid["l"]),
                    "close": float(mid["c"]),
                    "volume": float(c.get("volume", 0)),
                }
            )
        df = pd.DataFrame(rows)
        if not df.empty:
            df["bar_time_utc"] = pd.to_datetime(df["bar_time_utc"], utc=True)
            df = df.sort_values("bar_time_utc").reset_index(drop=True)
        return df
