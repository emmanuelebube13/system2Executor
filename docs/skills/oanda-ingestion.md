# OANDA Ingestion Patterns

**Skill ID:** `oanda-ingestion`
**File:** `docs/implementation-roadmap/system-1-model-building/tasks/skills/oanda-ingestion.md`
**Applies To:** `data-pipeline-agent` (MODEL-001).

---

## API Configuration

From `.env`:
```
OANDA_API_KEY=...
OANDA_URL=https://api-fxpractice.oanda.com
OANDA_ACCOUNT_ID_DEMO=101-002-38449021-001
```

Client:
```python
from oandapyV20 import API
import oandapyV20.endpoints.instruments as instruments

api = API(access_token=os.environ["OANDA_API_KEY"], environment="practice")
```

---

## Granularity Codes

| Our Code | OANDA Code | Role |
|----------|-----------|------|
| `D1` | `D` | Primary modeling/regime |
| `H4` | `H4` | Entry timing |
| `W1` | `W` | Macro context |
| `H1` | `H1` | Legacy (preserve, do not modify) |

---

## Candle Request Pattern

```python
params = {
    "granularity": "D",       # OANDA code
    "count": 500,             # Max per page (OANDA limit)
    "from": "2005-01-01T00:00:00Z",   # ISO8601 UTC
    "price": "MAB",           # Midpoint candles
    "includeFirst": True,
}
r = instruments.InstrumentsCandles(instrument="EUR_USD", params=params)
api.request(r)
data = r.response
candles = data.get("candles", [])
```

**Critical:** OANDA returns candles in **ascending** time order within each page.

---

## Only Complete Candles

```python
complete_candles = [c for c in candles if c.get("complete", False)]
```

Skip in-progress (current) candles. The `complete` flag is set by OANDA when the candle's interval has fully elapsed.

---

## Candle Parsing

```python
def parse_candle(c):
    return {
        "bar_time_utc": c["time"],                              # ISO8601 string
        "open":  float(c["mid"]["o"]),
        "high":  float(c["mid"]["h"]),
        "low":   float(c["mid"]["l"]),
        "close": float(c["mid"]["c"]),
        "volume": int(c.get("volume", 0)),
        "complete": c["complete"],
    }
```

Full OHLC candles: `c["complete"] == True` means the candle period is finished.

---

## Paging / Backfill Loop

```python
cursor = load_cursor(instrument, granularity)  # From results/state/ingest_progress.json
if cursor is None:
    cursor = datetime(2005, 1, 1, tzinfo=timezone.utc)

while cursor < datetime.now(timezone.utc):
    candles = fetch_page(instrument, granularity, from_time=cursor, count=500)
    # candles = [c for c in candles if c["complete"]]  -- filter
    run_dq_checks(candles)
    upsert(candles)
    cursor = candles[-1]["time"]  # Advance cursor to last candle time
    save_cursor(instrument, granularity, cursor)
```

**Tail page:** If OANDA returns fewer than 500 candles and none newer, the backfill for that (instrument, granularity) is complete.

---

## Rate Limits & Backoff

```python
import time
import random

def fetch_with_backoff(instrument, granularity, **params, max_retries=5):
    for attempt in range(max_retries):
        try:
            r = instruments.InstrumentsCandles(instrument=instrument, params=params)
            api.request(r)
            return r.response
        except V20Error as e:
            if e.code == 429 or (500 <= (e.code or 0) < 600):
                wait = min(60, (2 ** attempt)) * (0.75 + random.random() * 0.5)  # 1s→2s→4s→8s→16s with jitter
                time.sleep(wait)
                continue
            raise
    raise RuntimeError(f"Max retries exceeded for {instrument}/{granularity}")
```

**Jitter is critical** — without it, multiple agents hitting OANDA simultaneously could self-synchronize and keep triggering 429s.

---

## Resumable Cursor Pattern

```python
# results/state/ingest_progress.json
import json, os

STATE_FILE = "results/state/ingest_progress.json"

def load_cursor(instrument, granularity):
    if not os.path.exists(STATE_FILE):
        return None
    with open(STATE_FILE) as f:
        state = json.load(f)
    ts = state.get(instrument, {}).get(granularity, {}).get("last_bar_utc")
    return datetime.fromisoformat(ts) if ts else None

def save_cursor(instrument, granularity, last_bar_utc):
    state = {}
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            state = json.load(f)
    state.setdefault(instrument, {})[granularity] = {"last_bar_utc": last_bar_utc.isoformat(), "backsfill_complete": True}
    # Atomic write
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=2)
    os.rename(tmp, STATE_FILE)
```

---

## Data Quality Checks (Per Batch)

```python
def run_dq_checks(candles):
    quarantined = []

    # 1. OHLC sanity
    for c in candles:
        if c["low"] > min(c["open"], c["close"]) or c["high"] < max(c["open"], c["close"]):
            quarantined.append((c, "OHLC_SANITY"))
        if c["open"] <= 0 or c["high"] <= 0 or c["low"] <= 0 or c["close"] <= 0:
            quarantined.append((c, "NON_POSITIVE_PRICE"))

    # 2. Monotonic bar times (within page)
    for i in range(1, len(candles)):
        if candles[i]["bar_time_utc"] <= candles[i-1]["bar_time_utc"]:
            quarantined.append((candles[i], "NON_MONOTONIC"))

    # 3. Duplicate detection (on natural key)
    from collections import Counter
    keys = [(c["instrument"], c["granularity"], c["bar_time_utc"]) for c in candles]
    dup_keys = {k for k, v in Counter(keys).items() if v > 1}
    for c in candles:
        if (c["instrument"], c["granularity"], c["bar_time_utc"]) in dup_keys:
            quarantined.append((c, "DUPLICATE"))

    ok = [c for c in candles if c not in [q[0] for q in quarantined]]
    return ok, quarantined
```

---

## Expected Gaps (Weekend/Holiday)

- **Weekend gaps** (Friday close to Sunday open for forex) are expected and should be logged at INFO level, not quarantined or flagged as errors.
- **Holiday gaps** (e.g., Christmas, New Year) are expected for the relevant currency pairs.
- **Gap detection:** expected next bar = previous bar + interval. If gap > 1.5 * interval, log it but only quarantine if the gap appears in a non-weekend, non-holiday window.
- **Coverage metric:** missing-expected-bar ratio < 0.5%, excluding documented closures.

---

## The Saturday Cron Pattern (Preserve)

Existing ingestion uses `shell/cron_oanda_ingest_saturday.sh`. MODEL-001 is additive — D1/W1 are new granularities. Do NOT alter or remove the Saturday cron until all downstream tasks have been migrated to D1/H4/W1.

---

## Per-Instrument History Limitations

Some instruments' OANDA practice history starts after 2005:
- Document the earliest available date in the ingest manifest.
- Backfill only from the actual earliest available bar, not from 2005.
- Flag in `results/state/ingest_progress.json` with `"history_start_override": "2010-01-01"`.
