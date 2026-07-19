# Point-in-Time Leakage Prevention

**Skill ID:** `point-in-time-leakage`
**File:** `docs/implementation-roadmap/system-1-model-building/tasks/skills/point-in-time-leakage.md`
**Applies To:** Every agent that joins features, regime labels, or macro events to trade/signal times.

---

## The Golden Rule

> **No feature or label at bar `t` may use data from `t+1`, `t+2`, ...**

This rule must hold for:
- Feature computation (MODEL-002): rolling windows are trailing-only.
- Regime tagging (MODEL-004): regime label at trade entry is the label active at or before entry.
- Gatekeeper features (MODEL-006): regime probs joined at signal time.
- Macro features (MODEL-010): event sentiment only after release time.

---

## Trailing Windows (Feature Computation)

```python
import pandas as pd

def validate_trailing_window(df, feature_col, window_size):
    """
    Assert that feature_col at bar t depends only on rows up to t.
    Strategy: shift the input, recompute, compare.
    """
    df = df.sort_values("bar_time_utc")

    # Compute feature with original data
    original = df[feature_col].values

    # Inject a shock at row i and verify only rows >= i + window_size change
    for i in range(window_size, len(df) - window_size):
        df_modified = df.copy()
        df_modified.loc[df_modified.index[i], "Close"] *= 10  # 10x price spike

        # Recompute feature
        modified = recompute_feature(df_modified, feature_col)

        # Rows 0..i should be IDENTICAL (past unaffected)
        assert np.array_equal(original[:i+1], modified[:i+1], equal_nan=True), \
            f"Leakage: past rows changed at index {i} for {feature_col}"

        # Rows i+window_size.. should differ (future affected by the spike)
        # This is expected and correct.
```

## Rolling Features (Trailing-Only)

```python
# CORRECT: trailing window
df["returns_1"] = df["Close"].pct_change()  # Uses t-1 only

df["atr_14"] = compute_atr(df, window=14)    # Uses t-13 through t
# Implemented as: trailing max of true range over last 14 bars (not centered).

df["price_position_20"] = (
    (df["Close"] - df["Low"].rolling(20, min_periods=1).min())
    / (df["High"].rolling(20, min_periods=1).max() - df["Low"].rolling(20, min_periods=1).min())
)
```

**WRONG patterns to avoid:**
```python
# WRONG: uses future data
df["returns_1"] = df["Close"].shift(-1) / df["Close"] - 1  # Forward-looking

# WRONG: centered window
df["sma"] = df["Close"].rolling(20, center=True).mean()  # Uses t±10
```

---

## Regime Join at Trade Entry (MODEL-004)

```python
def get_regime_at_entry(trade_entry_time, asset_id, granularity, conn):
    """
    Point-in-time regime: the regime label active at or just before trade entry.
    NO future regime data used.
    """
    sql = text("""
        SELECT regime_smoothed, prob_trending_up, prob_trending_down, prob_ranging, prob_high_vol, bar_time_utc
        FROM fact_market_regime_v2
        WHERE asset_id = :asset_id
          AND granularity = :granularity
          AND bar_time_utc <= :entry_time          -- KEY: at or before, never after
        ORDER BY bar_time_utc DESC
        LIMIT 1
    """)
    result = conn.execute(sql, {
        "asset_id": asset_id,
        "granularity": granularity,
        "entry_time": trade_entry_time,
    }).fetchone()

    if result is None:
        # No regime label available yet (early history) — flag, don't drop
        return None  # Caller must handle missing regime

    return dict(result._mapping)


# Validation test
def test_no_future_regime_used(attributed_trades):
    for trade in attributed_trades:
        regime_time = trade["regime_bar_time_utc"]
        entry_time = trade["trade_entry_time_utc"]
        assert regime_time <= entry_time, \
            f"Future regime used! Entry: {entry_time}, Regime: {regime_time}"
```

---

## Macro Event Feature Join (MODEL-010)

```python
def get_macro_features_at_signal(signal_time, asset_id, conn):
    """
    Macro features known at signal time.
    Two types:
    1. Past events: sentiment, impact, time_since_last_event (KNOWN)
    2. Future scheduled events: time_to_next_event, in_event_window (KNOWN from calendar)
    BUT: sentiment/result of future events is UNKNOWN — do NOT include it.
    """
    # Past events: sentiment and impact are KNOWN
    past_features = conn.execute(text("""
        SELECT
            COALESCE(AVG(sentiment_score), 0) AS macro_sentiment_score,
            MAX(impact_level) AS macro_event_impact,
            EXTRACT(EPOCH FROM (:signal_time - MAX(release_time))) / 3600.0 AS hours_since_last_event
        FROM fact_macro_events
        WHERE asset_id = :asset_id
          AND release_time <= :signal_time          -- KEY: only released events
          AND release_time >= :signal_time - INTERVAL '24 hours'
    """), {"asset_id": asset_id, "signal_time": signal_time}).fetchone()

    # Future scheduled events: timing is KNOWN, sentiment is NOT
    next_event = conn.execute(text("""
        SELECT
            EXTRACT(EPOCH FROM (MIN(release_time) - :signal_time)) / 3600.0 AS hours_to_next_event,
            CASE WHEN MIN(release_time) <= :signal_time + INTERVAL '2 hours' THEN TRUE ELSE FALSE END AS in_event_window
        FROM fact_macro_events
        WHERE asset_id = :asset_id
          AND release_time > :signal_time            -- Scheduled, not yet released
          AND release_time <= :signal_time + INTERVAL '7 days'
    """), {"asset_id": asset_id, "signal_time": signal_time}).fetchone()

    return {
        "macro_sentiment_score": past_features["macro_sentiment_score"],
        "hours_since_last_event": past_features["hours_since_last_event"],
        "hours_to_next_event": next_event["hours_to_next_event"],
        "in_event_window": next_event["in_event_window"],
    }
```

**The trap:** `macro_sentiment_score` for a future FOMC meeting is unknowable until the release. The model must only see it AFTER release time. The *schedule* of the meeting (time, expected impact) is fair game because it's public ahead of time.

---

## Leakage Test Suite

Every agent that does time-series joins must pass these tests:

```python
def test_no_future_data_leakage():
    """Inject a future event and verify past rows don't change."""

# Test 1: Feature computation
def test_feature_leakage():
    df = load_price_data(instrument, granularity, date_range)
    df_original = compute_features(df)

    # Inject shock at arbitrary index
    idx = len(df) // 2
    df_shocked = df.copy()
    df_shocked.loc[df_shocked.index[idx], "Close"] *= 100
    df_shocked_features = compute_features(df_shocked)

    # Rows before the shock must be identical
    for col in feature_columns:
        assert (df_original[col].iloc[:idx+1] == df_shocked_features[col].iloc[:idx+1]).all(), \
            f"Leakage in feature {col}"

# Test 2: Regime join
def test_regime_join_no_lookahead():
    trade = {"entry_time": datetime(2026, 6, 15, 14, 0, tzinfo=timezone.utc)}
    regime = get_regime_at_entry(trade["entry_time"], asset_id, granularity, conn)
    assert regime["bar_time_utc"] <= trade["entry_time"]

# Test 3: Macro sentiment leak
def test_macro_sentiment_no_lookahead():
    signal_time = datetime(2026, 6, 15, 13, 0, tzinfo=timezone.utc)
    features = get_macro_features_at_signal(signal_time, asset_id, conn)

    # Verify: any sentiment comes from events with release_time <= signal_time
    events_used = conn.execute(text("""
        SELECT release_time, sentiment_score FROM fact_macro_events
        WHERE asset_id = :asset_id AND release_time <= :signal_time
    """), {"asset_id": asset_id, "signal_time": signal_time}).fetchall()

    for event in events_used:
        assert event["release_time"] <= signal_time
```

---

## Smoothing and Lag

Persistence smoothing (MODEL-003) introduces acceptable lag (up to 3 bars) but is NOT look-ahead:

```python
def test_smoothing_is_causal():
    """
    The smoothed label at bar t depends only on labels at bars 0..t.
    It is NOT a future-aware filter.
    """
    labels = np.array([1, 1, 2, 2, 1, 1, 1, 2, 1, 2, 2, 2])
    smoothed = persistence_smooth(labels, min_bars=3)

    # Append a new bar
    labels_ext = np.append(labels, [1])
    smoothed_ext = persistence_smooth(labels_ext, min_bars=3)

    # The first len(labels) entries must match OR be at most 3 bars behind
    # (because smoothing may "hold" a prior label pending confirmation)
    for i in range(len(labels)):
        if smoothed[i] != smoothed_ext[i]:
            # Permitted only if the change is due to a pending segment that just resolved
            assert abs(i - transition_point) <= 3
```

---

## Checklist for Every Agent

Before declaring a task complete, verify:

- [ ] All rolling-window computations use `.rolling()` or equivalent TRAILING windows (no `center=True`, no `.shift(-1)` for feature inputs).
- [ ] All joins to `Fact_Market_Regime_V2` filter `bar_time_utc <= trade/signal time`.
- [ ] All joins to `Fact_Macro_Events` filter `release_time <= signal_time` for sentiment.
- [ ] Future scheduled events may be used for timing but NEVER for sentiment/result.
- [ ] Leakage unit test passes (inject shock → only future rows change).
- [ ] Persistence smoothing is causal (tested by appending a new bar).
- [ ] `date_range` or time-window slicing uses `<=` (inclusive) not `>=` for backward lookups.
