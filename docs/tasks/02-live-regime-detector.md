# EXEC-002 — Live Regime Detector

**Task ID:** EXEC-002
**System:** System 2 — Execution Engine
**Priority:** P1-High
**Estimated Effort:** 2d
**Prerequisites:** EXEC-001, MODEL-003
**External Dependencies:**
- **OANDA candles API:** `GET /v3/instruments/{instrument}/candles` for the latest H1/H4 candles used as regime features. Live, granularity-aware market input.
- **Regime model artifact (MODEL-003 via EXEC-001):** the trained HMM lives in the verified `active` model set on Computer 2.
- **Secrets (FND-003):** OANDA key for candle reads.

## Objective
Run the downloaded HMM model to predict the live regime (with probabilities + persistence smoothing) on the latest candles for use by the signal/strategy selector.

## Current State
- Regime detection is currently **Layer 1** (`src/layer1_regime/Fact_market_regime_v2.py`), a K-Means batch job writing `Fact_Market_Regime_V2` (H1/H4). Layer 4 reads the latest regime row from that table (stage 2 of its 8 stages).
- There is **no** live, on-host HMM inference on Computer 2; the reorg introduces an HMM regime model (trained by MODEL-003) that must run on live candles where execution happens.
- Granularity contracts (H1/H4) must be preserved.

## Target State
- A regime-detection module on Computer 2 that:
  - Loads the HMM from the EXEC-001 `active` set (and falls back to `last_good` if the active HMM fails to load).
  - Pulls the latest N H1/H4 candles from OANDA, builds the same feature vector the model was trained on (feature contract supplied with MODEL-003's manifest).
  - Produces a current regime label **with per-state probabilities** and applies **persistence smoothing** (do not flip regime on a single noisy candle).
  - Exposes the smoothed regime + probabilities + as-of timestamp + `model_set_id` to the execution path and to Layer 5 telemetry.
- The output is consumed by the signal/strategy selector (downstream of System 3's decision, used by Layer 4 only to contextualize/log, never to override an AMS-approved order's risk).

## Technical Specification

**Feature contract:** read feature names/order/scaling from the HMM's companion manifest in the active set (mirrors how Layer 3 manifests carry feature lists). Candle features are derived per granularity (e.g., ATR/ADX/returns) consistent with MODEL-003 training — EXEC-002 must **not** invent features.

**Regime output object (text):**
```
{
  granularity: "H1" | "H4",
  as_of: ISO-8601 UTC (candle close time),
  raw_state: int,
  raw_probs: [p0, p1, ...],          # HMM posterior over states
  smoothed_label: string,            # mapped, persistence-smoothed
  smoothed_confidence: float,
  persistence_window: int,
  model_set_id: string,
  source: "hmm-live"
}
```

**Persistence smoothing (text / pseudo-code):** maintain a short rolling buffer of recent raw states; only change the published `smoothed_label` when the new state has held for `PERSISTENCE_WINDOW` consecutive predictions **or** its probability exceeds a high-confidence threshold. Otherwise hold the previous label.
```
if new_state == last_published: hold
elif count_consecutive(new_state) >= PERSISTENCE_WINDOW or prob(new_state) >= HIGH_CONF: switch
else: hold last_published
```

**Env vars:** `REGIME_GRANULARITIES` (default "H1,H4"), `REGIME_CANDLE_LOOKBACK`, `PERSISTENCE_WINDOW`, `REGIME_HIGH_CONF`, `REGIME_REFRESH_SEC`.

**Data flow (text):** on each refresh / new candle close → fetch candles → build features per granularity → HMM `predict_proba` → map state to label → apply persistence smoothing → publish regime object to in-process cache + Layer 5 endpoint + log. Compute per granularity independently (H1 and H4 do not share a buffer).

## Testing & Validation
- **Unit:** persistence smoothing holds on a single-candle flip, switches after `PERSISTENCE_WINDOW`, and switches immediately on high-confidence; feature vector matches the manifest's expected shape/order.
- **Determinism:** same candles + same model set ⇒ identical raw state/probabilities (seedless predict is deterministic).
- **Integration:** with a known candle window, the published regime matches an offline reference run of the same HMM.
- **Edge cases:** missing/short candle history → withhold a fresh label, keep last published, warn; corrupt/unloadable HMM in active set → fall back to `last_good` HMM and alert; **weekend gap** → no new closed candles, so no spurious regime change; **granularity** — H1 and H4 produce independent labels and never cross-contaminate.
- **Failure mode:** OANDA candle fetch fails → serve last good regime with a staleness flag; alert if stale beyond threshold.

## Rollback Plan
- Feature-flag the live HMM detector; if disabled, the execution path falls back to reading the existing `Fact_Market_Regime_V2` (Layer 1 K-Means) as today — no loss of a regime signal.
- The detector is read-only (no writes to broker or core fact tables beyond optional telemetry), so disabling it has no side effects on placed orders.

## Acceptance Criteria
- [ ] The detector loads the HMM from the EXEC-001 active set and predicts a regime with per-state probabilities for both H1 and H4.
- [ ] Persistence smoothing prevents single-candle regime flips while still switching on sustained or high-confidence changes.
- [ ] A failed/corrupt active HMM transparently falls back to `last_good` (or to `Fact_Market_Regime_V2`) and raises an alert.
- [ ] The published regime object carries `as_of` and `model_set_id` for traceability and is available to Layer 5.

## Notes & Risks
- This detector **contextualizes** execution; it must never alter the risk/size of an AMS-approved order (that authority is System 3's). If a future design wants regime to gate execution, that gate belongs in System 3, not here.
- Risk: train/serve feature skew. Mitigate by sourcing the feature contract from the model manifest, not by re-deriving it independently.
