# HMM Semantic Mapping

**Skill ID:** `hmm-semantic-mapping`
**File:** `docs/implementation-roadmap/system-1-model-building/tasks/skills/hmm-semantic-mapping.md`
**Applies To:** `ml-regime-agent` (MODEL-003).

---

## HMM Configuration

```python
from hmmlearn.hmm import GaussianHMM
from sklearn.preprocessing import StandardScaler
import numpy as np

FIXED_RANDOM_SEED = 42  # Must be fixed for reproducibility

hmm = GaussianHMM(
    n_components=4,
    covariance_type="full",       # or "diag" for speed
    n_iter=1000,                  # Max EM iterations
    tol=1e-4,                     # Convergence tolerance
    random_state=FIXED_RANDOM_SEED,
    n_init=3,                     # Random restarts
    init_params="stmc",           # Initialize startprob, transmat, means, covars
    verbose=False,
)
```

**Input features** (from MODEL-002 feature store): standardized `[atr_14, adx, volatility_20, returns_1]` per granularity.

---

## Deterministic State→Label Mapping (THE CRITICAL PATTERN)

HMM states are unordered (0,1,2,3). They MUST be deterministically mapped to semantic labels. This mapping must be stored and reused for retrains.

```python
def map_states_to_labels(hmm, X, feature_names):
    """
    Deterministic mapping by state statistics.
    Returns dict: {state_index: "Trending-Up"|"Trending-Down"|"Ranging"|"High-Vol"}
    """
    n = hmm.n_components
    # Get which feature indices correspond to what
    vol_idx = feature_names.index("volatility_20")     # or atr_14 for vol proxy
    ret_idx = feature_names.index("returns_1")
    atr_idx = feature_names.index("atr_14")

    means = hmm.means_  # shape: (n_components, n_features)

    # High-Vol: highest mean volatility (or ATR)
    vol_scores = means[:, vol_idx] + means[:, atr_idx]  # Combine vol and ATR as vol proxy
    high_vol_state = int(np.argmax(vol_scores))

    # Among remaining, classify by mean returns
    remaining = [i for i in range(n) if i != high_vol_state]
    ret_scores = {i: means[i, ret_idx] for i in remaining}

    # Trending-Up: highest mean return (positive bias)
    trending_up_state = max(ret_scores, key=ret_scores.get)

    # Trending-Down: lowest mean return (negative bias)
    trending_down_state = min(ret_scores, key=ret_scores.get)

    # Ranging: the remaining state
    remaining2 = [i for i in remaining if i not in (trending_up_state, trending_down_state)]
    ranging_state = remaining2[0]

    mapping = {
        high_vol_state: "High-Vol",
        trending_up_state: "Trending-Up",
        trending_down_state: "Trending-Down",
        ranging_state: "Ranging",
    }
    return mapping
```

**This mapping must be serialized with the model** (`hmm_model.joblib`). On retrain, reuse the same mapping logic — do NOT hardcode state indices.

```python
# Package for serialization
hmm_package = {
    "model": hmm,
    "scaler": scaler,
    "state_label_mapping": mapping,    # {0: "Trending-Up", 1: "Trending-Down", ...}
    "feature_names": ["atr_14", "adx", "volatility_20", "returns_1"],
    "seed": FIXED_RANDOM_SEED,
}
joblib.dump(hmm_package, "models/hmm_model.joblib")
```

---

## Probability Vectors

```python
posteriors = hmm.predict_proba(X)  # shape: (n_samples, 4)

# Columns must map to the semantic order:
# prob_trending_up, prob_trending_down, prob_ranging, prob_high_vol

# Reorder by semantic label:
semantic_order = ["Trending-Up", "Trending-Down", "Ranging", "High-Vol"]
state_to_semantic = mapping          # {0: "Trending-Up", 1: "High-Vol", ...}
semantic_to_idx = {v: k for k, v in state_to_semantic.items()}

probs_ordered = np.column_stack([
    posteriors[:, semantic_to_idx[label]] for label in semantic_order
])

# Verify: sum to 1
assert np.allclose(probs_ordered.sum(axis=1), 1.0, atol=1e-6)
```

---

## Persistence Smoothing (3-bar minimum)

A **deterministic state machine (debounce)** — not a future-aware filter:

```python
def persistence_smooth(labels, min_bars=3):
    """
    Suppress regime changes that last fewer than min_bars consecutive bars.
    Uses only past data: the smoothed label at bar t depends on bars 0..t, never t+1.
    """
    smoothed = labels.copy()
    i = 0
    while i < len(labels):
        j = i
        while j < len(labels) and labels[j] == labels[i]:
            j += 1
        segment_len = j - i
        if segment_len < min_bars and i > 0:
            # Too short — revert to the prior regime
            smoothed[i:j] = smoothed[i-1]
        i = j
    return smoothed
```

**Validation:** No segment in `regime_smoothed` is shorter than 3 bars. Flicker rate (regime switches / total bars) must be lower than raw labels and lower than the K-Means baseline.

---

## Convergence & Quality Gates

```python
def check_hmm_quality(hmm, X, scores):
    """Returns (passed, reason_if_failed)."""
    # 1. Convergence
    if not hmm.monitor_.converged:
        return False, "HMM did not converge"

    # 2. Degenerate covariance
    for k in range(hmm.n_components):
        cov = hmm.covars_[k]
        eigvals = np.linalg.eigvalsh(cov)
        if np.any(eigvals < 1e-8):
            return False, f"Degenerate covariance in component {k}"

    # 3. All states populated (> 1%)
    labels = hmm.predict(X)
    _, counts = np.unique(labels, return_counts=True)
    min_pct = counts.min() / len(labels)
    if min_pct < 0.01:
        return False, f"Component has < 1% of samples ({min_pct:.3%})"

    return True, None

# Usage in pipeline:
passed, reason = check_hmm_quality(hmm, X, scores)
if not passed:
    print(f"HMM quality gate failed: {reason}. Falling back to K-Means.")
    return kmeans_fallback(X)  # Existing Fact_market_regime_v2.py path
```

---

## K-Means Fallback Contract

The existing K-Means path in `src/layer1_regime/Fact_market_regime_v2.py` is **preserved as a first-class fallback**:

1. If the HMM fails any quality gate → call existing K-Means function.
2. Set `Regime_Model = 'KMeans'` in `Fact_Market_Regime_V2`.
3. For K-Means rows, `prob_*` columns are one-hot (1.0 for assigned cluster, 0.0 for others).
4. `regime_raw == regime_smoothed` (no smoothing applied to K-Means).
5. Log the fallback decision with the reason.

---

## Schema (Additive to Fact_Market_Regime_V2)

New columns (add, do not drop existing):
```sql
ALTER TABLE fact_market_regime_v2
  ADD COLUMN IF NOT EXISTS regime_model VARCHAR(10) DEFAULT 'KMeans',
  ADD COLUMN IF NOT EXISTS regime_raw VARCHAR(20),
  ADD COLUMN IF NOT EXISTS regime_smoothed VARCHAR(20),
  ADD COLUMN IF NOT EXISTS prob_trending_up DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS prob_trending_down DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS prob_ranging DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS prob_high_vol DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS model_version VARCHAR(50);
```

Existing columns (`asset_id`, `granularity`, `bar_time_utc`, K-Means cluster columns, regime label) are preserved. H1/H4 granularity support is preserved; D1 is added.

---

## Flicker Rate Comparison

```python
def flicker_rate(labels):
    switches = (labels[1:] != labels[:-1]).sum()
    return switches / (len(labels) - 1)

# Assert: smoothed < raw < kmeans_baseline
assert flicker_rate(smoothed) < flicker_rate(raw) < flicker_rate(kmeans_labels)
```

---

## Labeled Holdout for Accuracy

If a labeled holdout exists (e.g., manually labeled regime windows), compute:
```python
from sklearn.metrics import accuracy_score

predictions = hmm.predict(X_holdout)
mapped_predictions = [mapping[p] for p in predictions]
accuracy = accuracy_score(y_holdout, mapped_predictions)

# Gate: accuracy >= 0.70
assert accuracy >= 0.70, f"Regime accuracy {accuracy:.2%} below 70% threshold"
```
