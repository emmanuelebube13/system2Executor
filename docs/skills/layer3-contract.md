# Layer 3 Champion Artifact Contract

**Skill ID:** `layer3-contract`
**File:** `docs/implementation-roadmap/system-1-model-building/tasks/skills/layer3-contract.md`
**Applies To:** `ml-regime-agent` (MODEL-006), `serializer-infra-agent` (MODEL-007), `queue-nlp-agent` (MODEL-008).

---

## Champion Artifact Triad

Every Layer 3 champion deployment produces exactly three files in `models/`:

| File | Content | Serialization |
|------|---------|---------------|
| `champion_model.pkl` | Trained ML model (XGBoost, LightGBM, or sklearn pipeline) | `pickle` / `joblib` |
| `champion_preprocessor.pkl` | Fitted `ColumnTransformer` with column-name tracking | `pickle` / `joblib` |
| `champion_manifest.json` | Metadata: features, thresholds, hashes, OOS uplift | JSON |

---

## Champion Manifest Schema

```json
{
  "manifest_version": "1.0.0",
  "created_at_utc": "2026-06-23T14:00:00Z",
  "model_type": "xgboost|lightgbm",
  "model_params": {
    "max_depth": 6,
    "learning_rate": 0.05,
    "n_estimators": 200
  },
  "features": [
    "returns_1",
    "atr_14",
    "price_position_20",
    "volatility_20",
    "prob_trending_up",
    "prob_trending_down",
    "prob_ranging",
    "prob_high_vol",
    "regime_smoothed"
  ],
  "categorical_features": ["regime_smoothed"],
  "target_column": "is_winner",
  "approval_threshold": 0.20,
  "dynamic_thresholds": {
    "Trending-Up": 0.72,
    "Trending-Down": 0.65,
    "Ranging": 0.80,
    "High-Vol": 0.55,
    "fallback": 0.75
  },
  "turnover_limits": {
    "min_approval_rate": 0.01,
    "max_approval_rate": 0.35
  },
  "oos_uplift": {
    "approved_pnl": 1234.56,
    "rejected_pnl": -89.01,
    "uplift": 1323.57,
    "p_value": 0.012,
    "significant": true
  },
  "regime_features": ["prob_trending_up", "prob_trending_down", "prob_ranging", "prob_high_vol", "regime_smoothed"],
  "feature_set_version": "1.0.0",
  "regime_model_version": "hmm_v1",
  "training_run_id": "mlflow_run_uuid",
  "selection_mode": "strict",
  "sha256": {
    "champion_model.pkl": "abc123...",
    "champion_preprocessor.pkl": "def456..."
  }
}
```

---

## Feature Alignment (ColumnTransformer)

The `ColumnTransformer` in `src/layer3_ml/feature_alignment.py` tracks column names for train/inference parity:

```python
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OneHotEncoder

# Build preprocessor
numeric_features = [
    "returns_1", "atr_14", "price_position_20", "volatility_20",
    "prob_trending_up", "prob_trending_down", "prob_ranging", "prob_high_vol",
]
categorical_features = ["regime_smoothed"]

preprocessor = ColumnTransformer([
    ("num", StandardScaler(), numeric_features),
    ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), categorical_features),
], remainder="drop")

# After fitting, store the expected column names
fitted_columns = (
    numeric_features +
    list(preprocessor.named_transformers_["cat"].get_feature_names_out(categorical_features))
)
```

**At inference time:**
```python
# Verify incoming data has the expected columns
assert set(fitted_columns) == set(incoming_df.columns), \
    f"Feature mismatch: expected {fitted_columns}, got {incoming_df.columns}"
```

---

## SHA256 Integrity

```python
import hashlib
import pickle

def compute_artifact_hash(model_or_preprocessor):
    """Compute deterministic SHA256 of a sklearn-compatible model object."""
    serialized = pickle.dumps(model_or_preprocessor, protocol=5)
    return hashlib.sha256(serialized).hexdigest()

def verify_champion_artifacts(model_path, preprocessor_path, manifest_path):
    with open(manifest_path) as f:
        manifest = json.load(f)

    with open(model_path, "rb") as f:
        model = pickle.load(f)
    with open(preprocessor_path, "rb") as f:
        preprocessor = pickle.load(f)

    model_hash = compute_artifact_hash(model)
    preprocessor_hash = compute_artifact_hash(preprocessor)

    assert model_hash == manifest["sha256"]["champion_model.pkl"], \
        f"Model hash mismatch: {model_hash} != {manifest['sha256']['champion_model.pkl']}"
    assert preprocessor_hash == manifest["sha256"]["champion_preprocessor.pkl"], \
        f"Preprocessor hash mismatch: {preprocessor_hash} != {manifest['sha256']['champion_preprocessor.pkl']}"

    return True
```

---

## Dynamic Threshold Resolution

```python
def get_threshold_for_signal(signal_regime, manifest):
    """
    Resolve the approval threshold for a signal's regime.
    Falls back to fallback threshold if regime not in dynamic_thresholds.
    """
    thresholds = manifest.get("dynamic_thresholds", {})

    if signal_regime in thresholds:
        return thresholds[signal_regime]
    else:
        # Fall back: dynamic_thresholds.fallback → manifest.approval_threshold → env var
        return thresholds.get(
            "fallback",
            manifest.get("approval_threshold", float(os.environ.get("LAYER3_APPROVAL_THRESHOLD", 0.20)))
        )


def score_signal(signal_features, signal_regime, model, preprocessor, manifest):
    X = preprocessor.transform(signal_features)
    score = model.predict_proba(X)[0, 1]   # Probability of positive class (winner)

    threshold = get_threshold_for_signal(signal_regime, manifest)
    approved = score >= threshold

    return score, approved, threshold
```

---

## Degenerate Model Refusal

The training pipeline must refuse to promote a degenerate model (analogous to existing Layer 3 behavior):

```python
def check_model_is_degenerate(model, X_val, y_val, manifest):
    """Returns True if the model should NOT be promoted."""

    # 1. Approval rate outside turnover band
    scores = model.predict_proba(X_val)[:, 1]
    approval_rate = (scores >= manifest["approval_threshold"]).mean()

    min_rate = manifest.get("turnover_limits", {}).get("min_approval_rate", 0.01)
    max_rate = manifest.get("turnover_limits", {}).get("max_approval_rate", 0.35)

    if approval_rate < min_rate:
        return True, f"Approval rate {approval_rate:.2%} below min {min_rate:.2%}"
    if approval_rate > max_rate:
        return True, f"Approval rate {approval_rate:.2%} above max {max_rate:.2%}"

    # 2. All predictions same class
    predictions = model.predict(X_val)
    unique = np.unique(predictions)
    if len(unique) == 1:
        return True, f"Degenerate: all predictions are class {unique[0]}"

    # 3. OOS uplift not significant (if computed)
    uplift = manifest.get("oos_uplift", {})
    if uplift.get("significant") is False:
        return True, "OOS uplift not significant"

    return False, None


# In training pipeline:
is_degenerate, reason = check_model_is_degenerate(model, X_val, y_val, manifest)
if is_degenerate:
    print(f"Refusing to promote degenerate model: {reason}")
    if not args.allow_degenerate:   # Default: refuse
        sys.exit(1)
```

---

## Legacy Fallback Contract

Layer 4 currently supports a fallback to legacy artifacts:

```python
# Layer 4 fallback loading pattern (from src/layer4_executor/live_pipeline.py):
def load_model_artifacts():
    if all(os.path.exists(f"models/{f}") for f in CHAMPION_FILES):
        # Load champion
        model = joblib.load("models/champion_model.pkl")
        preprocessor = joblib.load("models/champion_preprocessor.pkl")
        with open("models/champion_manifest.json") as f:
            manifest = json.load(f)
    else:
        # Fallback to legacy
        model = joblib.load("models/best_ml_gatekeeper_sklearn.pkl")
        preprocessor = joblib.load("models/best_ml_gatekeeper_preprocessor.pkl")
        manifest = None  # No manifest for legacy

    return model, preprocessor, manifest
```

**MODEL-006 must preserve this fallback.** The champion contract is additive — it enhances the legacy path but does not break it.

---

## Tournament Selection (Existing Pattern)

The tournament selection in `src/layer3_ml/training/train_ml_gatekeeper.py` is preserved:
- Multiple model candidates trained.
- Best selected by validation metrics.
- SHA256 hashed for artifact integrity.
- `--selection-mode strict|fallback` controls behavior.
- `--promote-as-champion` writes champion artifacts.

---

## Mandatory Fields Before Promotion

Before the champion manifest can be used by MODEL-008 (scoring), it MUST contain:

- [ ] `features` list (non-empty)
- [ ] `approval_threshold` (or `dynamic_thresholds`)
- [ ] `sha256` for both model and preprocessor
- [ ] `feature_set_version` (traceable to MODEL-002 lineage)
- [ ] `regime_model_version` (traceable to MODEL-003, if regime features present)
- [ ] `training_run_id` (traceable to MLflow)

Missing any → refuse to score signals. The queue producer (MODEL-008) must validate the manifest before publishing.
