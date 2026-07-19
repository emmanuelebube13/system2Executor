"""Deterministic regime state->label mapping + smoothing — self-contained copy.

Copied from the monolith ``src/system1/regime/mapping.py`` (pure, no DB/network) so
System 2's live inference maps states and orders probabilities identically to training.
See skill ``docs/skills/hmm-semantic-mapping.md``.
"""

from __future__ import annotations

from typing import Dict, List

import numpy as np

SEMANTIC_ORDER: List[str] = ["Trending-Up", "Trending-Down", "Ranging", "High-Vol"]
PROB_COLUMNS = ["prob_trending_up", "prob_trending_down", "prob_ranging", "prob_high_vol"]


def map_states_to_labels(
    means: np.ndarray, feature_names: List[str], direction_feature: str = "returns_1"
) -> Dict[int, str]:
    """Deterministic state->semantic mapping by component means.

    High-Vol = highest (volatility_20 + atr_14); among the rest, Trending-Up = highest
    mean ``direction_feature``, Trending-Down = lowest, Ranging = remaining.
    """
    n = means.shape[0]
    vol_i = feature_names.index("volatility_20")
    atr_i = feature_names.index("atr_14")
    ret_i = feature_names.index(direction_feature)

    vol_scores = means[:, vol_i] + means[:, atr_i]
    high_vol = int(np.argmax(vol_scores))
    remaining = [i for i in range(n) if i != high_vol]
    ret_scores = {i: means[i, ret_i] for i in remaining}
    up = max(ret_scores, key=ret_scores.get)
    down = min(ret_scores, key=ret_scores.get)
    ranging = [i for i in remaining if i not in (up, down)][0]
    return {high_vol: "High-Vol", up: "Trending-Up", down: "Trending-Down", ranging: "Ranging"}


def order_probabilities(posteriors: np.ndarray, mapping: Dict[int, str]) -> np.ndarray:
    """Reorder raw state posteriors into SEMANTIC_ORDER columns."""
    semantic_to_state = {v: k for k, v in mapping.items()}
    return np.column_stack(
        [posteriors[:, semantic_to_state[label]] for label in SEMANTIC_ORDER]
    )


def persistence_smooth(labels: List[str], min_bars: int = 3) -> List[str]:
    """Causal batch debounce: suppress regime segments shorter than ``min_bars``.

    The smoothed label at bar t depends only on bars 0..t (never future). Used for
    offline/reference parity; live inference uses OnlineDebouncer in live_regime.py.
    """
    smoothed = list(labels)
    n = len(labels)
    i = 0
    while i < n:
        j = i
        while j < n and labels[j] == labels[i]:
            j += 1
        if (j - i) < min_bars and i > 0:
            smoothed[i:j] = [smoothed[i - 1]] * (j - i)
        i = j
    if n:
        k = 0
        while k < n and smoothed[k] == smoothed[0]:
            k += 1
        if k < min_bars and k < n:
            smoothed[:k] = [smoothed[k]] * k
    return smoothed


def flicker_rate(labels: List[str]) -> float:
    arr = np.asarray(labels)
    if len(arr) < 2:
        return 0.0
    return float((arr[1:] != arr[:-1]).sum()) / (len(arr) - 1)
