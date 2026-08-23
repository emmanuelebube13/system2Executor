"""Tests for state->label mapping and posterior ordering (regime_mapping)."""

from __future__ import annotations

import numpy as np
import pytest

from system2.artifact_sync import regime_mapping as M


# ----- many-to-one / incomplete mappings (2026-08-21 bundle) ----------------------
#
# mapping is state -> label and is many-to-one. The live H4 model maps
# {1: Trending-Down, 0: Ranging, 2: High-Vol, 3: Ranging}: two states on Ranging,
# none on Trending-Up. Inverting that dict dropped a state's probability mass and
# raised KeyError on the absent label.

def test_duplicate_labels_sum_rather_than_overwrite():
    mapping = {0: "Ranging", 1: "Trending-Down", 2: "High-Vol", 3: "Ranging"}
    probs = np.array([[0.1, 0.2, 0.3, 0.4]])          # states 0..3
    out = M.order_probabilities(probs, mapping)
    # SEMANTIC_ORDER = [Trending-Up, Trending-Down, Ranging, High-Vol]
    assert out[0].tolist() == pytest.approx([0.0, 0.2, 0.5, 0.3])
    assert out.sum() == pytest.approx(1.0), "probability mass must be preserved"


def test_a_label_no_state_carries_is_zero_not_an_error():
    mapping = {0: "Ranging", 1: "Trending-Down", 2: "High-Vol", 3: "Ranging"}
    out = M.order_probabilities(np.array([[0.25, 0.25, 0.25, 0.25]]), mapping)
    assert out[0][M.SEMANTIC_ORDER.index("Trending-Up")] == 0.0


def test_the_live_h1_mapping_round_trips():
    """H1 from the 2026-08-21 bundle: two Trending-Up states, no Ranging."""
    mapping = {1: "Trending-Up", 2: "Trending-Up", 3: "Trending-Down", 0: "High-Vol"}
    probs = np.array([[0.4, 0.1, 0.2, 0.3]])
    out = M.order_probabilities(probs, mapping)
    assert out[0].tolist() == pytest.approx([0.3, 0.3, 0.0, 0.4])
    assert out.sum() == pytest.approx(1.0)


def test_one_to_one_mapping_is_unchanged():
    """The old behaviour must survive for bundles that were already 1:1."""
    mapping = {0: "Trending-Up", 1: "Trending-Down", 2: "Ranging", 3: "High-Vol"}
    probs = np.array([[0.1, 0.2, 0.3, 0.4], [0.4, 0.3, 0.2, 0.1]])
    out = M.order_probabilities(probs, mapping)
    assert np.allclose(out, probs)


def test_mapping_referencing_a_nonexistent_state_is_refused():
    mapping = {0: "Ranging", 7: "Trending-Up"}
    with pytest.raises(ValueError, match="outside the model"):
        M.order_probabilities(np.array([[0.5, 0.5]]), mapping)
