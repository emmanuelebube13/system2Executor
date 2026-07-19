# Financial Metrics

**Skill ID:** `financial-metrics`
**File:** `docs/implementation-roadmap/system-1-model-building/tasks/skills/financial-metrics.md`
**Applies To:** `attribution-vetting-agent` (MODEL-004, MODEL-005), `ml-regime-agent` (MODEL-006 OOS uplift).

---

## Win Rate

```python
win_rate = winning_trades / total_trades  # float in [0, 1]

# Gate: >= 0.40 (40%)
```

---

## Profit Factor

```python
gross_profit = sum(trade.profit for trade in trades if trade.profit > 0)
gross_loss = abs(sum(trade.profit for trade in trades if trade.profit < 0))

if gross_loss == 0:
    profit_factor = float('inf')  # All winners
else:
    profit_factor = gross_profit / gross_loss

# Gate: >= 1.5
```

---

## Capital model (prerequisite for drawdown & recovery)

P/L is stored as per-trade **R-multiples** — there is no capital base in the data. To express
drawdown/recovery as real **percentages** (matching the gates), convert R-multiples into a
compounding equity curve by risking a fixed fraction `f` of equity per trade:

```python
DEFAULT_RISK_FRACTION = 0.01  # 1% per trade; aligned with the Quarter-Kelly / 2% risk cap

def equity_curve(r_multiples, f=DEFAULT_RISK_FRACTION):
    growth = np.maximum(1.0 + f * np.asarray(r_multiples, float), 1e-9)  # floor keeps it > 0
    return np.concatenate(([1.0], np.cumprod(growth)))                   # 1.0 = starting capital
```
This guarantees drawdown ∈ [0, 1). `f` cancels in Sharpe (a ratio) so it affects only
drawdown/recovery magnitude, not Sharpe or ranking order materially.

---

## Sharpe Ratio (Annualized)

**Annualize by realized trade frequency, NOT bar frequency.** A per-trade return series is
scaled by `sqrt(trades_per_year)`, where `trades_per_year = trade_count / years_spanned`.
(Annualizing by bars/year — e.g. 6048 for H1 — was a bug that overstated Sharpe ~19×.)

```python
import numpy as np

def annualized_sharpe(returns, trades_per_year):
    """returns: per-trade R-multiples. trades_per_year: realized cadence (count / years)."""
    r = np.asarray(returns, float)
    if len(r) < 2:
        return float('nan')
    std = np.std(r, ddof=1)
    if std < 1e-12:        # constant series => undefined risk-adjusted return
        return 0.0
    return (np.mean(r) / std) * np.sqrt(max(trades_per_year, 0.0))

# Gate: >= 0.8
```

---

## Maximum Drawdown

```python
def max_drawdown(r_multiples, f=DEFAULT_RISK_FRACTION):
    """Peak-to-trough of the fixed-fractional equity curve; a fraction in [0, 1)."""
    eq = equity_curve(r_multiples, f)
    peak = np.maximum.accumulate(eq)          # strictly positive by construction
    return float(np.max((peak - eq) / peak))

# Gate: <= 0.25 (25%).  Invariant: result is always in [0, 1] — assert this in tests.
```

---

## Recovery Factor

```python
def recovery_factor(r_multiples, f=DEFAULT_RISK_FRACTION):
    """total_return% / max_drawdown% on the same equity curve (unit-consistent)."""
    eq = equity_curve(r_multiples, f)
    total_return = eq[-1] - 1.0
    mdd = max_drawdown(r_multiples, f)
    return float('inf') if mdd == 0 and total_return > 0 else (total_return / mdd if mdd else 0.0)

# Gate: >= 3.0
```

---

## Sanity guard (must fail the run, not ship)

```python
MAX_PLAUSIBLE_DRAWDOWN = 1.0   # > 100% drawdown is impossible
MAX_PLAUSIBLE_SHARPE   = 10.0  # |annualized Sharpe| above this is not attainable
```
Any cell breaching these indicates a measurement bug. The attribution run collects violations
and raises, so corrupt metrics (the 118,280% drawdowns / Sharpe-42 class of bug) can never
silently reach the regime→strategy map.

---

## Expectancy

```python
# Average R-multiple per trade
expectancy = np.mean([trade.profit / abs(trade.risk) for trade in trades])
```

---

## Average R (Avg Win Size / Avg Loss Size)

```python
avg_win = np.mean([t.profit for t in trades if t.profit > 0]) if any(t.profit > 0 for t in trades) else 0
avg_loss = abs(np.mean([t.profit for t in trades if t.profit < 0])) if any(t.profit < 0 for t in trades) else 0

if avg_loss > 0:
    avg_R = avg_win / avg_loss
else:
    avg_R = float('inf')
```

---

## Bayesian Shrinkage (Low-Confidence Cells)

When a strategy×regime cell has too few trades, shrink its metrics toward the strategy's global metric:

```python
def bayesian_shrinkage(cell_metric, global_metric, cell_n, global_n, min_n=20):
    """
    Shrink a per-cell metric toward the global metric when sample size is small.
    weight on cell ≈ cell_n / (cell_n + min_n)
    """
    if cell_n >= min_n:
        return cell_metric, False  # No shrinkage, high confidence

    weight = cell_n / (cell_n + min_n)
    shrunk = weight * cell_metric + (1 - weight) * global_metric
    return shrunk, True  # Shrunk, low confidence
```

**Usage:**
```python
shrunk_pf, low_conf = bayesian_shrinkage(cell_pf, global_pf, cell_trades, global_trades, min_n=20)
```

The shrunk metric always lies between the cell metric and the global metric. Low-confidence cells cannot qualify a strategy in MODEL-005 regardless of their shrunk values.

---

## Walk-Forward OOS Measurement

```python
def oos_month_span(folds):
    """
    folds: list of (train_start, train_end, oos_start, oos_end) tuples.
    Returns: total calendar months spanned by the union of OOS windows.
    """
    oos_intervals = [(f[2], f[3]) for f in folds]
    # Merge overlapping intervals
    sorted_intervals = sorted(oos_intervals, key=lambda x: x[0])
    merged = []
    for start, end in sorted_intervals:
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))

    total_days = sum((end - start).days for start, end in merged)
    return total_days / 30.44  # Average days per month

# Gate: >= 60 months
```

---

## OOS Uplift Significance Test

For MODEL-006 gatekeeper evaluation:

```python
from scipy import stats

def oos_uplift_test(approved_returns, rejected_returns, n_bootstrap=10000, alpha=0.05):
    """
    approved_returns: per-trade returns for ML-approved signals (OOS only).
    rejected_returns: per-trade returns for ML-rejected signals (OOS only).
    Returns: (mean_uplift, p_value, is_significant)
    """
    # Bootstrap test
    observed_diff = np.mean(approved_returns) - np.mean(rejected_returns)

    pooled = np.concatenate([approved_returns, rejected_returns])
    n_a = len(approved_returns)
    bootstrap_diffs = []
    rng = np.random.RandomState(42)

    for _ in range(n_bootstrap):
        perm = rng.choice(pooled, size=len(pooled), replace=True)
        a_sample = perm[:n_a]
        r_sample = perm[n_a:]
        bootstrap_diffs.append(np.mean(a_sample) - np.mean(r_sample))

    p_value = (np.sum(np.array(bootstrap_diffs) <= 0) + 1) / (n_bootstrap + 1)
    is_significant = p_value < alpha

    return observed_diff, p_value, is_significant
```

**Gate for MODEL-006 promotion:** `is_significant == True` AND `observed_diff > 0`.

---

## Composite Ranking Formula (MODEL-005)

```python
def composite_score(strategy_cell):
    """
    Higher is better. Weights are documented.
    """
    sharpe_norm = strategy_cell["sharpe"]         # Already a ratio, range ~[0, 3]
    pf_norm = strategy_cell["profit_factor"]       # Range ~[0.5, 5]
    recovery_norm = strategy_cell["recovery_factor"] # Range ~[0, 10]
    maxdd_penalty = strategy_cell["max_drawdown"] * 1.0  # Penalize higher drawdown

    score = (0.5 * sharpe_norm +
             0.3 * pf_norm +
             0.2 * recovery_norm -
             maxdd_penalty)

    return score
```

Tie-breaking: higher `trade_count` wins. If still tied, lower `max_drawdown` wins.

---

## Confidence Intervals (For Reporting)

```python
def bootstrap_ci(metric_values, n_bootstrap=5000, ci=0.95):
    """Bootstrapped confidence interval for a metric."""
    boot_means = []
    rng = np.random.RandomState(42)
    for _ in range(n_bootstrap):
        sample = rng.choice(metric_values, size=len(metric_values), replace=True)
        boot_means.append(np.mean(sample))
    lower = np.percentile(boot_means, (1 - ci) / 2 * 100)
    upper = np.percentile(boot_means, (1 + ci) / 2 * 100)
    return lower, upper
```
