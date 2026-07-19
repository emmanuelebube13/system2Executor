# SYSTEM BOUNDARY — What System 2 ("The Hand") does NOT do

> Standing artifact. **Every agent re-reads this before starting any task.**
> System 2 is the **Hand**, not the **Guardian**. Risk *policy and decisioning* live in
> System 3 (AMS, "The Guardian"). System 2 owns *actuation, sanity, and its own runtime*.

## The line

System 2 turns **already-sized, risk-approved orders** into real OANDA fills and manages open
positions to close. It is the **only** component that physically touches the broker. It must stay
safe and operable even when System 1, System 3, the queue, and GCS are all unreachable.

## DO NOT re-implement (System 3 / AMS owns these)

| Capability | Owner (System 3 task) | System 2's ONLY relationship |
|---|---|---|
| Position **sizing** (Quarter-Kelly + multipliers) | Risk Engine (task 05) | Receives already-sized orders; **never resizes**. |
| **Portfolio circuit breakers** (daily-loss, max-exposure, consecutive-error, drawdown) | Circuit Breaker System (task 06) | Receives a halt/flatten **command**; executes it via §7 mechanism. Mirrors breaker *state* read-only in telemetry. |
| **Account State Machine** (demo→live graduation, equity tiers) | tasks 04, 12 | Reads state if published; **never authors** it. |
| **Notification Service** (trader-facing alerts, channels, routing) | Notification Service (task 11) | **Emits** machine events to `AMS_Inbound_Queue`; System 3 notifies humans. |
| **Human-override controls** (policy, approvals, kill authority) | task 14 | Honors override **commands**; provides local actuator + manual local fallback only. |
| Signal scoring / Decision Gate A–J | tasks 03, 08 | None — upstream of the order. |

## What System 2 DOES own (defense-in-depth + actuation, NOT a copy of the above)

- The **mechanism** to stop/flatten locally (only it touches OANDA) — EXEC-010 §7.1.
- **Last-line-of-defense sanity validation** of orders (fat-finger guard, *not* sizing) — EXEC-004 §7.2.
- **Broker reconciliation** (OANDA is source of truth) — EXEC-006/010 §8.
- **Its own lifecycle / graceful shutdown** — EXEC-010 §8.1–8.2.
- **Its own structured logs / health / heartbeat** (emit events; do not route notifications) — §9.
- **Its own local datastore + migrations** (Computer 2, per logged connectivity decision).
- Model self-sufficiency: **pull + verify** artifacts from GCS; compute the **live regime** locally.

## Drift check

If a task seems to need sizing, a portfolio-level risk rule, an account-state transition, a human
alert channel, or override *policy* — **it is wrong. STOP** and route it to the queue contract
(`AMS_Outbound_Queue` in / `AMS_Inbound_Queue` out) instead of building it here.
