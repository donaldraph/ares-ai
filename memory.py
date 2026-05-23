"""
ARES Memory Layer
-----------------
Append-only JSONL persistence for incidents and decisions.
Provides context injection for Nova prompts and outcome evaluation.
"""

import json
import os
import threading
import time
from collections import defaultdict
from typing import Callable

MEMORY_DIR      = "./memory"
INCIDENTS_FILE  = os.path.join(MEMORY_DIR, "incidents.jsonl")
DECISIONS_FILE  = os.path.join(MEMORY_DIR, "decisions.jsonl")

INCIDENT_ERROR_THRESHOLD   = 0.5
INCIDENT_LATENCY_THRESHOLD = 2000.0
OUTCOME_EVAL_DELAY         = 20   # seconds after AI decision to evaluate outcome
CONTEXT_WINDOW             = 10   # recent records to load for context

_write_lock = threading.Lock()

# Track which regions are already in an active incident to avoid duplicate writes
_active_incidents: set[str] = set()
_incidents_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Storage primitives
# ---------------------------------------------------------------------------

def _ensure_dir():
    os.makedirs(MEMORY_DIR, exist_ok=True)


def _append(filepath: str, record: dict):
    """Thread-safe append of one JSON record to a JSONL file."""
    _ensure_dir()
    line = json.dumps(record) + "\n"
    with _write_lock:
        with open(filepath, "a", encoding="utf-8") as f:
            f.write(line)


def _load_recent(filepath: str, n: int) -> list[dict]:
    """Return the last N records from a JSONL file. Returns [] if file absent."""
    if not os.path.exists(filepath):
        return []
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            lines = [l.strip() for l in f if l.strip()]
        return [json.loads(l) for l in lines[-n:]]
    except Exception as e:
        print(f"[memory] Failed to read {filepath}: {e}")
        return []


# ---------------------------------------------------------------------------
# Incident recording
# ---------------------------------------------------------------------------

def record_incident(snapshot: dict):
    """
    Inspect snapshot for incident conditions.
    Appends one record per newly-triggered region.
    Clears a region from active incidents when it recovers.
    """
    regions = snapshot.get("regions", {})

    with _incidents_lock:
        for region, m in regions.items():
            reachable   = m.get("reachable", True)
            error_rate  = m.get("error_rate")  or 0.0
            latency_ms  = m.get("latency_ms")  or 0.0

            is_incident = (
                not reachable
                or error_rate  >= INCIDENT_ERROR_THRESHOLD
                or latency_ms  >= INCIDENT_LATENCY_THRESHOLD
            )

            if is_incident and region not in _active_incidents:
                _active_incidents.add(region)
                record = {
                    "timestamp":   time.time(),
                    "region":      region,
                    "metrics": {
                        "reachable":  reachable,
                        "error_rate": error_rate,
                        "latency_ms": latency_ms,
                    },
                    "detected_by": "ares",
                }
                _append(INCIDENTS_FILE, record)
                print(f"[memory] Incident recorded: {region} "
                      f"(reachable={reachable}, error={error_rate:.2f}, "
                      f"latency={latency_ms:.0f}ms)")

            elif not is_incident and region in _active_incidents:
                # Region recovered — allow future incidents to be recorded
                _active_incidents.discard(region)
                print(f"[memory] Incident cleared: {region} recovered")


# ---------------------------------------------------------------------------
# Decision recording
# ---------------------------------------------------------------------------

def record_decision(decision: dict):
    """Append a decision to decisions.jsonl."""
    record = {
        "timestamp":          decision.get("decided_at", time.time()),
        "decision_source":    decision.get("decision_source"),
        "weights":            decision.get("weights"),
        "reason":             decision.get("reason"),
        "triggers":           decision.get("triggers", []),
        "snapshot_timestamp": decision.get("snapshot_timestamp"),
    }
    _append(DECISIONS_FILE, record)


# ---------------------------------------------------------------------------
# Context builder
# ---------------------------------------------------------------------------

def build_context() -> str:
    """
    Load last CONTEXT_WINDOW incidents and decisions.
    Return a plain-text summary to inject into the Nova prompt.
    """
    recent_incidents = _load_recent(INCIDENTS_FILE, CONTEXT_WINDOW)
    recent_decisions = _load_recent(DECISIONS_FILE, CONTEXT_WINDOW)

    lines = ["Historical context:"]

    # --- Incidents ---
    if not recent_incidents:
        lines.append("  No recent incidents on record.")
    else:
        # Count incidents per region
        counts: dict[str, int] = defaultdict(int)
        for inc in recent_incidents:
            counts[inc.get("region", "unknown")] += 1

        lines.append(f"  Recent incidents ({len(recent_incidents)} in window):")
        for region, count in sorted(counts.items()):
            last = next(
                (i for i in reversed(recent_incidents) if i.get("region") == region),
                None,
            )
            if last:
                m  = last.get("metrics", {})
                ts = last.get("timestamp", 0)
                age_s = int(time.time() - ts)
                lines.append(
                    f"    - {region}: {count} incident(s), "
                    f"last {age_s}s ago "
                    f"(error={m.get('error_rate', '?'):.2f}, "
                    f"latency={m.get('latency_ms', '?'):.0f}ms, "
                    f"reachable={m.get('reachable', '?')})"
                )

    # --- Decisions ---
    if not recent_decisions:
        lines.append("  No recent decisions on record.")
    else:
        lines.append(f"  Recent decisions ({len(recent_decisions)} in window):")

        # Detect routing oscillations: weight for a region flipping more than twice
        weight_history: dict[str, list[int]] = defaultdict(list)
        for dec in recent_decisions:
            for region, w in (dec.get("weights") or {}).items():
                weight_history[region].append(w)

        oscillating = []
        for region, weights in weight_history.items():
            flips = sum(1 for i in range(1, len(weights)) if abs(weights[i] - weights[i-1]) > 20)
            if flips >= 2:
                oscillating.append(region)

        if oscillating:
            lines.append(f"    WARNING: Routing oscillation detected for: {oscillating}")

        # Recovery patterns: decisions that followed an incident for the same region
        last_dec = recent_decisions[-1]
        age_s    = int(time.time() - last_dec.get("timestamp", time.time()))
        lines.append(
            f"    Last decision: source={last_dec.get('decision_source')}, "
            f"{age_s}s ago, "
            f"weights={last_dec.get('weights')}, "
            f"reason=\"{last_dec.get('reason')}\""
        )

        # Show outcome if recorded
        outcomes = [d for d in recent_decisions if d.get("record_type") == "outcome"]
        if outcomes:
            last_outcome = outcomes[-1]
            lines.append(
                f"    Last outcome evaluation: {last_outcome.get('outcome')} "
                f"({last_outcome.get('outcome_detail', '')})"
            )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Outcome evaluation
# ---------------------------------------------------------------------------

def evaluate_outcome_async(
    decision: dict,
    snapshot_before: dict,
    fetch_snapshot_fn: Callable[[], dict],
):
    """
    Spawn a daemon thread that waits OUTCOME_EVAL_DELAY seconds,
    fetches a fresh snapshot, compares to snapshot_before,
    and appends an outcome record to decisions.jsonl.
    """
    def _eval():
        decision_ts = decision.get("decided_at", time.time())
        time.sleep(OUTCOME_EVAL_DELAY)

        try:
            snapshot_after = fetch_snapshot_fn()
        except Exception as e:
            print(f"[memory] Outcome eval failed — could not fetch snapshot: {e}")
            return

        regions_before = snapshot_before.get("regions", {})
        regions_after  = snapshot_after.get("regions",  {})

        improvements = 0
        worsenings   = 0
        details      = []

        for region in set(regions_before) | set(regions_after):
            before = regions_before.get(region, {})
            after  = regions_after.get(region,  {})

            e_before = before.get("error_rate") or 0.0
            e_after  = after.get("error_rate")  or 0.0
            l_before = before.get("latency_ms") or 0.0
            l_after  = after.get("latency_ms")  or 0.0

            e_improved = e_after  < e_before - 0.05   # >5pp improvement
            l_improved = l_after  < l_before * 0.8    # >20% latency drop
            e_worsened = e_after  > e_before + 0.05
            l_worsened = l_after  > l_before * 1.2

            if e_improved or l_improved:
                improvements += 1
                details.append(
                    f"{region}: error {e_before:.2f}->{e_after:.2f}, "
                    f"latency {l_before:.0f}->{l_after:.0f}ms"
                )
            elif e_worsened or l_worsened:
                worsenings += 1
                details.append(
                    f"{region}: error {e_before:.2f}->{e_after:.2f}, "
                    f"latency {l_before:.0f}->{l_after:.0f}ms"
                )

        outcome = "improved" if improvements > worsenings else "worsened"

        record = {
            "record_type":    "outcome",
            "decision_ts":    decision_ts,
            "evaluated_at":   time.time(),
            "outcome":        outcome,
            "outcome_detail": "; ".join(details) if details else "no change detected",
        }
        _append(DECISIONS_FILE, record)
        print(f"[memory] Outcome for decision at {decision_ts:.0f}: "
              f"{outcome} — {record['outcome_detail']}")

    t = threading.Thread(target=_eval, daemon=True, name="outcome-eval")
    t.start()
