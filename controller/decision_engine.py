import json
import os
import time
import threading
from contextlib import asynccontextmanager

import boto3
import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

import memory

TELEMETRY_URL      = "http://localhost:9000/snapshot"
POLL_INTERVAL      = 5   # seconds between telemetry polls
AI_COOLDOWN        = 45  # minimum seconds between Nova calls
BACKOFF_BASE       = 2   # exponential backoff base (seconds)
BACKOFF_MAX        = 120 # cap on backoff wait

# Read from environment — no credentials in code
BEDROCK_REGION   = os.environ.get("BEDROCK_REGION", "us-east-1")
BEDROCK_MODEL_ID = os.environ.get("BEDROCK_MODEL_ID", "amazon.nova-lite-v1:0")

bedrock         = boto3.client("bedrock-runtime", region_name=BEDROCK_REGION)
bedrock_control = boto3.client("bedrock",         region_name=BEDROCK_REGION)


# --- Startup check ---
def check_model_availability():
    print(f"\n[startup] Querying available models in region: {BEDROCK_REGION}")

    response    = bedrock_control.list_foundation_models()
    all_models  = [m["modelId"] for m in response["modelSummaries"]]
    nova_models = [m for m in all_models if "nova" in m.lower()]

    print(f"[startup] Nova models available in {BEDROCK_REGION}:")
    for m in nova_models:
        marker = " (configured)" if m == BEDROCK_MODEL_ID else ""
        print(f"          {m}{marker}")

    if BEDROCK_MODEL_ID not in all_models:
        raise RuntimeError(
            f"\n[startup] BEDROCK_MODEL_ID '{BEDROCK_MODEL_ID}' is not available "
            f"in region '{BEDROCK_REGION}'.\n"
            f"          Available Nova models: {nova_models}\n"
            f"          Set BEDROCK_MODEL_ID to one of the above and restart."
        )

    print(f"[startup] Model '{BEDROCK_MODEL_ID}' confirmed available. Starting.\n")


@asynccontextmanager
async def lifespan(app: FastAPI):
    check_model_availability()
    t = threading.Thread(target=decision_loop, daemon=True, name="decision-loop")
    t.start()
    print(f"[startup] Decision loop started (thread id={t.ident})")
    yield


app = FastAPI(lifespan=lifespan)

# --- In-memory state ---
latest_decision: dict | None = None
decision_lock = threading.Lock()

# Manual override — None when inactive
# Shape: { "weights": {...}, "reason": str, "expires_at": float }
override: dict | None = None
override_lock = threading.Lock()

# Tracks the last snapshot we used to detect changes
last_observed: dict = {}   # { region -> { reachable, latency_ms, error_rate } }
last_worst_region: str | None = None
last_ai_call_at: float = 0.0
throttle_backoff: float = 0.0   # extra wait on top of cooldown after throttle
throttle_fail_count: int = 0


# --- Override helpers ---
def active_override() -> dict | None:
    """Return the override dict if it exists and has not expired, else None."""
    with override_lock:
        if override is None:
            return None
        if time.time() > override["expires_at"]:
            return None
        return override


# --- Prompt ---
SYSTEM_PROMPT = """You are ARES, an autonomous traffic routing engine.
You receive real-time telemetry from cloud region services and decide how to distribute user traffic.

Rules:
- Weights must be integers that sum to exactly 100
- Prefer healthy regions with low latency and low error rates
- A region with error_rate above 0.5 or latency_ms above 2000 should receive minimal traffic (0-10)
- A region that is unreachable (reachable: false) must receive 0 weight
- Reason must be one or two sentences max, plain English, no jargon

You must respond with ONLY valid JSON. No preamble, no markdown, no explanation outside the JSON.

Response schema:
{"weights": {"us-east-1": <integer>, "eu-west-1": <integer>}, "reason": "<string>"}"""


# --- Change detection ---
def detect_changes(snapshot: dict) -> list[str]:
    """
    Compare current snapshot against last observed state.
    Returns a list of human-readable reasons a Nova call is warranted.
    Empty list means nothing meaningful changed.
    """
    global last_observed, last_worst_region

    regions  = snapshot.get("regions", {})
    triggers = []

    # First observation — store state and trigger immediately
    if not last_observed:
        for region, metrics in regions.items():
            last_observed[region] = {
                "reachable":  metrics.get("reachable"),
                "latency_ms": metrics.get("latency_ms"),
                "error_rate": metrics.get("error_rate"),
            }
        last_worst_region = max(regions.items(), key=lambda item: (
            item[1].get("reachable") is False,
            item[1].get("error_rate") or 0.0,
            item[1].get("latency_ms") or 0.0,
        ))[0] if regions else None
        return ["initial observation"]

    # Derive worst_region from raw data: most unreachable first, then highest error_rate
    def region_badness(item):
        _, m = item
        reachable  = m.get("reachable") is False
        error_rate = m.get("error_rate") or 0.0
        latency    = m.get("latency_ms") or 0.0
        return (reachable, error_rate, latency)

    current_worst = max(regions.items(), key=region_badness)[0] if regions else None

    for region, metrics in regions.items():
        prev = last_observed.get(region, {})

        curr_reachable  = metrics.get("reachable")
        curr_latency    = metrics.get("latency_ms")
        curr_error_rate = metrics.get("error_rate")

        prev_reachable  = prev.get("reachable")
        prev_latency    = prev.get("latency_ms")
        prev_error_rate = prev.get("error_rate")

        # 1. Reachability flipped
        if prev_reachable is not None and curr_reachable != prev_reachable:
            direction = "became reachable" if curr_reachable else "became unreachable"
            triggers.append(f"{region} {direction}")

        # 2. Latency changed by more than 40%
        if curr_latency is not None and prev_latency is not None and prev_latency > 0:
            pct_change = abs(curr_latency - prev_latency) / prev_latency
            if pct_change > 0.40:
                triggers.append(
                    f"{region} latency shifted {pct_change:.0%} "
                    f"({prev_latency:.0f}ms -> {curr_latency:.0f}ms)"
                )

        # 3. error_rate crosses 0.2 threshold in either direction
        if curr_error_rate is not None and prev_error_rate is not None:
            crossed_up   = prev_error_rate <  0.2 <= curr_error_rate
            crossed_down = prev_error_rate >= 0.2 >  curr_error_rate
            if crossed_up:
                triggers.append(f"{region} error_rate crossed above 0.2 ({curr_error_rate:.2f})")
            elif crossed_down:
                triggers.append(f"{region} error_rate recovered below 0.2 ({curr_error_rate:.2f})")

        # 4. Recovery: was unreachable or high-error, now healthy
        was_bad = (prev_reachable is False) or (prev_error_rate is not None and prev_error_rate >= 0.5)
        is_good = curr_reachable is True and (curr_error_rate is not None and curr_error_rate < 0.2)
        if was_bad and is_good:
            triggers.append(f"{region} recovered to healthy state")

    # 5. Worst region changed
    if last_worst_region is not None and current_worst != last_worst_region:
        triggers.append(
            f"worst region shifted from {last_worst_region} to {current_worst}"
        )

    # Update observed state
    for region, metrics in regions.items():
        last_observed[region] = {
            "reachable":  metrics.get("reachable"),
            "latency_ms": metrics.get("latency_ms"),
            "error_rate": metrics.get("error_rate"),
        }
    last_worst_region = current_worst

    return triggers


# --- Bedrock call ---
def call_nova(snapshot: dict) -> dict:
    historical_context = memory.build_context()
    user_message = (
        "Here is the current telemetry snapshot from all regions. "
        "Decide how to route traffic.\n\n"
        + json.dumps(snapshot, indent=2)
        + "\n\n"
        + historical_context
    )

    response = bedrock.converse(
        modelId=BEDROCK_MODEL_ID,
        system=[{"text": SYSTEM_PROMPT}],
        messages=[
            {"role": "user", "content": [{"text": user_message}]}
        ],
        inferenceConfig={
            "maxTokens": 256,
            "temperature": 0.1,
        },
    )

    text = response["output"]["message"]["content"][0]["text"].strip()

    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]

    parsed  = json.loads(text)
    weights = parsed["weights"]
    reason  = parsed["reason"]

    assert isinstance(reason, str), "reason must be a string"
    assert set(weights.keys()) == {"us-east-1", "eu-west-1"}, "unexpected weight keys"
    assert sum(weights.values()) == 100, f"weights must sum to 100, got {sum(weights.values())}"

    return {"weights": weights, "reason": reason}


def get_ai_decision(snapshot: dict) -> dict:
    return call_nova(snapshot)


# --- Heuristic fallback ---
def heuristic_decision(snapshot: dict) -> dict:
    """
    Deterministic routing policy derived purely from telemetry.
    Used when AI is throttled or unavailable.

    Scoring per region (higher = more traffic):
      - Unreachable         -> score 0, excluded entirely
      - error_rate          -> penalised quadratically
      - latency_ms          -> penalised relative to a 1000 ms ceiling

    Scores are normalised to integers summing to exactly 100.
    If every region scores 0, the least-bad region receives 100
    so routing never collapses entirely.
    """
    regions = snapshot.get("regions", {})
    LATENCY_CEIL = 1000.0   # ms — above this, latency stops adding extra penalty

    raw_scores: dict[str, float] = {}
    for region, m in regions.items():
        if not m.get("reachable", False):
            raw_scores[region] = 0.0
            continue

        error_rate = m.get("error_rate") or 0.0
        latency    = m.get("latency_ms")  or 0.0

        # Quadratic error penalty so 80% errors hurts far more than 20%
        error_penalty   = error_rate ** 2
        # Latency penalty capped at 1.0 so a very slow-but-alive region still competes
        latency_penalty = min(latency, LATENCY_CEIL) / LATENCY_CEIL

        score = max(0.0, 1.0 - error_penalty - 0.3 * latency_penalty)
        raw_scores[region] = score

    total = sum(raw_scores.values())

    if total == 0:
        # All regions unhealthy — emergency route to least-bad one
        least_bad = min(
            regions.items(),
            key=lambda item: (
                not item[1].get("reachable", False),
                item[1].get("error_rate")  or 1.0,
                item[1].get("latency_ms")  or float("inf"),
            ),
        )[0]
        weights = {r: (100 if r == least_bad else 0) for r in regions}
        reason  = (
            f"All regions degraded — emergency routing 100% to least-bad "
            f"region ({least_bad})."
        )
    else:
        # Normalise to integers summing to exactly 100
        floats    = {r: (s / total) * 100 for r, s in raw_scores.items()}
        weights   = {r: int(v) for r, v in floats.items()}
        remainder = 100 - sum(weights.values())
        if remainder:
            best = max(raw_scores, key=raw_scores.__getitem__)
            weights[best] += remainder

        penalised = [r for r, s in raw_scores.items() if s == 0]
        parts     = [f"{r}={weights[r]}%" for r in sorted(weights)]
        reason    = (
            f"Heuristic routing: {', '.join(parts)}."
            + (f" Zero-weighted (unreachable): {penalised}." if penalised else "")
        )

    print(f"[heuristic] {reason}")
    return {"weights": weights, "reason": reason}


# --- Control loop ---
def fetch_snapshot() -> dict:
    r = httpx.get(TELEMETRY_URL, timeout=5.0)
    r.raise_for_status()
    return r.json()


def decision_loop():
    global last_ai_call_at, throttle_backoff, throttle_fail_count

    while True:
        try:
            # Skip AI/heuristic entirely while a manual override is active
            if active_override() is not None:
                ov = active_override()
                ttl_left = round(ov["expires_at"] - time.time())
                print(f"[loop] Manual override active — skipping AI ({ttl_left}s remaining)")
                time.sleep(POLL_INTERVAL)
                continue

            snapshot = fetch_snapshot()
            memory.record_incident(snapshot)
            triggers = detect_changes(snapshot)

            if not triggers:
                print("[loop] No meaningful change detected — skipping AI call")
                time.sleep(POLL_INTERVAL)
                continue

            # Cooldown gate
            now                = time.time()
            effective_cooldown = AI_COOLDOWN + throttle_backoff
            time_since_last    = now - last_ai_call_at

            if time_since_last < effective_cooldown:
                wait_remaining = effective_cooldown - time_since_last
                print(
                    f"[loop] Change detected ({'; '.join(triggers)}) "
                    f"but cooldown active — {wait_remaining:.0f}s remaining. "
                    f"Applying heuristic fallback."
                )
                fallback = heuristic_decision(snapshot)
                _commit_decision(fallback, triggers, snapshot, source="fallback")
                time.sleep(POLL_INTERVAL)
                continue

            print(f"[loop] Triggering AI reasoning — reasons: {'; '.join(triggers)}")

            try:
                ai_result = get_ai_decision(snapshot)
                _commit_decision(ai_result, triggers, snapshot, source="ai")
                last_ai_call_at    = time.time()
                throttle_backoff   = 0.0
                throttle_fail_count = 0
                print(f"[loop] AI decision: {ai_result['weights']} | {ai_result['reason']}")
                with decision_lock:
                    committed = latest_decision.copy()
                memory.evaluate_outcome_async(committed, snapshot, fetch_snapshot)

            except Exception as e:
                error_str = str(e).lower()
                if "throttling" in error_str or "toomanyrequests" in error_str:
                    throttle_fail_count += 1
                    throttle_backoff = min(BACKOFF_BASE ** throttle_fail_count, BACKOFF_MAX)
                    print(
                        f"[loop] Throttled by Bedrock (attempt {throttle_fail_count}) — "
                        f"backoff {throttle_backoff:.0f}s. Applying heuristic fallback."
                    )
                else:
                    print(f"[loop] AI call failed: {e} — applying heuristic fallback.")

                fallback = heuristic_decision(snapshot)
                _commit_decision(fallback, triggers, snapshot, source="fallback")

        except Exception as e:
            print(f"[loop] Telemetry fetch failed: {e}")

        time.sleep(POLL_INTERVAL)


def _commit_decision(result: dict, triggers: list[str], snapshot: dict, source: str):
    """Write a completed decision (AI or heuristic) into shared state."""
    decision = {
        "weights":            result["weights"],
        "reason":             result["reason"],
        "decision_source":    source,
        "triggers":           triggers,
        "snapshot_timestamp": snapshot.get("timestamp"),
        "decided_at":         time.time(),
    }
    with decision_lock:
        global latest_decision
        latest_decision = decision
    memory.record_decision(decision)


# Thread started in lifespan — do not start here


# --- Request models ---
class OverrideRequest(BaseModel):
    weights: dict[str, int]
    ttl_seconds: int


# --- Endpoints ---
@app.post("/override")
def set_override(req: OverrideRequest):
    global override

    # Validate weights sum to 100
    total = sum(req.weights.values())
    if total != 100:
        raise HTTPException(
            status_code=422,
            detail=f"weights must sum to 100, got {total}"
        )
    if req.ttl_seconds <= 0:
        raise HTTPException(
            status_code=422,
            detail="ttl_seconds must be a positive integer"
        )

    created_at = time.time()
    expires_at = created_at + req.ttl_seconds

    with override_lock:
        override = {
            "weights":    req.weights,
            "reason":     "manual override",
            "created_at": created_at,
            "expires_at": expires_at,
        }

    # Immediately surface the override as the latest decision
    with decision_lock:
        global latest_decision
        latest_decision = {
            "weights":            req.weights,
            "reason":             "manual override",
            "decision_source":    "manual_override",
            "triggers":           ["manual override set"],
            "snapshot_timestamp": None,
            "decided_at":         created_at,
            "expires_at":         expires_at,
        }

    print(
        f"[override] Manual override set: {req.weights} "
        f"for {req.ttl_seconds}s (expires at {expires_at:.0f})"
    )
    return {
        "ok":         True,
        "weights":    req.weights,
        "expires_at": expires_at,
        "ttl_seconds": req.ttl_seconds,
    }


@app.get("/weights")
def weights():
    # Override takes priority over latest_decision
    ov = active_override()
    if ov is not None:
        return {
            "weights":         ov["weights"],
            "decision_source": "manual_override",
            "decided_at":      ov["created_at"],
            "expires_at":      ov["expires_at"],
        }

    with decision_lock:
        if latest_decision is None:
            raise HTTPException(status_code=503, detail="No decision made yet")
        return {
            "weights":         latest_decision["weights"],
            "decision_source": latest_decision["decision_source"],
            "decided_at":      latest_decision["decided_at"],
        }


@app.get("/decision")
def decision():
    # Override takes priority over latest_decision
    ov = active_override()
    if ov is not None:
        return {
            "weights":            ov["weights"],
            "reason":             ov["reason"],
            "decision_source":    "manual_override",
            "triggers":           ["manual override active"],
            "snapshot_timestamp": None,
            "decided_at":         ov["created_at"],
            "expires_at":         ov["expires_at"],
        }

    with decision_lock:
        if latest_decision is None:
            raise HTTPException(status_code=503, detail="No decision made yet")
        return latest_decision