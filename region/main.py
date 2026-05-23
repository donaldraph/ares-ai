import os
import time
import threading
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

REGION_NAME   = os.getenv("REGION_NAME", "us-east-1")
# --- State ---
state = {
    "base_latency_ms": 12.0,      # healthy baseline
    "latency_ms": 12.0,
    "error_rate": 0.0,
    "recovering": False,
}
state_lock = threading.Lock()



# --- Background decay loop ---
def decay_loop():
    """Gradually move latency and error_rate back toward baseline."""
    while True:
        time.sleep(2)
        with state_lock:
            if not state["recovering"]:
                continue

            target_latency = state["base_latency_ms"]
            target_error   = 0.0

            state["latency_ms"]  += (target_latency - state["latency_ms"])  * 0.15
            state["error_rate"]  += (target_error   - state["error_rate"])  * 0.15

            # Snap to zero when close enough
            if abs(state["latency_ms"] - target_latency) < 1.0:
                state["latency_ms"] = target_latency
            if state["error_rate"] < 0.005:
                state["error_rate"] = 0.0

            # Stop recovering once both are at baseline
            if (state["latency_ms"] == target_latency and
                    state["error_rate"] == 0.0):
                state["recovering"] = False


threading.Thread(target=decay_loop, daemon=True).start()


# --- Helpers ---
def current_status() -> str:
    er = state["error_rate"]
    lat = state["latency_ms"]
    if er >= 0.5 or lat >= 2000:
        return "failing"
    if er >= 0.15 or lat >= 300:
        return "degraded"
    return "healthy"


# --- Routes ---
@app.get("/health")
def health():
    with state_lock:
        return {
            "region":     REGION_NAME,
            "latency_ms": round(state["latency_ms"], 1),
            "error_rate": round(state["error_rate"], 3),
            "status":     current_status(),
            "timestamp":  time.time(),
        }


class ChaosLatencyRequest(BaseModel):
    latency_ms: float = 1500.0   # how bad to make it

class ChaosErrorRequest(BaseModel):
    error_rate: float = 0.75     # 0.0–1.0


@app.post("/chaos/latency")
def chaos_latency(req: ChaosLatencyRequest):
    with state_lock:
        state["latency_ms"] = req.latency_ms
        state["recovering"] = False   # stop any ongoing recovery
    return {"ok": True, "latency_ms": req.latency_ms, "region": REGION_NAME}


@app.post("/chaos/errors")
def chaos_errors(req: ChaosErrorRequest):
    with state_lock:
        state["error_rate"] = req.error_rate
        state["recovering"] = False
    return {"ok": True, "error_rate": req.error_rate, "region": REGION_NAME}


@app.post("/recover")
def recover():
    with state_lock:
        state["recovering"] = True
    return {"ok": True, "message": f"{REGION_NAME} recovery initiated (gradual)"}