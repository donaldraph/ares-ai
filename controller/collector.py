import time
import threading
from collections import deque

import httpx
from fastapi import FastAPI

app = FastAPI()

# --- Config ---
REGION_ENDPOINTS = {
    "us-east-1": "http://localhost:8001",
    "eu-west-1":  "http://localhost:8002",
}
POLL_INTERVAL    = 5   # seconds
SAMPLE_WINDOW    = 20  # internal buffer size
SNAPSHOT_SAMPLES = 10  # samples exposed per region

# --- In-memory store ---
samples: dict[str, deque] = {
    region: deque(maxlen=SAMPLE_WINDOW)
    for region in REGION_ENDPOINTS
}
store_lock = threading.Lock()


# --- Polling ---
def poll_region(base_url: str) -> dict:
    try:
        r = httpx.get(f"{base_url}/health", timeout=4.0)
        r.raise_for_status()
        data = r.json()
        return {
            "reachable":  True,
            "latency_ms": data["latency_ms"],
            "error_rate": data["error_rate"],
            "timestamp":  data["timestamp"],
        }
    except Exception:
        return {
            "reachable":  False,
            "latency_ms": None,
            "error_rate": None,
            "timestamp":  time.time(),
        }


def poll_loop():
    while True:
        for region, url in REGION_ENDPOINTS.items():
            sample = poll_region(url)
            with store_lock:
                samples[region].append(sample)
        time.sleep(POLL_INTERVAL)


threading.Thread(target=poll_loop, daemon=True).start()


# --- Snapshot endpoint ---
@app.get("/snapshot")
def snapshot():
    with store_lock:
        regions = {}
        for region, buf in samples.items():
            snap = list(buf)[-SNAPSHOT_SAMPLES:]
            latest = snap[-1] if snap else {}
            regions[region] = {
                "reachable":  latest.get("reachable"),
                "latency_ms": latest.get("latency_ms"),
                "error_rate": latest.get("error_rate"),
                "samples":    snap,
            }

    return {
        "regions":   regions,
        "timestamp": time.time(),
    }