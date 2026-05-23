# ARES Region Service

## Setup

```bash
pip install -r requirements.txt
```

## Run two regions

Terminal 1 — us-east-1:
```bash
REGION_NAME=us-east-1 uvicorn main:app --port 8001
```

Terminal 2 — eu-west-1:
```bash
REGION_NAME=eu-west-1 uvicorn main:app --port 8002
```

## Endpoints

### GET /health
Returns current region health snapshot.

### POST /chaos/latency
Instantly spikes latency. Optional body:
```json
{ "latency_ms": 2000.0 }
```

### POST /chaos/errors
Instantly spikes error rate. Optional body:
```json
{ "error_rate": 0.8 }
```

### POST /recover
Begins gradual recovery toward baseline. No body needed.
Recovery uses exponential decay — takes ~20–30 seconds to fully heal.

## Quick demo sequence

```bash
# Inject latency fault into us-east-1
curl -X POST http://localhost:8001/chaos/latency \
     -H "Content-Type: application/json" \
     -d '{"latency_ms": 2500}'

# Inject error fault into us-east-1
curl -X POST http://localhost:8001/chaos/errors \
     -H "Content-Type: application/json" \
     -d '{"error_rate": 0.75}'

# Watch it degrade
curl http://localhost:8001/health

# Start recovery
curl -X POST http://localhost:8001/recover

# Watch it heal over ~30 seconds
watch -n 2 "curl -s http://localhost:8001/health | python3 -m json.tool"
```
