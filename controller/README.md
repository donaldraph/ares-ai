# ARES Telemetry Collector

## Run order

Terminal 1 — us-east-1 region service:
```bash
REGION_NAME=us-east-1 uvicorn main:app --port 8001
```

Terminal 2 — eu-west-1 region service:
```bash
REGION_NAME=eu-west-1 uvicorn main:app --port 8002
```

Terminal 3 — telemetry collector:
```bash
uvicorn collector:app --port 8080
```

The collector starts polling immediately. Give it ~10 seconds to
accumulate enough samples for meaningful trends.

## Endpoints

### GET /snapshot
Returns interpreted state for all regions, ready for AI consumption.

```bash
curl -s http://localhost:8080/snapshot | python3 -m json.tool
```

## Example snapshot output

```json
{
  "regions": {
    "us-east-1": {
      "status": "failing",
      "reachable": true,
      "sample_count": 12,
      "avg_latency_30s": 2487.3,
      "latest_latency_ms": 2500.0,
      "latest_error_rate": 0.75,
      "latency_trend": "stable",
      "error_trend": "stable",
      "consecutive_failures": 4,
      "last_seen": 1718123456.789
    },
    "eu-west-1": {
      "status": "healthy",
      "reachable": true,
      "sample_count": 12,
      "avg_latency_30s": 12.1,
      "latest_latency_ms": 12.0,
      "latest_error_rate": 0.0,
      "latency_trend": "stable",
      "error_trend": "stable",
      "consecutive_failures": 0,
      "last_seen": 1718123456.812
    }
  },
  "global_summary": {
    "degraded_regions": ["us-east-1"],
    "healthy_regions": ["eu-west-1"],
    "worst_region": "us-east-1",
    "total_regions": 2
  },
  "timestamp": 1718123457.001
}
```

## Adding a third region

In collector.py, add to REGION_ENDPOINTS:
```python
REGION_ENDPOINTS = {
    "us-east-1": "http://localhost:8001",
    "eu-west-1":  "http://localhost:8002",
    "ap-south-1": "http://localhost:8003",   # add this
}
```

Then run a third region service:
```bash
REGION_NAME=ap-south-1 uvicorn main:app --port 8003
```
