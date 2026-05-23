import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI()

ROUTER_URL     = "http://localhost:8500/command"
CONTROLLER_URL = "http://localhost:9100"

REGION_ALIASES = {
    "us-east-1": "us-east-1",
    "us east 1": "us-east-1",
    "us east":   "us-east-1",
    "east":      "us-east-1",
    "virginia":  "us-east-1",
    "eu-west-1": "eu-west-1",
    "eu west 1": "eu-west-1",
    "eu west":   "eu-west-1",
    "eu":        "eu-west-1",
    "europe":    "eu-west-1",
    "ireland":   "eu-west-1",
}

KNOWN_REGIONS = {"us-east-1", "eu-west-1"}


class SpeakRequest(BaseModel):
    message: str


# --- Helpers ---

async def classify_intent(message: str, client: httpx.AsyncClient) -> dict:
    r = await client.post(ROUTER_URL, json={"message": message}, timeout=10.0)
    r.raise_for_status()
    return r.json()


async def get_decision(client: httpx.AsyncClient) -> dict:
    r = await client.get(f"{CONTROLLER_URL}/decision", timeout=5.0)
    r.raise_for_status()
    return r.json()


async def post_override(weights: dict, ttl_seconds: int, client: httpx.AsyncClient) -> dict:
    r = await client.post(
        f"{CONTROLLER_URL}/override",
        json={"weights": weights, "ttl_seconds": ttl_seconds},
        timeout=5.0,
    )
    r.raise_for_status()
    return r.json()


def resolve_region(entities: dict) -> str | None:
    """Extract and normalise a region name from classifier entities."""
    raw = (
        entities.get("region")
        or entities.get("region_name")
        or entities.get("target_region")
        or ""
    )
    if isinstance(raw, list):
        raw = raw[0] if raw else ""
    return REGION_ALIASES.get(str(raw).lower().strip())


def weights_for_region(region: str) -> dict:
    return {r: (100 if r == region else 0) for r in KNOWN_REGIONS}


# --- Intent handlers ---

async def handle_status_query(client: httpx.AsyncClient) -> dict:
    decision = await get_decision(client)
    return {
        "intent":      "status_query",
        "explanation": decision.get("reason", "No reason available."),
        "decision":    decision,
    }


async def handle_explain_incident(client: httpx.AsyncClient) -> dict:
    decision = await get_decision(client)
    reason   = decision.get("reason", "No incident explanation available.")
    source   = decision.get("decision_source", "unknown")
    triggers = decision.get("triggers", [])

    narrative = f"The current routing decision (source: {source}) is: {reason}"
    if triggers:
        narrative += f" This was triggered by: {'; '.join(triggers)}."

    return {
        "intent":      "explain_incident",
        "explanation": narrative,
        "decision":    decision,
    }


async def handle_force_region(entities: dict, client: httpx.AsyncClient) -> dict:
    region = resolve_region(entities)

    if region is None:
        return {
            "intent":      "force_region",
            "explanation": (
                f"Could not identify a valid region from your request. "
                f"Known regions: {sorted(KNOWN_REGIONS)}."
            ),
            "ok": False,
        }

    weights = weights_for_region(region)
    result  = await post_override(weights, ttl_seconds=300, client=client)

    return {
        "intent":      "force_region",
        "explanation": (
            f"Manual override applied: routing 100% of traffic to {region} "
            f"for 300 seconds."
        ),
        "weights":     weights,
        "expires_at":  result.get("expires_at"),
        "ok":          True,
    }


def handle_predict_failure() -> dict:
    # Simulated — no AI call yet
    return {
        "intent":      "predict_failure",
        "explanation": (
            "Failure prediction is not yet connected to a live model. "
            "In the next ARES release, this will analyse latency trends and "
            "error rate trajectories to estimate failure probability per region."
        ),
        "prediction":  "simulated",
        "ok":          True,
    }


def handle_unknown(explanation: str) -> dict:
    return {
        "intent":      "unknown",
        "explanation": "I don't understand the request.",
        "classifier_explanation": explanation,
        "ok":          False,
    }


# --- Main endpoint ---

@app.post("/speak")
async def speak(req: SpeakRequest):
    if not req.message.strip():
        raise HTTPException(status_code=400, detail="message must not be empty")

    async with httpx.AsyncClient() as client:
        try:
            classification = await classify_intent(req.message, client)
        except httpx.HTTPStatusError as e:
            raise HTTPException(
                status_code=502,
                detail=f"Router service error: {e.response.status_code} {e.response.text}",
            )
        except Exception as e:
            raise HTTPException(status_code=503, detail=f"Router unreachable: {e}")

        intent      = classification.get("intent", "unknown")
        entities    = classification.get("entities", {})
        confidence  = classification.get("confidence", 0.0)
        explanation = classification.get("explanation", "")

        try:
            if intent == "status_query":
                result = await handle_status_query(client)

            elif intent == "explain_incident":
                result = await handle_explain_incident(client)

            elif intent == "force_region":
                result = await handle_force_region(entities, client)

            elif intent == "predict_failure":
                result = handle_predict_failure()

            else:
                result = handle_unknown(explanation)

        except httpx.HTTPStatusError as e:
            raise HTTPException(
                status_code=502,
                detail=f"Controller service error: {e.response.status_code} {e.response.text}",
            )
        except Exception as e:
            raise HTTPException(status_code=503, detail=f"Controller unreachable: {e}")

    return {
        **result,
        "confidence": confidence,
        "raw_intent": intent,
    }
