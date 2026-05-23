import json
import os

import boto3
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI()

BEDROCK_REGION   = os.environ.get("BEDROCK_REGION",   "us-east-1")
BEDROCK_MODEL_ID = os.environ.get("BEDROCK_MODEL_ID", "amazon.nova-lite-v1:0")

bedrock = boto3.client("bedrock-runtime", region_name=BEDROCK_REGION)

SYSTEM_PROMPT = """You are the ARES Operator, an intent classification engine for an autonomous
multi-region cloud traffic control system.

Your only job is to classify the user's message into exactly one of these intents:

  status_query    – user wants to know current system or region health
  explain_incident – user wants an explanation of a recent failure or anomaly
  force_region    – user wants to manually direct traffic to a specific region
  predict_failure – user is asking whether a failure is likely or imminent
  unknown         – message does not match any of the above

Rules:
- confidence is a float between 0.0 and 1.0 reflecting how certain you are
- entities is an object; extract any region names, time references, or percentages mentioned
- explanation is one plain-English sentence describing what the user wants
- Respond with ONLY valid JSON. No preamble, no markdown, no text outside the JSON.

Response schema:
{
  "intent":      "<one of the five intents above>",
  "confidence":  <float 0.0–1.0>,
  "entities":    {},
  "explanation": "<string>"
}"""

VALID_INTENTS = {"status_query", "explain_incident", "force_region", "predict_failure", "unknown"}


class CommandRequest(BaseModel):
    message: str


def extract_text_from_bedrock(response: dict) -> str:
    """Concatenate all text blocks from the Bedrock converse response."""
    content = response["output"]["message"]["content"]
    return " ".join(block["text"] for block in content if "text" in block).strip()


def classify(message: str) -> dict:
    response = bedrock.converse(
        modelId=BEDROCK_MODEL_ID,
        system=[{"text": SYSTEM_PROMPT}],
        messages=[{"role": "user", "content": [{"text": message}]}],
        inferenceConfig={
            "maxTokens": 256,
            "temperature": 0.1,
        },
    )

    text = extract_text_from_bedrock(response)

    # Strip accidental markdown fences
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        print(f"[operator] JSON parse failed. Raw model response:\n{text}")
        raise

    # Validate required fields and types
    intent     = parsed.get("intent")
    confidence = parsed.get("confidence")
    entities   = parsed.get("entities")
    explanation = parsed.get("explanation")

    if intent not in VALID_INTENTS:
        raise ValueError(f"Invalid intent '{intent}'. Must be one of {VALID_INTENTS}")
    if not isinstance(confidence, (int, float)) or not (0.0 <= confidence <= 1.0):
        raise ValueError(f"confidence must be a float between 0 and 1, got {confidence!r}")
    if not isinstance(entities, dict):
        raise ValueError(f"entities must be an object, got {type(entities)}")
    if not isinstance(explanation, str):
        raise ValueError(f"explanation must be a string, got {type(explanation)}")

    return {
        "intent":      intent,
        "confidence":  round(float(confidence), 3),
        "entities":    entities,
        "explanation": explanation,
    }


@app.post("/command")
def command(req: CommandRequest):
    if not req.message.strip():
        raise HTTPException(status_code=400, detail="message must not be empty")

    try:
        result = classify(req.message)
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=502, detail=f"Model returned non-JSON: {e}")
    except ValueError as e:
        raise HTTPException(status_code=502, detail=f"Model response failed validation: {e}")
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Bedrock call failed: {e}")

    return result