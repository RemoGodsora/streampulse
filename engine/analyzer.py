import json
import re
from groq import AsyncGroq
from config import GROQ_API_KEY, GROQ_MODEL
from engine.models import AnomalyEvent

groq_client = AsyncGroq(api_key=GROQ_API_KEY) if GROQ_API_KEY else None

SYSTEM_PROMPT = "DevOps SRE AI. Return ONLY JSON: {\"severity\":\"CRITICAL\",\"root_cause\":\"...\",\"recommended_action\":\"...\"}"

def extract_json(raw_text: str) -> dict:
    cleaned = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw_text.strip(), flags=re.MULTILINE)
    match = re.search(r"\{.*\}", cleaned, re.DOTALL)
    if match:
        return json.loads(match.group(0))
    return json.loads(cleaned)

def heuristic_diagnosis(anomaly: AnomalyEvent) -> dict:
    """Deterministic SRE fallback when LLM API rate limits or network fails."""
    if anomaly.service == "billing-gateway":
        return {
            "severity": "CRITICAL",
            "root_cause": f"Payment gateway thread-pool starvation (Latency: {anomaly.current_value}ms, Z: {anomaly.z_score}σ)",
            "recommended_action": "Enable circuit breaker, restart gateway pods, inspect upstream banking provider."
        }
    elif anomaly.service == "inference-worker":
        return {
            "severity": "CRITICAL",
            "root_cause": f"Worker memory/GPU compute saturation (Latency: {anomaly.current_value}ms, Status {anomaly.status_code})",
            "recommended_action": "Auto-scale replica count, drain queue backlog, restart worker container."
        }
    else:
        return {
            "severity": "HIGH",
            "root_cause": f"High statistical variance detected on {anomaly.service} ({anomaly.z_score}σ above baseline)",
            "recommended_action": "Check upstream network health, database connection pool, and container CPU limits."
        }

async def diagnose_anomaly(anomaly: AnomalyEvent) -> dict:
    if not groq_client:
        return heuristic_diagnosis(anomaly)

    user_prompt = f"Service:{anomaly.service}, Latency:{anomaly.current_value}ms, Baseline:{anomaly.rolling_mean}ms, Z:{anomaly.z_score}σ, HTTP:{anomaly.status_code}. JSON:"

    try:
        response = await groq_client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.1,
            max_tokens=80
        )
        content = response.choices[0].message.content or ""
        return extract_json(content)
    except Exception as e:
        # If rate limit (429) or parse error occurs, seamlessly engage the heuristic engine
        print(f"⚠️ [GROQ RATE LIMIT / FALLBACK] {str(e)[:60]} -> Using Heuristic Engine")
        return heuristic_diagnosis(anomaly)