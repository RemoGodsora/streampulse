import asyncio
import json
from contextlib import asynccontextmanager
from typing import AsyncGenerator
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from aiokafka import AIOKafkaConsumer
import httpx
from config import KAFKA_BOOTSTRAP_SERVERS, ANOMALY_TOPIC, ALERT_WEBHOOK_URL

templates = Jinja2Templates(directory="api/templates")
incident_history = []
anomaly_queue = asyncio.Queue(maxsize=100)

async def dispatch_webhook(anomaly_data: dict):
    if not ALERT_WEBHOOK_URL:
        return
    diag = {}
    if anomaly_data.get("diagnostic"):
        try:
            diag = json.loads(anomaly_data["diagnostic"])
        except Exception:
            pass

    payload = {
        "content": (
            f"🚨 **StreamPulse Incident Alert** [{diag.get('severity', 'CRITICAL')}]\n"
            f"**Service**: `{anomaly_data.get('service')}` | **Latency**: `{anomaly_data.get('current_value')}ms` (Z: {anomaly_data.get('z_score')}σ)\n"
            f"**Root Cause**: {diag.get('root_cause', 'Unknown')}\n"
            f"**Action**: {diag.get('recommended_action', 'Check logs')}"
        )
    }
    async with httpx.AsyncClient() as client:
        try:
            await client.post(ALERT_WEBHOOK_URL, json=payload, timeout=3.0)
        except Exception as e:
            print(f"[WEBHOOK] Dispatch failed: {e}")

async def anomaly_stream_listener():
    consumer = AIOKafkaConsumer(
        ANOMALY_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="streampulse-sse-gateway",
        auto_offset_reset="latest"
    )
    await consumer.start()
    try:
        async for msg in consumer:
            data = json.loads(msg.value.decode("utf-8"))
            incident_history.insert(0, data)
            if len(incident_history) > 50:
                incident_history.pop()

            await anomaly_queue.put(data)
            asyncio.create_task(dispatch_webhook(data))
    except asyncio.CancelledError:
        pass
    finally:
        await consumer.stop()

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(anomaly_stream_listener())
    yield
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

app = FastAPI(title="StreamPulse Intelligence Gateway", lifespan=lifespan)

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={"recent": incident_history[:10]}
    )

async def event_generator() -> AsyncGenerator[str, None]:
    while True:
        data = await anomaly_queue.get()
        yield f"data: {json.dumps(data)}\n\n"

@app.get("/v1/stream/anomalies")
async def stream_anomalies():
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"}
    )