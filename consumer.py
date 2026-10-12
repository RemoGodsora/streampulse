import asyncio
import json
import math
import time
import redis.asyncio as aioredis
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from config import KAFKA_BOOTSTRAP_SERVERS, REDIS_HOST, REDIS_PORT, RAW_TOPIC, ANOMALY_TOPIC
from engine.models import TelemetryEvent, AnomalyEvent
from engine.analyzer import diagnose_anomaly

WINDOW_SECONDS = 30
Z_SCORE_THRESHOLD = 3.5
MIN_SAMPLES = 20

# Token bucket protection
ai_lock = asyncio.Lock()
GLOBAL_COOLDOWN_SECONDS = 8.0
last_diagnosis_timestamp = 0.0

async def compute_z_score(r: aioredis.Redis, service: str, metric_name: str, value: float, current_ts: float):
    key = f"window:{service}:{metric_name}"
    cutoff = current_ts - WINDOW_SECONDS
    await r.zremrangebyscore(key, 0, cutoff)

    records = await r.zrangebyscore(key, cutoff, "+inf")
    
    entry = f"{value}:{current_ts}:{time.time_ns()}"
    await r.zadd(key, {entry: current_ts})
    await r.expire(key, WINDOW_SECONDS * 2)

    if len(records) < MIN_SAMPLES:
        return 0.0, 0.0, 0.0

    values = [float(item.decode('utf-8').split(":")[0]) for item in records]
    n = len(values)
    mean = sum(values) / n
    variance = sum((v - mean) ** 2 for v in values) / (n - 1) if n > 1 else 0.0
    std_dev = math.sqrt(variance)

    if std_dev == 0.0 or value <= mean:
        return 0.0, mean, std_dev

    z_score = (value - mean) / std_dev
    return z_score, mean, std_dev

async def run_consumer():
    global last_diagnosis_timestamp
    redis_client = aioredis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
    
    consumer = AIOKafkaConsumer(
        RAW_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="streampulse-core-workers",
        auto_offset_reset="latest",
        enable_auto_commit=True
    )
    
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    await consumer.start()
    await producer.start()

    print("[CONSUMER] Connected to Redpanda & Redis")
    print(f"[CONSUMER] Monitoring '{RAW_TOPIC}' | Sliding window: {WINDOW_SECONDS}s | Z-threshold: >={Z_SCORE_THRESHOLD}σ\n")

    processed = 0

    try:
        async for msg in consumer:
            processed += 1
            raw_data = json.loads(msg.value.decode("utf-8"))
            event = TelemetryEvent(**raw_data)

            z_score, mean_lat, std_lat = await compute_z_score(
                redis_client, event.service, "latency_ms", event.latency_ms, event.timestamp
            )

            if z_score >= Z_SCORE_THRESHOLD:
                now = time.time()
                
                # Enforce serialization and cooldown across all services
                if (now - last_diagnosis_timestamp) >= GLOBAL_COOLDOWN_SECONDS:
                    if not ai_lock.locked():
                        async with ai_lock:
                            last_diagnosis_timestamp = time.time()

                            anomaly = AnomalyEvent(
                                event_id=event.event_id,
                                timestamp=event.timestamp,
                                service=event.service,
                                metric="latency_ms",
                                current_value=event.latency_ms,
                                rolling_mean=round(mean_lat, 2),
                                rolling_std=round(std_lat, 2),
                                z_score=round(z_score, 2),
                                status_code=event.status_code
                            )

                            print(f"\n🧠 [ANALYZING WITH GROQ] Anomaly on {anomaly.service} (Z: {anomaly.z_score}σ)...")
                            
                            diagnosis = await diagnose_anomaly(anomaly)
                            anomaly.diagnostic = json.dumps(diagnosis)

                            print(f"🚨 [{diagnosis.get('severity', 'ALERT')}] {diagnosis.get('root_cause')}")
                            print(f"🔧 [ACTION] {diagnosis.get('recommended_action')}\n")

                            await producer.send(ANOMALY_TOPIC, anomaly.model_dump())

            if processed % 200 == 0:
                print(f"⚡ [PROCESSED] {processed} frames analyzed.")

    except asyncio.CancelledError:
        pass
    finally:
        await consumer.stop()
        await producer.stop()
        await redis_client.aclose()
        print("[CONSUMER] Clean shutdown completed.")

if __name__ == "__main__":
    try:
        asyncio.run(run_consumer())
    except KeyboardInterrupt:
        print("\n[CONSUMER] Exiting.")