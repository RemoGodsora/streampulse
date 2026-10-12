import asyncio
import json
import random
import time
import uuid
import argparse
from aiokafka import AIOKafkaProducer
from config import KAFKA_BOOTSTRAP_SERVERS, RAW_TOPIC

SERVICES = ["auth-service", "billing-gateway", "vector-retriever", "inference-worker"]

def generate_telemetry_event(inject_anomaly: bool = False) -> dict:
    service = random.choice(SERVICES)
    
    # Healthy baseline metrics
    latency_ms = random.gauss(45.0, 10.0)
    cpu_percent = random.uniform(20.0, 55.0)
    error_rate = random.uniform(0.0, 0.02)
    status_code = 200 if random.random() > 0.02 else random.choice([400, 404])

    # Injected degradation / failure spike
    if inject_anomaly:
        if service == "billing-gateway":
            latency_ms = random.gauss(1850.0, 120.0)
            status_code = 504
            error_rate = 0.85
        elif service == "inference-worker":
            cpu_percent = random.uniform(94.0, 99.8)
            latency_ms = random.gauss(3200.0, 300.0)
            status_code = 503
            error_rate = 0.95

    return {
        "event_id": str(uuid.uuid4()),
        "timestamp": time.time(),
        "service": service,
        "latency_ms": max(5.0, round(latency_ms, 2)),
        "cpu_percent": round(cpu_percent, 2),
        "error_rate": round(error_rate, 4),
        "status_code": status_code,
    }

async def produce_stream(rate_per_sec: int, inject_interval: int):
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    await producer.start()
    print(f"[PRODUCER] Connected to Redpanda at {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"[PRODUCER] Target throughput: ~{rate_per_sec} events/sec")
    print(f"[PRODUCER] Injecting anomaly burst every {inject_interval}s\n")

    count = 0
    last_anomaly_time = time.time()

    try:
        while True:
            now = time.time()
            trigger_anomaly = False

            if inject_interval > 0 and (now - last_anomaly_time) >= inject_interval:
                trigger_anomaly = True
                last_anomaly_time = now
                print(f"[ALERT] >>> Injecting high-variance anomaly spike into stream!")

            # Batch dispatch
            batch_size = max(1, rate_per_sec // 10)
            for i in range(batch_size):
                # Only inject anomaly on the first item in the batch
                is_anom = trigger_anomaly and (i == 0)
                event = generate_telemetry_event(inject_anomaly=is_anom)
                await producer.send(RAW_TOPIC, event)
                count += 1

            if count % 200 == 0:
                print(f"[STREAMING] Emitted {count} telemetry frames to '{RAW_TOPIC}'")

            await asyncio.sleep(0.1)

    except asyncio.CancelledError:
        pass
    finally:
        await producer.stop()
        print("[PRODUCER] Flushed buffer and stopped cleanly.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=int, default=100, help="Events per second")
    parser.add_argument("--anomaly-interval", type=int, default=15, help="Seconds between anomaly bursts (0 for none)")
    args = parser.parse_args()

    try:
        asyncio.run(produce_stream(args.rate, args.anomaly_interval))
    except KeyboardInterrupt:
        print("\n[PRODUCER] Keyboard interrupt received.")