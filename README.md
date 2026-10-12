# StreamPulse ⚡
> High-Throughput Event-Driven Real-Time Intelligence Stream

StreamPulse is a high-performance streaming analytics and automated site-reliability triage platform. It ingests thousands of telemetry events per second, maintains rolling statistical baselines in Redis, gates anomalies using a $Z \ge 3.5\sigma$ threshold filter, and runs low-latency semantic attribution with automated runbook recommendations.

## Architecture

1. **Broker**: Redpanda (C++ Kafka-compatible streaming engine)
2. **State & Windowing**: Redis Sorted Sets (`ZSET`) maintaining 30-second rolling windows with atomic pruning
3. **Statistical Filter**: Real-time variance detection ($Z = \frac{x - \mu}{\sigma}$); only breaches ($Z \ge 3.5\sigma$) invoke diagnostic attribution
4. **Diagnostic Layer**: Groq Low-Latency Inference + SRE Heuristic Engine fallback with strict concurrency and rate-limit guardrails
5. **Observability**: FastAPI Server-Sent Events (SSE) live-streaming incident feed with sub-second propagation

## Quickstart

### 1. Boot Infrastructure
```bash
docker compose up -d
```
Access Redpanda Console: `http://localhost:8080`

### 2. Install Dependencies
```bash
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 3. Run Pipeline Services
Open three separate terminals in the project root:

* **Producer**:
  ```bash
  python producer.py --rate 100 --anomaly-interval 20
  ```
* **Consumer**:
  ```bash
  python consumer.py
  ```
* **Dashboard Gateway**:
  ```bash
  uvicorn api.main:app --reload --port 8001
  ```

### 4. Live Visualizer
Navigate to `http://localhost:8001` to monitor real-time ingested events and automated SRE triage cards.