from pydantic import BaseModel, Field
from typing import Optional

class TelemetryEvent(BaseModel):
    event_id: str
    timestamp: float
    service: str
    latency_ms: float
    cpu_percent: float
    error_rate: float
    status_code: int

class RollingMetrics(BaseModel):
    service: str
    count: int
    mean_latency: float
    std_latency: float
    z_score: float

class AnomalyEvent(BaseModel):
    event_id: str
    timestamp: float
    service: str
    metric: str
    current_value: float
    rolling_mean: float
    rolling_std: float
    z_score: float
    status_code: int
    diagnostic: Optional[str] = None