"""Eventos del sistema (ya resuelto)."""

import json
from dataclasses import asdict, dataclass
from datetime import datetime


@dataclass(frozen=True)
class Reading:
    """Una lectura de un sensor de PM2.5 (material particulado, µg/m³)."""
    event_id: str
    station_id: str
    ts: datetime          # hora del EVENTO: cuándo midió el sensor, no cuándo llegó
    pm25: float

    def to_json(self) -> bytes:
        d = asdict(self)
        d["ts"] = self.ts.isoformat()
        return json.dumps(d).encode()

    @staticmethod
    def from_json(raw: bytes) -> "Reading":
        d = json.loads(raw)
        return Reading(d["event_id"], d["station_id"], datetime.fromisoformat(d["ts"]), float(d["pm25"]))


@dataclass(frozen=True)
class WindowResult:
    station_id: str
    window_start: datetime
    window_end: datetime
    count: int
    avg_pm25: float
    max_pm25: float
