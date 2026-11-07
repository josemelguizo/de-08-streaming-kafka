"""Alertas de calidad del aire según PM2.5 promedio de la ventana."""

from dataclasses import dataclass
from datetime import datetime

from streaming.events import WindowResult

# Límite superior (µg/m³) de cada categoría, de menor a mayor. Basado en los
# rangos del Índice de Calidad del Aire usados en Colombia.
LEVELS = [
    (12.0, "buena"),
    (37.0, "aceptable"),
    (55.0, "dañina para grupos sensibles"),
    (150.0, "dañina"),
    (250.0, "muy dañina"),
    (float("inf"), "peligrosa"),
]


@dataclass(frozen=True)
class Alert:
    station_id: str
    window_start: datetime
    avg_pm25: float
    level: str


def classify(pm25: float) -> str:
    """Nombre de la categoría: la primera cuyo límite es >= pm25.
    Ej.: 12 -> "buena", 12.1 -> "aceptable".

    TODO
    """
    raise NotImplementedError("TODO: classify")


def detect_alerts(results: list[WindowResult],
                  min_level: str = "dañina para grupos sensibles") -> list[Alert]:
    """Una Alert por cada ventana cuya categoría sea min_level o peor,
    en el mismo orden de `results`.

    TODO
    """
    raise NotImplementedError("TODO: detect_alerts")
