"""Simulador de sensores de calidad del aire en el Valle de Aburrá (ya resuelto).

Imita los problemas reales del streaming:
  * desorden: los eventos llegan con unos segundos de diferencia de orden
  * tardíos: ~1% llega MUY tarde (la red del sensor se cayó un rato)
  * duplicados: ~1% se reenvía (el sensor no recibió confirmación)
"""

import random
import uuid
from datetime import datetime, timedelta

from streaming.events import Reading

STATIONS = {
    "MED-ITA": 28.0,   # Itagüí
    "MED-BEL": 22.0,   # Belén
    "MED-AGU": 18.0,   # Aguacatal
    "BEL-FEV": 25.0,   # Bello
    "ENV-HOS": 15.0,   # Envigado
}


def simulate(n: int, start: datetime, seed: int = 0, jitter_s: int = 8,
             late_prob: float = 0.01, dup_prob: float = 0.01):
    """Genera n lecturas en ORDEN DE LLEGADA (no en orden de evento)."""
    rng = random.Random(seed)
    events = []
    for i in range(n):
        station = rng.choice(list(STATIONS))
        ts = start + timedelta(seconds=i * 2)
        pm = max(0.0, rng.gauss(STATIONS[station], 6) + (25 if 300 < i < 420 and station == "MED-ITA" else 0))
        r = Reading(str(uuid.UUID(int=rng.getrandbits(128))), station, ts, round(pm, 1))
        arrival = ts + timedelta(seconds=rng.uniform(0, jitter_s))
        if rng.random() < late_prob:
            arrival += timedelta(minutes=rng.uniform(3, 10))
        events.append((arrival, r))
        if rng.random() < dup_prob:
            events.append((arrival + timedelta(seconds=rng.uniform(1, 5)), r))
    events.sort(key=lambda e: e[0])
    return [r for _, r in events]
