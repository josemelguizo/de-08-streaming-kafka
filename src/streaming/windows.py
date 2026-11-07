"""Ventanas de tiempo fijas (tumbling) con marca de agua.

Conceptos:
  * Tiempo de EVENTO: cuándo midió el sensor (Reading.ts). Es el que usamos.
    El tiempo de llegada puede ir desordenado.
  * Ventana tumbling de 60 s: [07:00:00, 07:01:00), [07:01:00, 07:02:00)...
    Cada evento cae en exactamente una.
  * Marca de agua (watermark) = mayor ts visto - allowed_lateness.
    Es la afirmación "ya no espero eventos anteriores a esta hora".
    Una ventana se CIERRA (se emite) cuando su final <= watermark.
  * Evento tardío: llega cuando su ventana ya se cerró. Se descarta y se cuenta.
"""

from collections import defaultdict
from datetime import datetime, timedelta

from streaming.events import Reading, WindowResult


class TumblingWindowAggregator:
    def __init__(self, window_seconds: int = 60, allowed_lateness_seconds: int = 30):
        self.size = timedelta(seconds=window_seconds)
        self.lateness = timedelta(seconds=allowed_lateness_seconds)
        self.max_event_ts: datetime | None = None
        self.open: dict[tuple[str, datetime], list[Reading]] = defaultdict(list)
        self.late_events = 0
        self.duplicates = 0

    def window_start(self, ts: datetime) -> datetime:
        """Inicio de la ventana donde cae ts. Con ventanas de 60 s,
        07:04:37 -> 07:04:00.

        TODO. Pista: cuenta los segundos desde datetime(1970, 1, 1) y redondea
        hacia abajo al múltiplo del tamaño de la ventana.
        """
        raise NotImplementedError("TODO: window_start")

    @property
    def watermark(self) -> datetime | None:
        """TODO: None si no ha llegado nada; si no, max_event_ts - lateness."""
        raise NotImplementedError("TODO: watermark")

    def add(self, r: Reading) -> list[WindowResult]:
        """Procesa un evento y devuelve las ventanas que se cerraron por él
        (casi siempre una lista vacía).

        TODO, en este orden:
          1. Si r.event_id ya está en una ventana abierta -> duplicado:
             suma 1 a self.duplicates y devuelve [].
          2. Si su ventana ya cerró (fin <= watermark) -> tardío:
             suma 1 a self.late_events y devuelve [].
          3. Guárdalo en self.open[(station_id, inicio_de_ventana)].
          4. Actualiza max_event_ts si r.ts es mayor.
          5. Emite y devuelve las ventanas cuyo fin <= watermark.

        Al emitir una ventana, calcula count, avg_pm25 (2 decimales) y max_pm25,
        y sácala de self.open. Devuelve las ventanas ordenadas por
        (station_id, window_start).
        """
        raise NotImplementedError("TODO: add")

    def flush(self) -> list[WindowResult]:
        """Emite TODAS las ventanas abiertas (al apagar el consumidor).

        TODO
        """
        raise NotImplementedError("TODO: flush")
