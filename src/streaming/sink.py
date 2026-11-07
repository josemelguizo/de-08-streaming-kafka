"""Destino de los resultados: una tabla en DuckDB."""

import duckdb

from streaming.events import WindowResult


class DuckDBSink:
    def __init__(self, path: str = ":memory:"):
        self.con = duckdb.connect(path)
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS pm25_por_minuto (
                station_id     VARCHAR,
                window_start   TIMESTAMP,
                window_end     TIMESTAMP,
                lecturas       INTEGER,
                pm25_promedio  DOUBLE,
                pm25_max       DOUBLE,
                PRIMARY KEY (station_id, window_start)
            )
        """)

    def write(self, results: list[WindowResult]) -> int:
        """Guarda las ventanas y devuelve cuántas escribió.

        TODO: tiene que ser IDEMPOTENTE. Escribir la misma ventana dos veces
        (porque el consumidor se reinició y reprocesó) deja una sola fila; si
        la ventana trae valores nuevos, los actualiza.
        Pista: INSERT ... ON CONFLICT (station_id, window_start) DO UPDATE SET ...
        """
        raise NotImplementedError("TODO: write")

    def count(self) -> int:
        return self.con.execute("SELECT COUNT(*) FROM pm25_por_minuto").fetchone()[0]
