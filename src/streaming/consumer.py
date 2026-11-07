"""Consume lecturas de Kafka, agrega por minuto y guarda en DuckDB (ya resuelto).

    python -m streaming.consumer

Garantía de entrega: los offsets se confirman (commit) DESPUÉS de escribir en
el sink. Si el proceso muere entre escribir y confirmar, al reiniciar se
reprocesan esos eventos: eso es "al menos una vez". Como el sink es
idempotente, reprocesar no duplica resultados. Juntos dan el efecto de
"exactamente una vez".
"""

import os
import signal

from confluent_kafka import Consumer

from streaming.alerts import detect_alerts
from streaming.events import Reading
from streaming.producer import TOPIC
from streaming.sink import DuckDBSink
from streaming.windows import TumblingWindowAggregator

running = True


def stop(*_):
    global running
    running = False


def handle(results, sink):
    sink.write(results)
    for a in detect_alerts(results):
        print(f"[ALERTA] {a.station_id} {a.window_start:%H:%M} PM2.5={a.avg_pm25} -> {a.level}")


def main():
    signal.signal(signal.SIGINT, stop)
    consumer = Consumer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "localhost:19092"),
        "group.id": "agregador-pm25",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC])
    agg = TumblingWindowAggregator(window_seconds=60, allowed_lateness_seconds=30)
    sink = DuckDBSink(os.getenv("DUCKDB_FILE", "aire.duckdb"))
    print("[consumer] escuchando... (Ctrl+C para terminar)")

    while running:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"[consumer] error: {msg.error()}")
            continue
        results = agg.add(Reading.from_json(msg.value()))
        if results:
            handle(results, sink)
            consumer.commit(asynchronous=False)

    handle(agg.flush(), sink)
    consumer.commit(asynchronous=False)
    consumer.close()
    print(f"[consumer] fin. ventanas guardadas: {sink.count()}, "
          f"tardíos: {agg.late_events}, duplicados: {agg.duplicates}")


if __name__ == "__main__":
    main()
