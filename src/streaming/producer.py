"""Publica lecturas simuladas en Kafka (ya resuelto).

    python -m streaming.producer --n 3000 --rate 50

--rate es cuántos eventos por segundo. La llave del mensaje es la estación:
así todas las lecturas de una estación caen en la misma partición y
conservan su orden relativo dentro de ella.
"""

import argparse
import os
import time
from datetime import datetime

from confluent_kafka import Producer

from streaming.generator import simulate

TOPIC = "lecturas-pm25"


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--n", type=int, default=3000)
    p.add_argument("--rate", type=float, default=50)
    args = p.parse_args()

    producer = Producer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "localhost:19092"),
                         "enable.idempotence": True})
    for i, reading in enumerate(simulate(args.n, datetime.now().replace(microsecond=0))):
        producer.produce(TOPIC, key=reading.station_id, value=reading.to_json())
        producer.poll(0)
        if i % 500 == 0:
            print(f"[producer] {i} enviados")
        time.sleep(1 / args.rate)
    producer.flush()
    print(f"[producer] listo: {args.n} lecturas en '{TOPIC}'")


if __name__ == "__main__":
    main()
