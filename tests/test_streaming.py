"""Pruebas de la lógica de streaming. No necesitan Kafka: le pasamos los
eventos uno por uno, en el orden en que "llegarían"."""

from datetime import datetime, timedelta

import pytest

from streaming.alerts import classify, detect_alerts
from streaming.events import Reading, WindowResult
from streaming.generator import simulate
from streaming.sink import DuckDBSink
from streaming.windows import TumblingWindowAggregator

T0 = datetime(2026, 3, 10, 7, 0, 0)


def r(seconds, pm=20.0, station="MED-ITA", eid=None):
    return Reading(eid or f"{station}-{seconds}-{pm}", station, T0 + timedelta(seconds=seconds), pm)


def feed(agg, events):
    out = []
    for e in events:
        out.extend(agg.add(e))
    return out


# ---------- ventanas ----------

def test_inicio_de_ventana():
    agg = TumblingWindowAggregator(window_seconds=60)
    assert agg.window_start(T0 + timedelta(seconds=59)) == T0
    assert agg.window_start(T0 + timedelta(seconds=60)) == T0 + timedelta(minutes=1)
    assert agg.window_start(datetime(2026, 3, 10, 7, 4, 37)) == datetime(2026, 3, 10, 7, 4)


def test_no_emite_mientras_la_ventana_esta_abierta():
    agg = TumblingWindowAggregator(60, allowed_lateness_seconds=10)
    assert feed(agg, [r(0), r(30), r(65)]) == []          # watermark = 55s < 60s


def test_emite_cuando_la_marca_de_agua_pasa_el_final():
    agg = TumblingWindowAggregator(60, allowed_lateness_seconds=10)
    out = feed(agg, [r(0, 10.0), r(30, 30.0), r(71, 50.0)])  # watermark = 61s
    assert out == [WindowResult("MED-ITA", T0, T0 + timedelta(minutes=1), 2, 20.0, 30.0)]
    assert agg.watermark == T0 + timedelta(seconds=61)


def test_ventanas_separadas_por_estacion():
    agg = TumblingWindowAggregator(60, 0)
    out = feed(agg, [r(10, station="A"), r(20, station="B"), r(61, station="A")])
    assert sorted(w.station_id for w in out) == ["A", "B"]


def test_evento_desordenado_dentro_de_la_tolerancia_cuenta():
    agg = TumblingWindowAggregator(60, allowed_lateness_seconds=20)
    out = feed(agg, [r(10), r(65), r(50), r(85)])          # el 50 llega después del 65
    assert out[0].count == 2 and agg.late_events == 0


def test_evento_demasiado_tarde_se_descarta_y_se_cuenta():
    agg = TumblingWindowAggregator(60, allowed_lateness_seconds=10)
    out = feed(agg, [r(10), r(75), r(30)])                 # la ventana 0-60 ya cerró
    assert out[0].count == 1
    assert agg.late_events == 1


def test_duplicados_se_ignoran():
    agg = TumblingWindowAggregator(60, 0)
    out = feed(agg, [r(10, eid="x"), r(10, eid="x"), r(20, eid="y"), r(61)])
    assert out[0].count == 2 and agg.duplicates == 1


def test_flush_emite_lo_pendiente_en_orden():
    agg = TumblingWindowAggregator(60, 30)
    feed(agg, [r(10, station="B"), r(20, station="A"), r(70, station="A")])
    out = agg.flush()
    assert [(w.station_id, w.window_start.minute) for w in out] == [("A", 0), ("A", 1), ("B", 0)]
    assert agg.flush() == []


def test_con_el_simulador_no_se_pierde_ni_duplica_nada():
    events = simulate(2000, T0, seed=3, late_prob=0.0, dup_prob=0.05)
    agg = TumblingWindowAggregator(60, allowed_lateness_seconds=15)
    out = feed(agg, events) + agg.flush()
    assert sum(w.count for w in out) == 2000          # cada lectura única cuenta una vez
    assert agg.duplicates > 0 and agg.late_events == 0
    keys = [(w.station_id, w.window_start) for w in out]
    assert len(keys) == len(set(keys))                # cada ventana se emite una sola vez


# ---------- alertas ----------

@pytest.mark.parametrize("pm,level", [
    (5, "buena"), (12, "buena"), (12.1, "aceptable"), (40, "dañina para grupos sensibles"),
    (90, "dañina"), (200, "muy dañina"), (400, "peligrosa"),
])
def test_clasificacion(pm, level):
    assert classify(pm) == level


def test_detecta_solo_desde_el_nivel_pedido():
    results = [WindowResult("A", T0, T0, 10, avg, avg) for avg in (10, 30, 45, 80)]
    alerts = detect_alerts(results)
    assert [a.avg_pm25 for a in alerts] == [45, 80]
    assert alerts[1].level == "dañina"
    assert len(detect_alerts(results, min_level="dañina")) == 1


def test_el_simulador_tiene_un_episodio_de_contaminacion():
    agg = TumblingWindowAggregator(60, 15)
    out = feed(agg, simulate(2000, T0, seed=3)) + agg.flush()
    stations = {a.station_id for a in detect_alerts(out)}
    assert stations == {"MED-ITA"}


# ---------- sink idempotente ----------

def test_sink_idempotente():
    sink = DuckDBSink()
    w = WindowResult("A", T0, T0 + timedelta(minutes=1), 3, 20.0, 25.0)
    sink.write([w])
    sink.write([w])                                    # reproceso tras un fallo
    assert sink.count() == 1


def test_sink_actualiza_si_cambia():
    sink = DuckDBSink()
    w = WindowResult("A", T0, T0 + timedelta(minutes=1), 3, 20.0, 25.0)
    sink.write([w])
    sink.write([WindowResult("A", T0, T0 + timedelta(minutes=1), 4, 22.0, 30.0)])
    row = sink.con.execute("SELECT lecturas, pm25_promedio FROM pm25_por_minuto").fetchone()
    assert row == (4, 22.0)
