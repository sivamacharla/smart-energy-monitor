import json
import os
from collections import deque

from src.alerting.rules_engine import RulesEngine


def make_config(alerts_path, sustained_minutes=5):
    return {
        "alerting": {
            "alerts_path": alerts_path,
            "thresholds_w": {"hvac_01": 1000},
            "spike_pct": 0.5,
            "offline_timeout_sec": 30,
            "sustained_minutes": sustained_minutes,
        }
    }


def test_threshold_alert_written(tmp_path):
    alerts_path = str(tmp_path / "alerts.jsonl")
    engine = RulesEngine(make_config(alerts_path))
    history = deque(maxlen=10)
    record = {"device_id": "hvac_01", "power_w": 1500, "timestamp": "2026-01-01T00:00:00+00:00"}
    history.append(record)
    engine.process(record, history)

    with open(alerts_path) as f:
        alerts = [json.loads(line) for line in f]
    assert any(a["rule"] == "threshold" for a in alerts)


def test_no_alert_below_threshold(tmp_path):
    alerts_path = str(tmp_path / "alerts.jsonl")
    engine = RulesEngine(make_config(alerts_path))
    history = deque(maxlen=10)
    record = {"device_id": "hvac_01", "power_w": 500, "timestamp": "2026-01-01T00:00:00+00:00"}
    history.append(record)
    engine.process(record, history)
    assert not os.path.exists(alerts_path)


def test_spike_alert(tmp_path):
    alerts_path = str(tmp_path / "alerts.jsonl")
    engine = RulesEngine(make_config(alerts_path))
    history = deque(maxlen=10)
    for p in [100, 105, 98, 102, 101]:
        history.append({"device_id": "fridge_01", "power_w": p})
    spike_record = {"device_id": "fridge_01", "power_w": 400, "timestamp": "2026-01-01T00:00:00+00:00"}
    history.append(spike_record)
    engine.process(spike_record, history)

    with open(alerts_path) as f:
        alerts = [json.loads(line) for line in f]
    assert any(a["rule"] == "spike" for a in alerts)


def test_alert_dedup_within_window(tmp_path):
    alerts_path = str(tmp_path / "alerts.jsonl")
    engine = RulesEngine(make_config(alerts_path))
    history = deque(maxlen=10)
    record = {"device_id": "hvac_01", "power_w": 1500, "timestamp": "2026-01-01T00:00:00+00:00"}
    history.append(record)
    engine.process(record, history)
    engine.process(record, history)  # second call within dedup window

    with open(alerts_path) as f:
        alerts = [json.loads(line) for line in f]
    threshold_alerts = [a for a in alerts if a["rule"] == "threshold"]
    assert len(threshold_alerts) == 1
