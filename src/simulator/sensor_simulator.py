"""Simulates Raspberry Pi-connected energy meters publishing telemetry over MQTT."""
from __future__ import annotations

import argparse
import json
import logging
import math
import random
import signal
import threading
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
import yaml

logging.basicConfig(level=logging.INFO, format="%(asctime)s [simulator] %(message)s")
log = logging.getLogger(__name__)

DEVICE_PROFILES = {
    "hvac_01":         {"base_w": 1800, "noise": 120, "daily_swing": 900},
    "water_heater_01": {"base_w": 1200, "noise": 80,  "daily_swing": 400},
    "fridge_01":       {"base_w": 150,  "noise": 15,  "daily_swing": 20},
    "lighting_01":     {"base_w": 90,   "noise": 10,  "daily_swing": 60},
}

ANOMALY_PROBABILITY = 0.015  # ~1.5% of readings simulate a fault/spike


def load_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def diurnal_factor(now: datetime) -> float:
    """Crude day/night usage curve peaking at 19:00, trough at 04:00."""
    hour = now.hour + now.minute / 60
    return 0.5 + 0.5 * (1 + math.cos((hour - 19) / 24 * 2 * math.pi))


def make_reading(device_id: str, profile: dict, seq: int) -> dict:
    now = datetime.now(timezone.utc)
    factor = diurnal_factor(now)
    power = profile["base_w"] + profile["daily_swing"] * factor + random.gauss(0, profile["noise"])
    is_anomaly = random.random() < ANOMALY_PROBABILITY
    if is_anomaly:
        power *= random.uniform(2.2, 3.5)
    power = max(power, 0.0)
    voltage = random.gauss(120.0, 1.5)
    current = round(power / max(voltage, 1e-3), 3)
    return {
        "device_id": device_id,
        "seq": seq,
        "timestamp": now.isoformat(),
        "voltage": round(voltage, 2),
        "current": current,
        "power_w": round(power, 2),
        "simulated_anomaly": is_anomaly,
    }


class SensorSimulator:
    def __init__(self, config: dict, stop_event: threading.Event | None = None):
        self.cfg = config
        self.stop_event = stop_event or threading.Event()
        self.client = mqtt.Client(client_id=f"rpi-sim-{random.randint(1000, 9999)}")
        self.client.connect(config["mqtt"]["host"], config["mqtt"]["port"], keepalive=30)
        self.client.loop_start()
        self.seq = {d: 0 for d in DEVICE_PROFILES}

    def run(self, duration: int | None = None, interval: float | None = None):
        interval = interval or self.cfg["mqtt"].get("publish_interval_sec", 2)
        topic_tmpl = self.cfg["mqtt"]["telemetry_topic_template"]
        start = time.time()
        try:
            while not self.stop_event.is_set():
                for device_id, profile in DEVICE_PROFILES.items():
                    self.seq[device_id] += 1
                    reading = make_reading(device_id, profile, self.seq[device_id])
                    topic = topic_tmpl.format(device_id=device_id)
                    self.client.publish(topic, json.dumps(reading), qos=1)
                if duration and (time.time() - start) > duration:
                    break
                time.sleep(interval)
        finally:
            self.client.loop_stop()
            self.client.disconnect()
            log.info("simulator stopped after %d cycles/device", max(self.seq.values(), default=0))


def main():
    parser = argparse.ArgumentParser(description="Simulated Raspberry Pi energy meter publisher")
    parser.add_argument("--config", default="config/settings.yaml")
    parser.add_argument("--duration", type=int, default=None, help="seconds to run, default forever")
    args = parser.parse_args()

    config = load_config(args.config)
    sim = SensorSimulator(config)

    def handle_sigint(sig, frame):
        sim.stop_event.set()

    signal.signal(signal.SIGINT, handle_sigint)
    sim.run(duration=args.duration)


if __name__ == "__main__":
    main()
