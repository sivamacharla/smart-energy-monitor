"""
Memory-bounded MQTT ingestion pipeline (the "Raspberry Pi" side of the system).

Uses fixed-size per-device ring buffers so memory usage stays constant no
matter how long the pipeline runs -- important on constrained embedded
hardware. Validated records are batch-flushed to disk and handed off to the
rules engine and the cloud bridge for further processing.
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import threading
import time
from collections import deque, defaultdict
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
import yaml

from src.cloud.aws_iot_client import build_cloud_bridge
from src.alerting.rules_engine import RulesEngine

logging.basicConfig(level=logging.INFO, format="%(asctime)s [ingest] %(message)s")
log = logging.getLogger(__name__)

REQUIRED_FIELDS = {"device_id", "timestamp", "voltage", "current", "power_w", "seq"}


def validate_record(record: dict) -> tuple[bool, str]:
    missing = REQUIRED_FIELDS - record.keys()
    if missing:
        return False, f"missing fields: {missing}"
    if not isinstance(record["power_w"], (int, float)) or record["power_w"] < 0:
        return False, "power_w out of range"
    if not isinstance(record["voltage"], (int, float)) or not (80 <= record["voltage"] <= 160):
        return False, "voltage out of range"
    try:
        datetime.fromisoformat(record["timestamp"])
    except ValueError:
        return False, "bad timestamp"
    return True, ""


class IngestionPipeline:
    def __init__(self, config: dict):
        self.cfg = config
        icfg = config["ingestion"]
        self.buffer_size = icfg["buffer_size_per_device"]
        self.batch_flush_size = icfg["batch_flush_size"]
        self.raw_path = icfg["raw_data_path"]
        self.quarantine_path = icfg["quarantine_path"]

        self.buffers: dict[str, deque] = defaultdict(lambda: deque(maxlen=self.buffer_size))
        self.pending_batch: list[tuple[dict, datetime, float | None]] = []
        self.lock = threading.Lock()

        self.stats = {"received": 0, "valid": 0, "invalid": 0, "flushed": 0}
        self.last_latency_ms: float | None = None

        os.makedirs(os.path.dirname(self.raw_path), exist_ok=True)
        os.makedirs(os.path.dirname(self.quarantine_path), exist_ok=True)
        self._ensure_csv_header()

        self.cloud_bridge = build_cloud_bridge(config)
        self.rules_engine = RulesEngine(config)

        self.client = mqtt.Client(client_id=f"{icfg.get('client_id_prefix', 'ingest')}-{os.getpid()}")
        self.client.on_message = self._on_message
        self.client.on_connect = self._on_connect

    def _ensure_csv_header(self):
        if not os.path.exists(self.raw_path):
            with open(self.raw_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["device_id", "seq", "timestamp", "voltage", "current", "power_w", "ingested_at", "latency_ms"])

    def _on_connect(self, client, userdata, flags, rc):
        topic = self.cfg["mqtt"]["subscribe_topic"]
        client.subscribe(topic, qos=1)
        log.info("subscribed to %s (rc=%s)", topic, rc)

    def _on_message(self, client, userdata, msg):
        recv_time = datetime.now(timezone.utc)
        try:
            record = json.loads(msg.payload.decode("utf-8"))
        except json.JSONDecodeError:
            self.stats["invalid"] += 1
            return

        self.stats["received"] += 1
        ok, reason = validate_record(record)
        if not ok:
            self.stats["invalid"] += 1
            with open(self.quarantine_path, "a") as f:
                f.write(json.dumps({**record, "_reason": reason}) + "\n")
            return

        self.stats["valid"] += 1
        try:
            sent_time = datetime.fromisoformat(record["timestamp"])
            latency_ms = max((recv_time - sent_time).total_seconds() * 1000, 0)
        except Exception:
            latency_ms = None
        self.last_latency_ms = latency_ms

        device_id = record["device_id"]
        with self.lock:
            self.buffers[device_id].append(record)
            self.pending_batch.append((record, recv_time, latency_ms))
            if len(self.pending_batch) >= self.batch_flush_size:
                self._flush()

        # streaming consumers: rules engine (local, real-time) + cloud bridge
        self.rules_engine.process(record, self.buffers[device_id])
        self.cloud_bridge.publish(record)

    def _flush(self):
        if not self.pending_batch:
            return
        with open(self.raw_path, "a", newline="") as f:
            writer = csv.writer(f)
            for record, recv_time, latency_ms in self.pending_batch:
                writer.writerow([
                    record["device_id"], record["seq"], record["timestamp"],
                    record["voltage"], record["current"], record["power_w"],
                    recv_time.isoformat(), round(latency_ms, 2) if latency_ms is not None else "",
                ])
        self.stats["flushed"] += len(self.pending_batch)
        self.pending_batch.clear()

    def run(self, duration: int | None = None):
        self.client.connect(self.cfg["mqtt"]["host"], self.cfg["mqtt"]["port"], keepalive=30)
        self.client.loop_start()
        start = time.time()
        try:
            while True:
                time.sleep(1)
                if duration and (time.time() - start) > duration:
                    break
        except KeyboardInterrupt:
            pass
        finally:
            with self.lock:
                self._flush()
            self.client.loop_stop()
            self.client.disconnect()
            log.info("ingestion stopped: %s", self.stats)


def main():
    parser = argparse.ArgumentParser(description="MQTT ingestion pipeline")
    parser.add_argument("--config", default="config/settings.yaml")
    parser.add_argument("--duration", type=int, default=None)
    args = parser.parse_args()
    with open(args.config) as f:
        config = yaml.safe_load(f)
    pipeline = IngestionPipeline(config)
    pipeline.run(duration=args.duration)


if __name__ == "__main__":
    main()
