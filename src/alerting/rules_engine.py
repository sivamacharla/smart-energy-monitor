"""Rule-based alerting engine: threshold, spike, sustained-usage and offline-device rules."""
from __future__ import annotations

import json
import logging
import os
import statistics
import threading
import time
from datetime import datetime, timezone

log = logging.getLogger(__name__)


class RulesEngine:
    def __init__(self, config: dict):
        acfg = config["alerting"]
        self.thresholds = acfg["thresholds_w"]
        self.spike_pct = acfg["spike_pct"]
        self.offline_timeout = acfg["offline_timeout_sec"]
        self.sustained_seconds = acfg["sustained_minutes"] * 60
        self.alerts_path = acfg["alerts_path"]
        os.makedirs(os.path.dirname(self.alerts_path), exist_ok=True)

        self.lock = threading.Lock()
        self._last_seen: dict[str, float] = {}
        self._sustained_since: dict[str, float] = {}
        self._alert_dedup: dict[tuple[str, str], float] = {}
        self._dedup_window_sec = 60

    def _emit(self, device_id: str, rule: str, message: str, record: dict):
        key = (device_id, rule)
        now = time.monotonic()
        last = self._alert_dedup.get(key)
        if last and (now - last) < self._dedup_window_sec:
            return  # avoid spamming the same alert on every message
        self._alert_dedup[key] = now

        alert = {
            "device_id": device_id,
            "rule": rule,
            "message": message,
            "power_w": record.get("power_w"),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        with self.lock:
            with open(self.alerts_path, "a") as f:
                f.write(json.dumps(alert) + "\n")
        log.warning("ALERT [%s] %s: %s", device_id, rule, message)

    def process(self, record: dict, device_history):
        device_id = record["device_id"]
        power = record["power_w"]
        now_wall = time.time()
        self._last_seen[device_id] = now_wall

        # 1. threshold rule
        limit = self.thresholds.get(device_id)
        if limit is not None and power > limit:
            self._emit(device_id, "threshold", f"power {power}W exceeds limit {limit}W", record)

        # 2. spike rule -- compare against rolling average of recent history
        history_powers = [r["power_w"] for r in device_history]
        if len(history_powers) >= 5:
            baseline = statistics.mean(history_powers[:-1])
            if baseline > 0 and (power - baseline) / baseline > self.spike_pct:
                pct = round((power - baseline) / baseline * 100, 1)
                self._emit(device_id, "spike", f"power jumped {pct}% above rolling avg {round(baseline, 1)}W", record)

        # 3. sustained-usage / peak-hour inefficiency rule
        if limit is not None and power > limit:
            start = self._sustained_since.setdefault(device_id, now_wall)
            if now_wall - start > self.sustained_seconds:
                mins = self.sustained_seconds / 60
                self._emit(device_id, "sustained", f"sustained above {limit}W for over {mins:.0f} min -- review peak-hour usage", record)
        else:
            self._sustained_since.pop(device_id, None)

    def check_offline_devices(self):
        """Call periodically (e.g. from a watchdog thread) to flag silent devices."""
        now_wall = time.time()
        for device_id, last_seen in list(self._last_seen.items()):
            if now_wall - last_seen > self.offline_timeout:
                self._emit(device_id, "offline", f"no data received for over {self.offline_timeout}s", {"power_w": None})
