"""
Cloud bridge to AWS IoT Core.

If real AWS IoT credentials/certs are configured (aws_iot.enabled=true in
config and the cert files exist on disk), this publishes validated
telemetry straight to AWS IoT Core over the IoT data-plane API. Without
credentials it falls back to a local "cloud simulator" that appends to a
JSONL file so the rest of the pipeline behaves identically in dev/demo
environments that don't have a provisioned AWS account.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
from datetime import datetime, timezone

log = logging.getLogger(__name__)


def checksum(record: dict) -> str:
    """End-to-end integrity check carried alongside each record to the cloud."""
    payload = json.dumps(record, sort_keys=True).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:16]


def _envelope(record: dict, backend: str) -> dict:
    return {
        "record": record,
        "checksum": checksum(record),
        "cloud_received_at": datetime.now(timezone.utc).isoformat(),
        "backend": backend,
    }


class LocalCloudSim:
    """Stand-in for AWS IoT Core when no device certs are provisioned."""

    def __init__(self, path: str):
        self.path = path
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.lock = threading.Lock()
        self.sent = 0
        self.rejected = 0

    def publish(self, record: dict):
        envelope = _envelope(record, backend="local-sim")
        with self.lock:
            with open(self.path, "a") as f:
                f.write(json.dumps(envelope) + "\n")
            self.sent += 1


class AwsIotBridge:
    """Real AWS IoT Core publisher using boto3's IoT data-plane client."""

    def __init__(self, cfg: dict):
        import boto3  # imported lazily so boto3 is optional for local-only runs

        self.publish_topic = cfg["publish_topic"]
        self.client = boto3.client("iot-data", endpoint_url=f"https://{cfg['endpoint']}")
        self.sent = 0
        self.rejected = 0
        self.lock = threading.Lock()

    def publish(self, record: dict):
        envelope = _envelope(record, backend="aws-iot-core")
        try:
            self.client.publish(topic=self.publish_topic, qos=1, payload=json.dumps(envelope))
            with self.lock:
                self.sent += 1
        except Exception:
            log.exception("AWS IoT Core publish failed")
            with self.lock:
                self.rejected += 1


def build_cloud_bridge(config: dict):
    cfg = config["aws_iot"]
    certs_present = all(os.path.exists(cfg[k]) for k in ("root_ca", "private_key", "certificate"))
    if cfg.get("enabled") and certs_present:
        log.info("using AWS IoT Core bridge (endpoint=%s)", cfg["endpoint"])
        return AwsIotBridge(cfg)
    log.info("AWS IoT not provisioned -- using local cloud simulator at %s", cfg["local_fallback_path"])
    return LocalCloudSim(cfg["local_fallback_path"])
