# Smart Energy Monitoring & Analytics System

An end-to-end IoT pipeline that ingests real-time energy telemetry from
embedded devices, streams it through a memory-bounded gateway, validates
and syncs it to the cloud, raises rule-based alerts on inefficiencies, and
surfaces consumption insights through cleaned data and visualizations.

This repo is a runnable simulation of the full system: a Raspberry Pi is
stood in for by a Python MQTT publisher, and AWS IoT Core has a local
fallback so the whole pipeline works without any cloud credentials. Swap
in real hardware/AWS by flipping one config flag (see below).

## Architecture

```
 [Simulated sensors]        [Gateway / "Raspberry Pi"]           [Cloud]
 sensor_simulator.py  --MQTT--> mqtt_ingest.py  --validate-->  aws_iot_client.py
   (4 devices,                  - bounded ring buffer/device      - AWS IoT Core, or
    2s interval)                - batch CSV flush                  local JSONL fallback
                                 - schema/range validation
                                 - quarantines bad records
                                        |
                                        v
                                 rules_engine.py
                                 - threshold / spike /
                                   sustained-usage / offline
                                 - writes data/processed/alerts.jsonl
                                        |
                                        v
                          clean_and_visualize.py (analytics)
                          - dedup, interpolation, IQR anomaly flags
                          - per-device time series + peak-hour heatmap
                          - output/plots/*.png, data/processed/summary_stats.csv
```

## How this maps to the resume bullets

- **MQTT data ingestion pipeline on Raspberry Pi, low-latency + reduced
  memory footprint** -> [`src/ingestion/mqtt_ingest.py`](src/ingestion/mqtt_ingest.py):
  fixed-size `deque(maxlen=...)` ring buffers per device (constant memory
  regardless of runtime), batched disk writes, per-message latency tracking.
- **Data-cleaning and visualization workflows, anomaly/peak-hour
  insights** -> [`src/analytics/clean_and_visualize.py`](src/analytics/clean_and_visualize.py):
  dedup, gap interpolation, IQR-based anomaly flagging, per-device time
  series plots and an hour-of-day peak-usage heatmap.
- **Automated alerting via rule-based event triggers** ->
  [`src/alerting/rules_engine.py`](src/alerting/rules_engine.py): threshold,
  spike-vs-rolling-average, sustained-overuse (peak-hour inefficiency), and
  device-offline rules, each deduplicated and logged.
- **AWS IoT Core device provisioning + end-to-end validation** ->
  [`src/cloud/aws_iot_client.py`](src/cloud/aws_iot_client.py): publishes
  through AWS IoT Core's data-plane API when certs/endpoint are configured,
  attaches a SHA-256 checksum to every record for integrity verification,
  and falls back to a local cloud simulator otherwise.

## Setup

```bash
python -m venv .venv
.venv\Scripts\activate        # (or `source .venv/bin/activate` on macOS/Linux)
pip install -r requirements.txt
```

You need a local MQTT broker. Easiest path with Docker:

```bash
docker-compose up -d
```

(Alternatively install Mosquitto natively and point `config/settings.yaml`
at it.)

## Run the full demo

```bash
python main.py --duration 90
```

This runs the simulator + ingestion + alerting + cloud sync for 90 seconds,
then automatically generates plots and a summary. Outputs:

- `data/raw/telemetry.csv` -- cleaned ingestion log
- `data/processed/alerts.jsonl` -- rule-based alerts fired during the run
- `data/processed/cloud_sync.jsonl` -- records that "reached the cloud" (local sim)
- `output/plots/*.png` -- per-device time series + peak-hour heatmap
- `data/processed/summary_stats.csv` -- per-device consumption summary

You can also run each stage independently for a live demo across multiple
terminals:

```bash
python -m src.simulator.sensor_simulator --duration 120
python -m src.ingestion.mqtt_ingest --duration 120
python -m src.analytics.clean_and_visualize
```

## Enabling real AWS IoT Core

1. Provision a Thing in AWS IoT Core and download its root CA, private key,
   and certificate.
2. Drop them under `certs/` (already gitignored).
3. In `config/settings.yaml`, set `aws_iot.enabled: true` and fill in your
   `endpoint` and cert paths.
4. `pip install boto3` (already in `requirements.txt`) and configure AWS
   credentials (`aws configure` or env vars) with `iotdata:Publish`
   permission on the target topic.

No other code changes are needed -- `build_cloud_bridge()` automatically
switches from the local simulator to the real AWS IoT client.

## Tests

```bash
pytest
```

Covers the rules engine (threshold/spike/dedup logic) and the data-cleaning
pipeline (dedup, interpolation, anomaly flagging) without requiring a live
MQTT broker.
