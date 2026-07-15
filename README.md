# Smart Energy Monitoring & Analytics System

A small IoT pipeline for monitoring home/building energy usage: sensors publish
power readings over MQTT, a gateway process ingests and validates them, a
rules engine watches for waste/inefficiency, and everything eventually gets
cleaned up and turned into plots.

The original setup this is modeled on used a Raspberry Pi as the gateway and
AWS IoT Core for the cloud side. I don't have either of those wired up here,
so the Pi is stood in for by a Python script that publishes fake-but-realistic
sensor data, and the AWS side has a local fallback that just writes to a file
instead of actually hitting AWS. Flip a config flag once you've got real
credentials and it'll use AWS IoT Core instead — no code changes needed.

## How it fits together

```
sensor_simulator.py --MQTT--> mqtt_ingest.py --> rules_engine.py --> alerts.jsonl
   (4 fake devices)              |                 (threshold/spike/
                                  |                  sustained/offline)
                                  v
                          aws_iot_client.py
                     (AWS IoT Core, or local JSONL
                        file if not configured)

                       ... later, offline ...

              clean_and_visualize.py reads data/raw/telemetry.csv,
              dedups/interpolates/flags anomalies, spits out
              per-device plots + a peak-hour heatmap
```

A few things worth knowing about the ingestion side (`src/ingestion/mqtt_ingest.py`):
it keeps a fixed-size `deque` per device instead of an unbounded list, so
memory use doesn't grow no matter how long it runs — that matters if this is
actually sitting on a Pi. It also validates every record (field types, voltage
range, parseable timestamp) before doing anything else with it; anything that
fails gets written to a quarantine file instead of silently dropped.

The rules engine (`src/alerting/rules_engine.py`) is intentionally simple —
per-device wattage thresholds, a rolling-average spike check, a "sustained
overuse" check for peak-hour inefficiency, and an offline-device check. Alerts
get deduped so one bad device doesn't spam the log every couple seconds.

For the cloud side, I used boto3's IoT data-plane client rather than pulling
in the full AWS IoT device SDK — didn't need MQTT-over-mutual-TLS from the
gateway itself since the gateway already talks MQTT locally; this just
forwards validated records up with a checksum attached so you can catch
anything that got mangled in transit.

The analytics step is plain pandas/matplotlib instead of MATLAB — no license
needed, and it does the same job: drop duplicate readings, interpolate small
gaps, flag outliers per-device with an IQR check, then plot a time series per
device (anomalies marked in red) and an hour-of-day heatmap to spot peak usage.

## Running it

```bash
python -m venv .venv
.venv\Scripts\activate        # source .venv/bin/activate on mac/linux
pip install -r requirements.txt
```

You'll need an MQTT broker running on localhost:1883. Docker's the easiest way:

```bash
docker-compose up -d
```

(No Docker? Install Mosquitto directly, or point `config/settings.yaml` at
whatever broker you've got.)

Then just:

```bash
python main.py --duration 90
```

That runs the simulator + ingestion + alerting + cloud sync for 90 seconds and
then generates the plots automatically. When it's done you'll have:

- `data/raw/telemetry.csv` — everything that got ingested
- `data/processed/alerts.jsonl` — whatever the rules engine flagged
- `data/processed/cloud_sync.jsonl` — records that made it "to the cloud" (local sim)
- `output/plots/*.png` — per-device time series + the peak-hour heatmap
- `data/processed/summary_stats.csv` — quick per-device stats

If you want to watch it running live, it's more fun to run each piece in its
own terminal instead of through `main.py`:

```bash
python -m src.simulator.sensor_simulator --duration 120
python -m src.ingestion.mqtt_ingest --duration 120
python -m src.analytics.clean_and_visualize
```

## Hooking up real AWS IoT Core

1. Provision a Thing in the AWS IoT console, download its root CA, private
   key, and certificate.
2. Put them in `certs/` (gitignored, don't worry).
3. In `config/settings.yaml`, set `aws_iot.enabled: true` and fill in the
   endpoint + cert paths.
4. Make sure your AWS credentials are configured (`aws configure` or env
   vars) with `iotdata:Publish` on the topic you're using.

That's it — `build_cloud_bridge()` picks the real client over the local sim
automatically once it sees `enabled: true` and valid cert paths.

## Tests

```bash
pytest
```

Covers the rules engine and the cleaning logic directly, so you don't need a
broker running just to check that stuff isn't broken.
