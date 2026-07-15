"""
End-to-end demo runner: spins up the simulated Raspberry Pi sensor
publisher and the MQTT ingestion pipeline (with rule-based alerting and
the AWS IoT/local cloud bridge) together for a fixed window, then runs the
cleaning & visualization step over whatever telemetry was collected.

Requires an MQTT broker running on localhost:1883 (see docker-compose.yml).
"""
from __future__ import annotations

import argparse
import logging
import sys
import threading

import yaml

from src.simulator.sensor_simulator import SensorSimulator
from src.ingestion.mqtt_ingest import IngestionPipeline
from src.analytics import clean_and_visualize

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("main")


def main():
    parser = argparse.ArgumentParser(description="Run the full smart energy monitoring pipeline end to end")
    parser.add_argument("--config", default="config/settings.yaml")
    parser.add_argument("--duration", type=int, default=90, help="seconds to simulate before analyzing")
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    stop_event = threading.Event()
    sim = SensorSimulator(config, stop_event=stop_event)
    sim_thread = threading.Thread(target=sim.run, kwargs={"duration": args.duration}, daemon=True)

    pipeline = IngestionPipeline(config)
    ingest_thread = threading.Thread(target=pipeline.run, kwargs={"duration": args.duration + 3}, daemon=True)

    log.info("starting simulator + ingestion pipeline for %ss", args.duration)
    sim_thread.start()
    ingest_thread.start()

    sim_thread.join()
    ingest_thread.join(timeout=10)

    log.info("collection window done, stats=%s", pipeline.stats)
    log.info("running cleaning + visualization step")

    sys.argv = ["clean_and_visualize.py", "--config", args.config]
    clean_and_visualize.main()

    log.info("done. plots in output/plots, alerts in %s", config["alerting"]["alerts_path"])


if __name__ == "__main__":
    main()
