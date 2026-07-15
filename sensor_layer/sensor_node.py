#!/usr/bin/env python3
"""
sensor_node.py

Simulates every hive's sensor cluster for one apiary, publishing to a
LOCAL MQTT broker (Mosquitto). This script represents Layer 1 (Sensor
Layer) only -- it knows nothing about the fog node, batching,
filtering, or AWS. It just generates and publishes.

Topic structure:
    apiary/{apiary_id}/hive/{hive_id}/{sensor_type}

Publish pattern (paho-mqtt client + on_connect/on_publish callbacks)
follows the same structure used in Lab 1 (MQTT Fundamentals).

Frequency is configurable in two ways, checked in this order:
    1. Environment variable override, e.g. SENSOR_WEIGHT_INTERVAL_SEC=15
    2. config.json  -> sensors.<type>.interval_sec  (default)
"""

import json
import os
import random
import threading
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

from generators import build_generators_for_hive

CONFIG_PATH = os.environ.get("CONFIG_PATH", os.path.join(os.path.dirname(__file__), "config.json"))

stop_event = threading.Event()


def load_config():
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)


def resolve_interval(sensor_type, default_interval):
    """Env var override takes priority over config.json.
    e.g. SENSOR_WEIGHT_INTERVAL_SEC, SENSOR_INTERNAL_TEMP_INTERVAL_SEC"""
    env_key = f"SENSOR_{sensor_type.upper()}_INTERVAL_SEC"
    val = os.environ.get(env_key)
    if val is not None:
        try:
            return float(val)
        except ValueError:
            print(f"[WARN] Ignoring invalid {env_key}={val!r}, using config default.")
    return default_interval


def on_connect(client, userdata, flags, rc, properties=None):
    # rc is a paho ReasonCode object (VERSION2 callback API) - it already
    # stringifies to a human-readable reason (e.g. "Success"), so just
    # print it directly rather than trying to use it as a dict key.
    print(f"[CONNECT] {rc}")


def on_publish(client, userdata, mid, reason_code=None, properties=None):
    pass  # per-message ack; kept quiet to avoid flooding stdout


def sensor_loop(client, apiary_id, hive_id, sensor_type, sensor_def, generators):
    """One thread per (hive_id, sensor_type). Publishes one MQTT
    message per tick containing every field for that sensor type."""
    interval = resolve_interval(sensor_type, sensor_def["interval_sec"])
    unit = sensor_def.get("unit", "")
    topic = f"apiary/{apiary_id}/hive/{hive_id}/{sensor_type}"

    print(f"[START] {topic} every {interval}s")

    last_tick = time.time()
    while not stop_event.is_set():
        now = time.time()
        dt = now - last_tick
        last_tick = now

        readings = {}
        anomaly_flags = []
        for field_name, gen in generators.items():
            value, fired_label = gen.next_value(dt)
            readings[field_name] = value
            if fired_label:
                anomaly_flags.append(fired_label)

        payload = {
            "apiary_id": apiary_id,
            "hive_id": hive_id,
            "sensor_type": sensor_type,
            "unit": unit,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "readings": readings,
            # NOTE: this flag is for TESTING/DEMO visibility only -- in the
            # real pipeline, anomaly detection is the fog node's job, not
            # the sensor's. A real sensor would never know it's anomalous.
            "_simulated_anomaly_injected": anomaly_flags,
        }

        client.publish(topic, json.dumps(payload), qos=1)

        if anomaly_flags:
            print(f"[ANOMALY INJECTED] {topic} -> {anomaly_flags} {readings}")

        stop_event.wait(interval)


def main():
    config = load_config()
    apiary_id = config["apiary_id"]
    hives = config["hives"]
    sensor_cfg = config["sensors"]

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"sensors-{apiary_id}")
    client.on_connect = on_connect
    client.on_publish = on_publish

    host = os.environ.get("MQTT_BROKER_HOST", config.get("mqtt_broker_host", "localhost"))
    port = int(os.environ.get("MQTT_BROKER_PORT", config.get("mqtt_broker_port", 1883)))

    print(f"[INFO] Connecting to local broker {host}:{port} ...")
    client.connect(host, port, keepalive=60)
    client.loop_start()
    time.sleep(1)  # let the connection settle before publishing

    threads = []
    for hive_id in hives:
        # Each hive gets its own independent generator set / random seed,
        # so anomalies and noise don't sync up across hives.
        seed = random.randint(0, 1_000_000)
        hive_generators = build_generators_for_hive(sensor_cfg, seed=seed)

        for sensor_type, sensor_def in sensor_cfg.items():
            t = threading.Thread(
                target=sensor_loop,
                args=(client, apiary_id, hive_id, sensor_type, sensor_def, hive_generators[sensor_type]),
                daemon=True,
            )
            t.start()
            threads.append(t)

    print(f"[INFO] {len(threads)} sensor threads running across {len(hives)} hive(s). Ctrl+C to stop.")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[INFO] Stopping...")
        stop_event.set()
        for t in threads:
            t.join(timeout=2)
        client.loop_stop()
        client.disconnect()
        print("[INFO] Disconnected. Bye!")


if __name__ == "__main__":
    main()
