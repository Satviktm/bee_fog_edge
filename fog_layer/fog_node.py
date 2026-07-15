#!/usr/bin/env python3
"""
fog_node.py

Layer 2 (Fog Layer). Subscribes to the SAME local MQTT broker the
sensor layer publishes to, and for every incoming reading:

    1. filter_reading()   -> reject impossible/frozen-sensor values
    2. check_anomaly()    -> independently decide if this is a real
                              colony event (never trusts the sensor's
                              own self-reported flag)
    3. aggregate/batch     -> accumulate valid readings, emit one
                              averaged payload every `batch_size`
                              readings per (hive, sensor_type)
    4. outbox.insert()     -> durable local buffer; anomaly alerts get
                              priority=1 (sent first), routine batches
                              get priority=0

Three background threads run alongside the MQTT loop:
    - watchdog   -> flags a source "offline" if no message arrives
                    within heartbeat_timeout_factor x its expected interval
    - purge      -> enforces the bounded-buffer cap (oldest dropped first)
    - dispatcher -> drains the outbox via the pluggable Uploader,
                    retrying failed sends with exponential backoff
"""

import json
import os
import statistics
import threading
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

from state import FogState
from processing import filter_reading, check_anomaly
from outbox import Outbox
from uploader import MockUploader

CONFIG_PATH = os.environ.get("CONFIG_PATH", os.path.join(os.path.dirname(__file__), "config.json"))
stop_event = threading.Event()


def load_config():
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)


def now_iso():
    return datetime.now(timezone.utc).isoformat()


# ------------------------------------------------------------- payload builders

def build_alert_payload(hive_id, sensor_type, field_name, value, event_timestamp, label):
    """event_timestamp is the SENSOR's own timestamp (when the reading
    was actually taken) -- preserved even though this alert may not
    reach AWS until later, so history stays accurate regardless of
    delivery delay."""
    return {
        "kind": "alert",
        "hive_id": hive_id,
        "sensor_type": sensor_type,
        "field": field_name,
        "value": value,
        "event_timestamp": event_timestamp,
        "detected_at": now_iso(),
        "label": label,
    }


def build_batch_payload(hive_id, sensor_type, buffer):
    """Averages every field across the buffered readings. `buffer` is
    a list of {"timestamp": ..., "fields": {name: value, ...}}."""
    field_names = buffer[0]["fields"].keys()
    means = {
        name: round(statistics.mean(r["fields"][name] for r in buffer), 3)
        for name in field_names
    }
    return {
        "kind": "batch",
        "hive_id": hive_id,
        "sensor_type": sensor_type,
        "sample_count": len(buffer),
        "window_start": buffer[0]["timestamp"],
        "window_end": buffer[-1]["timestamp"],
        "readings": means,
    }


# ------------------------------------------------------------- core processing

class FogProcessor:
    def __init__(self, config, outbox):
        self.config = config
        self.outbox = outbox
        stuck_at_len = config["stuck_at_repeat_threshold"]
        baseline_lens = [
            rule["baseline_window"]
            for sensor_rules in config["anomaly_rules"].values()
            for rule in sensor_rules.values()
        ]
        baseline_len = max(baseline_lens) if baseline_lens else 1
        self.state = FogState(stuck_at_len=stuck_at_len, baseline_len=baseline_len)
        self.stuck_at_len = stuck_at_len

    def handle_message(self, hive_id, sensor_type, readings, event_timestamp):
        now = time.time()
        source = self.state.get(hive_id, sensor_type)

        if source.offline_flagged:
            print(f"[RECOVERED] {hive_id}/{sensor_type} is publishing again")
            source.offline_flagged = False
        source.last_seen_time = now

        range_cfg = self.config["range_filters"]
        rate_cfg = self.config["rate_of_change_filters"]
        anomaly_cfg = self.config["anomaly_rules"].get(sensor_type, {})

        valid_fields = {}
        for field_name, value in readings.items():
            field_state = source.fields[field_name]

            is_valid, reason = filter_reading(
                sensor_type, field_name, value, now, field_state,
                range_cfg, rate_cfg, self.stuck_at_len,
            )

            if not is_valid:
                print(f"[DROPPED] {hive_id}/{sensor_type}/{field_name}={value} reason={reason}")
                if reason == "stuck_at_value" and not field_state.fault_flagged:
                    alert = build_alert_payload(
                        hive_id, sensor_type, field_name, value, event_timestamp,
                        "sensor_fault_suspected:stuck_at_value",
                    )
                    self.outbox.insert(alert, priority=1)
                    field_state.fault_flagged = True
                    print(f"[FAULT ALERT] {hive_id}/{sensor_type}/{field_name} looks frozen")
                continue

            # Reading is trustworthy -> update filter-relevant state
            field_state.last_value = value
            field_state.last_time = now
            field_state.recent_values.append(value)
            field_state.fault_flagged = False

            # Anomaly check uses baseline BEFORE this reading is added to it
            rule_cfg = anomaly_cfg.get(field_name)
            if rule_cfg:
                label = check_anomaly(value, field_state, rule_cfg)
                if label:
                    alert = build_alert_payload(hive_id, sensor_type, field_name, value, event_timestamp, label)
                    self.outbox.insert(alert, priority=1)
                    print(f"[ANOMALY DETECTED] {hive_id}/{sensor_type}/{field_name}={value} -> {label}")
            field_state.baseline_values.append(value)

            valid_fields[field_name] = value

        if valid_fields:
            source.batch_buffer.append({"timestamp": event_timestamp, "fields": valid_fields})
            if len(source.batch_buffer) >= self.config["batch_size"]:
                batch = build_batch_payload(hive_id, sensor_type, source.batch_buffer)
                self.outbox.insert(batch, priority=0)
                print(f"[BATCH EMITTED] {hive_id}/{sensor_type} n={batch['sample_count']} {batch['readings']}")
                source.batch_buffer.clear()

    def check_heartbeats(self):
        now = time.time()
        expected = self.config["expected_sensor_intervals_sec"]
        timeout_factor = self.config["heartbeat_timeout_factor"]
        for (hive_id, sensor_type), source in self.state.all_sources().items():
            if source.last_seen_time is None or source.offline_flagged:
                continue
            expected_interval = expected.get(sensor_type, 60)
            timeout = expected_interval * timeout_factor
            if now - source.last_seen_time > timeout:
                source.offline_flagged = True
                alert = build_alert_payload(hive_id, sensor_type, None, None, now_iso(), "sensor_offline")
                self.outbox.insert(alert, priority=1)
                print(f"[OFFLINE] {hive_id}/{sensor_type} silent for >{timeout:.0f}s")


# ------------------------------------------------------------- background threads

def watchdog_loop(processor, interval_sec):
    while not stop_event.is_set():
        processor.check_heartbeats()
        stop_event.wait(interval_sec)


def purge_loop(outbox, max_age_hours, interval_sec):
    while not stop_event.is_set():
        purged = outbox.purge_older_than(max_age_hours)
        if purged:
            print(f"[PURGE] dropped {purged} row(s) older than {max_age_hours}h (oldest-first, buffer cap exceeded)")
        stop_event.wait(interval_sec)


def dispatcher_loop(outbox, uploader, interval_sec, backoff_base, backoff_max):
    while not stop_event.is_set():
        ready = outbox.get_next_ready(limit=1)
        if ready:
            row = ready[0]
            success = uploader.send(row["payload"])
            if success:
                outbox.mark_sent(row["id"])
            else:
                backoff = outbox.mark_failed(row["id"], row["attempts"], backoff_base, backoff_max)
                print(f"[RETRY SCHEDULED] row {row['id']} attempt {row['attempts']+1}, next try in {backoff:.0f}s")
        stop_event.wait(interval_sec)


# ------------------------------------------------------------- MQTT wiring

def on_connect(client, userdata, flags, rc, properties=None):
    print(f"[CONNECT] {rc}")


def make_on_message(processor):
    def on_message(client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
            processor.handle_message(
                payload["hive_id"],
                payload["sensor_type"],
                payload["readings"],
                payload["timestamp"],
            )
        except Exception as e:
            print(f"[ERROR] failed to process message on {msg.topic}: {e}")
    return on_message


def main():
    config = load_config()
    outbox = Outbox(config["outbox_db_path"])

    if config["uploader"] == "mock":
        uploader = MockUploader(simulate_failure_rate=float(os.environ.get("SIMULATE_FAILURE_RATE", 0)))
    else:
        raise NotImplementedError(
            "IoT Core uploader not wired up yet -- see uploader.py IoTCoreUploader "
            "and fill in endpoint/cert paths once the AWS IoT Thing exists."
        )

    processor = FogProcessor(config, outbox)

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="fog-node")
    client.on_connect = on_connect
    client.on_message = make_on_message(processor)
    client.connect(config["mqtt_broker_host"], config["mqtt_broker_port"], keepalive=60)
    client.subscribe(config["subscribe_topic"])
    client.loop_start()

    threads = [
        threading.Thread(target=watchdog_loop, args=(processor, config["heartbeat_check_interval_sec"]), daemon=True),
        threading.Thread(target=purge_loop, args=(outbox, config["outbox_max_age_hours"], config["outbox_purge_check_interval_sec"]), daemon=True),
        threading.Thread(target=dispatcher_loop, args=(outbox, uploader, config["dispatch_interval_sec"], config["retry_backoff_base_sec"], config["retry_backoff_max_sec"]), daemon=True),
    ]
    for t in threads:
        t.start()

    print(f"[INFO] Fog node running. Subscribed to '{config['subscribe_topic']}'. Ctrl+C to stop.")
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
        outbox.close()
        print("[INFO] Disconnected. Bye!")


if __name__ == "__main__":
    main()
