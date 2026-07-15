"""
uploader.py

The dispatcher (see fog_node.py) doesn't care HOW a payload actually
leaves the machine -- it just calls uploader.send(payload) and gets
True/False back. This makes it trivial to swap the transport later
without touching any of the filtering/aggregation/outbox logic:

    - MockUploader     -> used now, while there's no AWS IoT Thing/certs
                           yet. Just logs what WOULD have been sent, with
                           an optional simulated failure rate so the
                           retry/backoff logic in the outbox can be
                           tested honestly, without AWS.

    - IoTCoreUploader   -> the real one, following the exact MQTT+TLS
                           pattern from Lab 2 (aws_iot_publisher.py):
                           X.509 client cert + private key + Amazon Root
                           CA, connecting on port 8883. Fill in
                           ENDPOINT and the certs/ paths once the IoT
                           Thing has been created, then flip
                           config.json "uploader" from "mock" to
                           "iot_core".
"""

import random
import time


class Uploader:
    def send(self, payload: dict) -> bool:
        raise NotImplementedError


class MockUploader(Uploader):
    """Local stand-in for AWS IoT Core. Prints what it 'sent' and
    always succeeds, unless simulate_failure_rate is set > 0 -- used
    deliberately during testing to exercise the outbox's retry/backoff
    path without needing a real network failure."""

    def __init__(self, simulate_failure_rate: float = 0.0):
        self.simulate_failure_rate = simulate_failure_rate
        self.sent_count = 0
        self.failed_count = 0

    def send(self, payload: dict) -> bool:
        if random.random() < self.simulate_failure_rate:
            self.failed_count += 1
            print(f"[MOCK UPLOAD] simulated failure (payload kept in outbox)")
            return False

        self.sent_count += 1
        kind = payload.get("kind", "?")
        source = f"{payload.get('hive_id', '?')}/{payload.get('sensor_type', '?')}"
        print(f"[MOCK UPLOAD] OK  kind={kind:9s} source={source}")
        return True


class IoTCoreUploader(Uploader):
    """
    NOT YET CONNECTED -- fill in once the IoT Thing + certificates
    exist (per Lab 2), then set config.json "uploader": "iot_core".

    Expected cert layout (same as Lab 2):
        certs/AmazonRootCA1.pem
        certs/device_cert.pem.crt
        certs/device_private.pem.key
    """

    def __init__(self, endpoint, cert_dir, topic, port=8883):
        import ssl
        import json
        import paho.mqtt.client as mqtt
        from pathlib import Path

        self._json = json
        cert_dir = Path(cert_dir)
        self.topic = topic

        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="beehive-fog-node")
        self.client.tls_set(
            ca_certs=str(cert_dir / "AmazonRootCA1.pem"),
            certfile=str(cert_dir / "device_cert.pem.crt"),
            keyfile=str(cert_dir / "device_private.pem.key"),
            tls_version=ssl.PROTOCOL_TLS_CLIENT,
        )
        self.client.connect(endpoint, port, keepalive=60)
        self.client.loop_start()
        time.sleep(1)

    def send(self, payload: dict) -> bool:
        info = self.client.publish(self.topic, self._json.dumps(payload), qos=1)
        info.wait_for_publish(timeout=10)
        return info.is_published()
