"""
generators.py

Produces realistic-looking synthetic sensor values for the beehive
simulation. Each individual sensor "field" (e.g. weight.value,
acoustic.dominant_freq_hz) gets its own FieldGenerator instance.

Each generator combines three components:
    1. Diurnal baseline  -> a sine wave over a 24h period, so values
                             naturally rise/fall through the day rather
                             than being flat.
    2. Gaussian noise     -> small per-reading jitter, mimicking real
                             sensor imprecision.
    3. Anomaly injection  -> a rare, probabilistic, PERSISTENT step
                             change (e.g. a sudden weight drop that
                             stays down, simulating a swarm departure
                             or colony failure). This is what lets the
                             fog node's anomaly-detection logic be
                             exercised end-to-end during testing/demo.
"""

import math
import random
import time


class FieldGenerator:
    def __init__(self, field_name, cfg, rng=None):
        self.field_name = field_name
        self.mean = cfg["baseline_mean"]
        self.amplitude = cfg.get("diurnal_amplitude", 0.0)
        self.period_sec = cfg.get("diurnal_period_sec", 86400)
        self.noise_std = cfg.get("noise_std", 0.0)

        anomaly_cfg = cfg.get("anomaly", {})
        self.anomaly_enabled = anomaly_cfg.get("enabled", False)
        self.anomaly_prob_per_hour = anomaly_cfg.get("probability_per_hour", 0.0)
        self.anomaly_magnitude_fraction = anomaly_cfg.get("magnitude_fraction", 0.0)
        self.anomaly_persistent = anomaly_cfg.get("persistent", True)
        self.anomaly_label = anomaly_cfg.get("label", "anomaly")

        self.rng = rng or random.Random()
        self._start_time = time.time()
        self._offset = 0.0          # permanent shift applied once an anomaly fires
        self._anomaly_active = False

    def _diurnal_component(self, now):
        elapsed = now - self._start_time
        return self.amplitude * math.sin(2 * math.pi * elapsed / self.period_sec)

    def _maybe_trigger_anomaly(self, dt_seconds):
        """Roll the dice for an anomaly firing on this tick.
        Returns the anomaly label if one just fired, else None."""
        if not self.anomaly_enabled or self._anomaly_active:
            return None

        # Convert an hourly probability into a probability for this
        # specific tick, given how much time has actually elapsed.
        prob_this_tick = self.anomaly_prob_per_hour * (dt_seconds / 3600.0)
        if self.rng.random() < prob_this_tick:
            self._offset += self.mean * self.anomaly_magnitude_fraction
            if self.anomaly_persistent:
                self._anomaly_active = True  # step change stays; won't re-fire
            return self.anomaly_label
        return None

    def next_value(self, dt_seconds):
        """Compute the next reading. dt_seconds is the time since the
        previous reading for THIS field (used to scale anomaly
        probability correctly regardless of sampling interval)."""
        fired_label = self._maybe_trigger_anomaly(dt_seconds)

        now = time.time()
        value = (
            self.mean
            + self._diurnal_component(now)
            + self._offset
            + self.rng.gauss(0, self.noise_std)
        )
        return round(value, 3), fired_label


def build_generators_for_hive(sensor_cfg, seed=None):
    """
    Given the 'sensors' block of config.json, build one FieldGenerator
    per (sensor_type, field) pair for a single hive. Each hive gets its
    own independent set of generators (and its own random seed offset)
    so hives don't all misbehave in lockstep.
    """
    rng = random.Random(seed)
    generators = {}
    for sensor_type, sensor_def in sensor_cfg.items():
        generators[sensor_type] = {}
        for field_name, field_cfg in sensor_def["fields"].items():
            generators[sensor_type][field_name] = FieldGenerator(field_name, field_cfg, rng=rng)
    return generators
