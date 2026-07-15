"""
state.py

Holds the small amount of memory the fog node needs per
(hive_id, sensor_type) source in order to do its job:

  - last_value / last_time   -> for the rate-of-change filter
  - recent_values (deque)    -> for the stuck-at-same-value filter
  - baseline_values (deque)  -> for anomaly detection (compare current
                                 reading against a short recent baseline,
                                 NOT the sensor's own self-reported flag)
  - batch buffer             -> raw valid readings accumulating toward
                                 the next aggregated batch
  - last_seen_time           -> for the heartbeat/offline watchdog
"""

from collections import deque, defaultdict


class FieldState:
    """Tracks history for ONE field of ONE sensor on ONE hive,
    e.g. hive-02 / weight / value."""

    def __init__(self, stuck_at_len, baseline_len):
        self.last_value = None
        self.last_time = None
        self.recent_values = deque(maxlen=stuck_at_len)
        self.baseline_values = deque(maxlen=baseline_len)
        self.fault_flagged = False  # so a stuck sensor only raises ONE alert, not one per tick


class SourceState:
    """Tracks everything for ONE (hive_id, sensor_type) source, which
    may have multiple fields (e.g. acoustic has dominant_freq_hz AND
    rms_amplitude)."""

    def __init__(self, stuck_at_len, baseline_len):
        self.fields = defaultdict(lambda: FieldState(stuck_at_len, baseline_len))
        self.batch_buffer = []          # list of valid reading dicts awaiting a batch emit
        self.last_seen_time = None      # updated on every message, valid or not
        self.offline_flagged = False    # so the watchdog only alerts once per outage


class FogState:
    """Top-level container: one SourceState per (hive_id, sensor_type)."""

    def __init__(self, stuck_at_len, baseline_len):
        self._stuck_at_len = stuck_at_len
        self._baseline_len = baseline_len
        self._sources = {}

    def get(self, hive_id, sensor_type):
        key = (hive_id, sensor_type)
        if key not in self._sources:
            self._sources[key] = SourceState(self._stuck_at_len, self._baseline_len)
        return self._sources[key]

    def all_sources(self):
        return dict(self._sources)
