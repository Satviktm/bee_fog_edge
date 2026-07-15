"""
processing.py

Two independent responsibilities, kept deliberately separate:

  1. filter_reading()  -> "Is this a TRUSTWORTHY reading, or is the
                           sensor lying / broken?" Catches out-of-range
                           values, physically-impossible jumps, and
                           stuck/frozen sensors. Invalid readings are
                           dropped and never reach aggregation.

  2. check_anomaly()   -> "Given a TRUSTED reading, does it represent a
                           real colony event (swarm, queenlessness,
                           etc.)?" This is computed ENTIRELY from the
                           fog node's own short rolling baseline of
                           recent valid readings. It never reads or
                           trusts the sensor's own
                           `_simulated_anomaly_injected` field -- that
                           field only exists in the simulator for
                           testing visibility.
"""

import statistics


# ---------------------------------------------------------------- filtering

def filter_reading(sensor_type, field_name, value, now, field_state,
                    range_cfg, rate_cfg, stuck_at_len):
    """
    Returns (is_valid: bool, reason: str | None).
    On success, DOES NOT mutate field_state -- caller updates state
    only for readings it decides to keep, so a rejected reading never
    corrupts the rate-of-change / stuck-at baselines.
    """
    # 1. Range check -- physically impossible values
    bounds = range_cfg.get(sensor_type, {}).get(field_name)
    if bounds and not (bounds["min"] <= value <= bounds["max"]):
        return False, "out_of_range"

    # 2. Rate-of-change check -- impossible jump since last reading
    max_rate = rate_cfg.get(sensor_type, {}).get(field_name)
    if max_rate is not None and field_state.last_value is not None and field_state.last_time is not None:
        dt = now - field_state.last_time
        if dt > 0:
            rate = abs(value - field_state.last_value) / dt
            if rate > max_rate:
                return False, "rate_of_change_exceeded"

    # 3. Stuck-at check -- sensor frozen on one value (only detectable
    #    once range/rate checks already passed, since a frozen sensor
    #    reports a constant, in-range, zero-rate-of-change value)
    probe = list(field_state.recent_values) + [value]
    if len(probe) >= stuck_at_len and all(v == value for v in probe[-stuck_at_len:]):
        return False, "stuck_at_value"

    return True, None


# ---------------------------------------------------------------- anomaly detection

def check_anomaly(value, field_state, rule_cfg):
    """
    Compares `value` against the mean of this field's short rolling
    baseline (recent VALID readings only, maintained by the fog node
    itself). Returns an anomaly label string if it fires, else None.

    Baseline is updated by the caller AFTER this check, so the anomaly
    decision always uses history *prior to* the current reading.
    """
    baseline_window = rule_cfg["baseline_window"]
    if len(field_state.baseline_values) < baseline_window:
        return None  # not enough history yet to judge what's "normal"

    baseline_mean = statistics.mean(field_state.baseline_values)
    if baseline_mean == 0:
        return None  # avoid divide-by-zero; degenerate case

    change_fraction = (value - baseline_mean) / baseline_mean
    direction = rule_cfg["direction"]
    threshold = rule_cfg["fraction_threshold"]

    if direction == "drop" and change_fraction <= -threshold:
        return rule_cfg["label"]
    if direction == "rise" and change_fraction >= threshold:
        return rule_cfg["label"]
    return None
