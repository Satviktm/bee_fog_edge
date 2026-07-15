# Precision Beekeeping: Fog-Enabled Apiary Monitoring System

Fog and Edge Computing (H9FECC) — NCI MSc Cloud Computing

## Structure
- `sensor_layer/` — simulates 5 sensor types across multiple hives, publishing to local MQTT
- `fog_layer/` — subscribes locally, filters/detects anomalies/aggregates/batches/buffers, dispatches toward AWS (currently mocked, IoT Core wiring in progress)

See each folder's own README.md for setup and run instructions.
