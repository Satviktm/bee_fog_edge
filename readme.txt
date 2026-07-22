================================================================
 PRECISION BEEKEEPING: FOG-ENABLED APIARY MONITORING SYSTEM
 Fog and Edge Computing (H9FECC) - NCI MSc Cloud Computing
================================================================

GitHub repository:
  https://github.com/Satviktm/bee_fog_edge


----------------------------------------------------------------
1. PROJECT OVERVIEW
----------------------------------------------------------------
Simulated precision-beekeeping IoT system for a single apiary
(apiary-01) containing three hives. Five sensor types per hive
publish over local MQTT to a fog node, which filters readings,
independently detects colony anomalies (swarm departure, colony
failure, mould risk, queenlessness) against its own rolling
baseline, batches and durably buffers data, then dispatches to
AWS IoT Core over MQTT+TLS. From there, AWS Rules Engine fans
each message out to three destinations: DynamoDB (hot storage),
Lambda + SNS (real-time alerting), and S3 via Kinesis Firehose
(long-term archive). A browser dashboard, served from S3 static
hosting, polls a serverless API Gateway + Lambda read path every
30 seconds.


----------------------------------------------------------------
2. REPOSITORY STRUCTURE
----------------------------------------------------------------
sensor_layer/
    config.json          - sensor topology, sampling intervals,
                            anomaly-injection parameters
    generators.py         - synthetic sensor value generation
                            (diurnal cycle + noise + anomaly)
    sensor_node.py         - main script, one MQTT-publishing
                            thread per (hive, sensor type)
    requirements.txt
    README.md             - detailed sensor layer notes

fog_layer/
    config.json           - filter thresholds, anomaly rules,
                            batching, buffering, retry, and AWS
                            IoT Core connection settings
    state.py              - per-source rolling history/state
    processing.py          - filtering and independent anomaly
                            detection logic
    outbox.py              - SQLite-backed durable send queue
    uploader.py             - pluggable transport (Mock / IoT Core)
    fog_node.py             - main script: subscribe, process,
                            batch, buffer, dispatch, watchdog
    requirements.txt
    README.md              - detailed fog layer notes
    certs/                 - AWS IoT device certificates
                            (NOT included in repo - see section 4)

backend/lambda/
    beehive-alert-processor/lambda_function.py
                            - triggered by IoT Rules Engine for
                            kind="alert" payloads; writes to
                            BeehiveAlertLog and publishes to SNS
    beehive-api-read/lambda_function.py
                            - API Gateway read path backing the
                            dashboard's 5 endpoints

dashboard/
    index.html              - single-file static dashboard
                            (HTML/CSS/JS + Chart.js via CDN),
                            polls the API every 30s


----------------------------------------------------------------
3. PREREQUISITES
----------------------------------------------------------------
- An AWS account with IoT Core, Lambda, DynamoDB, SNS, Kinesis
  Firehose, S3, and API Gateway access (developed and tested
  against an AWS Academy Learner Lab, using the pre-configured
  LabRole for every IAM role - no roles were created manually)
- One EC2 instance (Ubuntu 22.04/24.04, t2.micro/t3.micro is
  sufficient) to run the sensor layer and fog node
- Python 3.10+
- Mosquitto MQTT broker (local, on the EC2 instance)


----------------------------------------------------------------
4. AWS RESOURCES USED (already provisioned for this submission)
----------------------------------------------------------------
IoT Core:
  Thing name        beehive-apiary-01
  Policy             beehive-iot-policy
  Endpoint            a1or11j3zzyigq-ats.iot.us-east-1.amazonaws.com
  Rules               beehive_store_batch_to_dynamodb
                     beehive_trigger_alert_lambda
                     beehive_archive_to_s3

DynamoDB:
  BeehiveHiveData     PK: hive_id (String)  SK: ts (Number)
  BeehiveAlertLog     PK: hive_id (String)  SK: ts (String)
                      NOTE: the two tables use different types
                      for their sort key by design/oversight -
                      see section 6, Known Limitations.

Lambda:
  beehive-alert-processor   (Execution role: LabRole)
  beehive-api-read          (Execution role: LabRole)

SNS:
  Topic               beehive-alerts
  Delivery            Email

Kinesis Firehose:
  Stream               beehive-sensor-firehose
  Destination           S3, prefix apiary-data/

S3:
  beehive-data-lake-satviktm      - Firehose archive destination
  beehive-dashboard-satviktm       - static website hosting for
                                     dashboard/index.html

API Gateway:
  beehive-dashboard-api  (HTTP API)
  Routes: GET /apiaries
          GET /apiary/{apiary_id}/hives
          GET /hive/{hive_id}/latest
          GET /hive/{hive_id}/history?minutes=N
          GET /alerts/recent?limit=N

To rebuild these resources from scratch in a fresh AWS account,
follow the setup steps in fog_layer/README.md (IoT Core Thing/
certs/policy) and recreate the DynamoDB tables, Lambda functions,
SNS topic, Firehose stream, S3 buckets and API Gateway routes
using the console, matching the names and configuration above.


----------------------------------------------------------------
5. SETUP AND RUN INSTRUCTIONS
----------------------------------------------------------------

Step 1 - Clone the repository onto your EC2 instance:
    git clone https://github.com/Satviktm/bee_fog_edge.git
    cd bee_fog_edge

Step 2 - Install and start the local MQTT broker:
    sudo apt-get update
    sudo apt-get install -y mosquitto mosquitto-clients
    sudo systemctl start mosquitto

Step 3 - Set up and run the sensor layer:
    cd sensor_layer
    python3 -m venv sensor-env
    source sensor-env/bin/activate
    pip install -r requirements.txt
    python3 sensor_node.py
  (leave running in this terminal)

Step 4 - AWS IoT Core certificates:
  To run the fog node against your own AWS account,
  create an IoT Thing, policy, and certificate as described in
  fog_layer/README.md, then place the four downloaded files in:
    fog_layer/certs/
        AmazonRootCA1.pem
        device_cert.pem.crt
        device_private.pem.key
        device_public.pem.key
  Update fog_layer/config.json -> iot_core.endpoint with your
  own IoT Core device data endpoint.

Step 5 - Set up and run the fog node (in a second terminal):
    cd fog_layer
    python3 -m venv fog-env
    source fog-env/bin/activate
    pip install -r requirements.txt
    python3 fog_node.py
  (leave running in this terminal)

Step 6 - Confirm data is flowing:
  - AWS Console -> IoT Core -> Test -> MQTT test client ->
    subscribe to apiary/# to see live messages arriving
  - AWS Console -> DynamoDB -> BeehiveHiveData -> Explore table
    items, to confirm batches are being stored
  - Trigger a colony anomaly and confirm an email alert arrives
    (see fog_layer/README.md for how to temporarily raise the
    anomaly probability for testing)

Step 7 - View the dashboard:
  Open the S3 static website endpoint for the
  beehive-dashboard-satviktm bucket in a browser. The dashboard
  polls the API every 30 seconds and requires no further setup.


----------------------------------------------------------------
6. KNOWN LIMITATIONS
----------------------------------------------------------------
- Single apiary scope: fog node payloads carry hive_id but not
  apiary_id, so the current implementation assumes one apiary.
  Multi-apiary support would require adding apiary_id to every
  batch/alert payload.

- Sort key type inconsistency: BeehiveHiveData.ts is a DynamoDB
  Number, while BeehiveAlertLog.ts is a String (the latter was
  originally created as String and left as-is once a type
  mismatch was found and worked around in the Lambda code,
  rather than recreating the table). Both API read paths handle
  this correctly, but a production version should standardise
  on one type.

- No producer/device identity check at the MQTT layer: the fog
  node trusts hive_id purely from the message payload, with no
  verification of which physical device published it. During
  testing, running two separate sensor-simulation processes
  that both claimed the same hive_id caused the fog node's
  in-memory baseline state to become briefly contaminated (see
  report, Reflection section, for a full discussion). A
  production system would authenticate each physical sensor
  device independently, e.g. per-device MQTT credentials.

- Sensor sampling intervals (60s / 30s / 300s) are significantly
  faster than realistic battery-powered hive sensor hardware
  (e.g. commercial products typically sample hourly to conserve
  battery). Faster intervals were used deliberately to keep
  system behaviour observable within development, testing, and
  demo time constraints; the sampling interval is fully
  configurable via sensor_layer/config.json or environment
  variable override with no code changes required.


----------------------------------------------------------------
7. AUTHOR
----------------------------------------------------------------
Satvik T M
NCI MSc Cloud Computing
