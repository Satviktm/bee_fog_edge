import json, time, boto3
from decimal import Decimal

dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
sns = boto3.client("sns", region_name="us-east-1")
alert_tbl = dynamodb.Table("BeehiveAlertLog")

SNS_ARN = "arn:aws:sns:us-east-1:021603588049:beehive-alerts"

def lambda_handler(event, context):
    hive_id = event.get("hive_id", "unknown")
    sensor_type = event.get("sensor_type", "unknown")
    field = event.get("field")
    value = event.get("value")
    label = event.get("label", "unknown")
    event_timestamp = event.get("event_timestamp", "unknown")
    detected_at = event.get("detected_at", "unknown")
    baseline_mean = event.get("baseline_mean")  # None for stuck-sensor / offline alerts

    print(f"[ALERT RECEIVED] hive={hive_id} sensor={sensor_type} field={field} value={value} label={label} baseline={baseline_mean}")

    ts_ms = int(time.time() * 1000)

    item = {
        "hive_id": hive_id,
        "ts": str(ts_ms),
        "sensor_type": sensor_type,
        "label": label,
        "event_timestamp": event_timestamp,
        "detected_at": detected_at,
    }
    if field is not None:
        item["field"] = field
    if value is not None:
        item["value"] = Decimal(str(value))
    if baseline_mean is not None:
        item["baseline_mean"] = Decimal(str(baseline_mean))

    alert_tbl.put_item(Item=item)

    if baseline_mean is not None and value is not None and baseline_mean != 0:
        pct = (value - baseline_mean) / baseline_mean * 100
        comparison_line = f"Value: {value} (baseline ~{baseline_mean:.1f}, {pct:+.0f}%)\n"
    else:
        comparison_line = f"Value: {value}\n"

    sns.publish(
        TopicArn=SNS_ARN,
        Subject=f"Beehive Alert: {hive_id} - {label}",
        Message=(
            f"Hive: {hive_id}\n"
            f"Sensor: {sensor_type}\n"
            f"Field: {field}\n"
            f"{comparison_line}"
            f"Alert type: {label}\n"
            f"Event occurred at: {event_timestamp}\n"
            f"Detected at (fog node): {detected_at}\n"
        ),
    )

    print(f"[ALERT] Written to BeehiveAlertLog + SNS published")
    return {"statusCode": 200, "hive_id": hive_id, "label": label}
