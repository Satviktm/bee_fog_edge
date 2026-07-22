import json
import time
from decimal import Decimal
import boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
hive_data_tbl = dynamodb.Table("BeehiveHiveData")
alert_log_tbl = dynamodb.Table("BeehiveAlertLog")

APIARY_ID = "apiary-01"
KNOWN_HIVES = ["hive-01", "hive-02", "hive-03"]  # matches sensor_layer/config.json


def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj) if obj % 1 else int(obj)
    raise TypeError


def respond(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps(body, default=decimal_default),
    }


def get_apiaries():
    return respond(200, {"apiaries": [{"apiary_id": APIARY_ID, "hive_count": len(KNOWN_HIVES)}]})


def get_hives(apiary_id):
    hives = []
    for hive_id in KNOWN_HIVES:
        resp = hive_data_tbl.query(
            KeyConditionExpression=Key("hive_id").eq(hive_id),
            ScanIndexForward=False,
            Limit=1,
        )
        items = resp.get("Items", [])
        hives.append({
            "hive_id": hive_id,
            "latest_reading": items[0] if items else None,
        })
    return respond(200, {"apiary_id": apiary_id, "hives": hives})


def get_hive_latest(hive_id):
    resp = hive_data_tbl.query(
        KeyConditionExpression=Key("hive_id").eq(hive_id),
        ScanIndexForward=False,
        Limit=10,
    )
    return respond(200, {"hive_id": hive_id, "latest": resp.get("Items", [])})


def get_hive_history(hive_id, minutes):
    cutoff_ms = int((time.time() - minutes * 60) * 1000)
    resp = hive_data_tbl.query(
        KeyConditionExpression=Key("hive_id").eq(hive_id) & Key("ts").gte(cutoff_ms),
        ScanIndexForward=True,
    )
    return respond(200, {"hive_id": hive_id, "minutes": minutes, "history": resp.get("Items", [])})


def get_alerts_recent(limit):
    resp = alert_log_tbl.scan()
    items = resp.get("Items", [])
    items.sort(key=lambda i: int(i.get("ts", "0")), reverse=True)
    return respond(200, {"alerts": items[:limit]})


def lambda_handler(event, context):
    route_key = event.get("routeKey", "")
    path_params = event.get("pathParameters") or {}
    query_params = event.get("queryStringParameters") or {}

    try:
        if route_key == "GET /apiaries":
            return get_apiaries()
        elif route_key == "GET /apiary/{apiary_id}/hives":
            return get_hives(path_params["apiary_id"])
        elif route_key == "GET /hive/{hive_id}/latest":
            return get_hive_latest(path_params["hive_id"])
        elif route_key == "GET /hive/{hive_id}/history":
            minutes = int(query_params.get("minutes", 60))
            return get_hive_history(path_params["hive_id"], minutes)
        elif route_key == "GET /alerts/recent":
            limit = int(query_params.get("limit", 10))
            return get_alerts_recent(limit)
        else:
            return respond(404, {"error": f"No handler for route: {route_key}"})
    except Exception as e:
        print(f"[ERROR] {route_key}: {e}")
        return respond(500, {"error": str(e)})
