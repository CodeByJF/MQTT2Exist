#!/usr/bin/env python3
import os, json, logging, requests
from datetime import datetime
from typing import Optional
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except ImportError:
    from backports.zoneinfo import ZoneInfo  # Python 3.8 and older
import paho.mqtt.client as mqtt

# ---------- Config via env ----------
MQTT_HOST      = "localhost"
MQTT_PORT      = int("1883")
MQTT_TOPIC     = "openScaleSync/measurements/last"
MQTT_USERNAME  = os.getenv("MQTT_USER")
MQTT_PASSWORD  = os.getenv("MQTT_PASSWORD")
MQTT_TLS       = os.getenv("MQTT_TLS", "false").lower() == "true"

EXIST_TOKEN    = os.getenv("EXIST_TOKEN") # OAuth2 Bearer with write scope
ATTR_WEIGHT    = "weight"      # set to your Exist attribute *name*
ATTR_FAT       = "body_fat"      # set to your Exist attribute *name*
LOCAL_TZ       = "America/Toronto"
LOG_LEVEL      = "INFO"

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s [%(levelname)s] %(message)s")
EXIST_UPDATE_URL = "https://exist.io/api/2/attributes/update/"

def lb_to_kg(lb): 
    return float(lb) * 0.45359237

def to_local_date(date_like):
    """Return YYYY-MM-DD in LOCAL_TZ from ISO string with or without timezone, or unix seconds."""
    tz = ZoneInfo(LOCAL_TZ)
    if date_like is None:
        return datetime.now(tz).strftime("%Y-%m-%d")
    if isinstance(date_like, (int, float)):
        return datetime.fromtimestamp(float(date_like), tz).strftime("%Y-%m-%d")
    # ISO string possibly like 2025-11-04T07:11-0500 (no colon in offset)
    s = str(date_like)
    if len(s) >= 5 and (s[-5] in "+-") and (s[-3] != ":"):
        s = s[:-2] + ":" + s[-2:]
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz)
    return dt.astimezone(ZoneInfo(LOCAL_TZ)).strftime("%Y-%m-%d")

def parse_payload(payload: str):
    """
    Expected OpenScaleSync-style payload subsets:
    {
      "date": "2025-11-04T07:11-0500",
      "weight": 84.75,           # or "weight_kg"
      "unit": "kg"               # or "lb"
      "fat": 24.22               # percent
    }
    """
    d = json.loads(payload)

    date_str = to_local_date(d.get("date") or d.get("timestamp") or d.get("ts"))

    weight = d.get("weight_kg", d.get("weight"))
    if weight is None:
        raise ValueError("Payload missing 'weight' or 'weight_kg'")
    unit = (d.get("unit") or "kg").lower()
    weight_kg = float(weight) if unit == "kg" else lb_to_kg(weight)

    fat_pct = d.get("fat")
    fat_pct = (fat_pct/100)
    fat_pct = None if fat_pct is None else float(fat_pct)

    return date_str, round(weight_kg, 2), (None if fat_pct is None else round(fat_pct, 2))

def post_exist(date_str: str, weight_kg: float, fat_pct: Optional[float]):
    if not EXIST_TOKEN:
        raise RuntimeError("Set EXIST_TOKEN (OAuth2 Bearer with write scope).")
    updates = [{"name": ATTR_WEIGHT, "date": date_str, "value": weight_kg}]
    if fat_pct is not None:
        updates.append({"name": ATTR_FAT, "date": date_str, "value": fat_pct})

    r = requests.post(
        EXIST_UPDATE_URL,
        headers={"Authorization": f"Bearer {EXIST_TOKEN}", "Content-Type": "application/json"},
        json=updates, timeout=30
    )
    if r.status_code not in (200, 202):
        raise RuntimeError(f"Exist update failed {r.status_code}: {r.text[:300]}")
    logging.info("Exist response: %s", r.json())

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        logging.info("Connected to MQTT; subscribing %s", MQTT_TOPIC)
        client.subscribe(MQTT_TOPIC, qos=1)
    else:
        logging.error("MQTT connect rc=%s", rc)

def on_message(client, userdata, msg):
    payload = msg.payload.decode("utf-8", errors="replace")
    logging.info("MQTT %s: %s", msg.topic, payload)
    try:
        date_str, weight_kg, fat_pct = parse_payload(payload)
        post_exist(date_str, weight_kg, fat_pct)
    except Exception as e:
        logging.error("Processing failed: %s", e)

def main():
    client = mqtt.Client(protocol=mqtt.MQTTv5)
    if MQTT_USERNAME:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD or None)
    if MQTT_TLS:
        client.tls_set()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_forever()

if __name__ == "__main__":
    main()
