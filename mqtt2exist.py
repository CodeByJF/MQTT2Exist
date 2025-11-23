#!/usr/bin/env python3
"""
MQTT 2 Exist.io bridge for OpenScaleSync measurements.

Reads weight/body fat metrics from an MQTT topic and forwards them to Exist.io
as attribute updates.

Configuration is done via environment variables (ideally loaded from a .env file).

Required env vars:
- EXIST_TOKEN      : Exist.io OAuth2 bearer token with write scope

Recommended env vars:
- MQTT_HOST        : MQTT broker hostname (default: localhost)
- MQTT_PORT        : MQTT broker port (default: 1883)
- MQTT_TOPIC       : MQTT topic to subscribe to (default: openScaleSync/measurements/last)
- MQTT_USERNAME    : MQTT username (optional)
- MQTT_PASSWORD    : MQTT password (optional)
- MQTT_TLS         : "true" to enable TLS, anything else = plain (default: false)
- ATTR_WEIGHT      : Exist attribute name for weight (default: weight)
- ATTR_FAT         : Exist attribute name for body fat (default: body_fat)
- LOCAL_TZ         : Local timezone for date conversion (default: America/Toronto)
- LOG_LEVEL        : Logging level (default: INFO)
"""

import os
import json
import logging
from datetime import datetime
from typing import Optional, Tuple

import requests
from dotenv import load_dotenv

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except ImportError:
    from backports.zoneinfo import ZoneInfo  # Python 3.8 and older

import paho.mqtt.client as mqtt

# ---------- Load .env ----------
load_dotenv()

# ---------- Config via env ----------
MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "openScaleSync/measurements/last")
MQTT_USERNAME = os.getenv("MQTT_USERNAME")  # optional
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")  # optional
MQTT_TLS = os.getenv("MQTT_TLS", "false").lower() == "true"

EXIST_TOKEN = os.getenv("EXIST_TOKEN")  # OAuth2 Bearer with write scope
ATTR_WEIGHT = os.getenv("ATTR_WEIGHT", "weight")
ATTR_FAT = os.getenv("ATTR_FAT", "body_fat")
LOCAL_TZ = os.getenv("LOCAL_TZ", "America/Toronto")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

EXIST_UPDATE_URL = "https://exist.io/api/2/attributes/update/"

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


# ---------- Helpers ----------

def lb_to_kg(lb: float) -> float:
    return float(lb) * 0.45359237


def to_local_date(date_like) -> str:
    """
    Return YYYY-MM-DD in LOCAL_TZ from:
    - ISO string with or without timezone, e.g. "2025-11-04T07:11-0500"
    - Unix seconds (int/float)
    - None -> now in LOCAL_TZ
    """
    tz = ZoneInfo(LOCAL_TZ)

    if date_like is None:
        return datetime.now(tz).strftime("%Y-%m-%d")

    if isinstance(date_like, (int, float)):
        return datetime.fromtimestamp(float(date_like), tz).strftime("%Y-%m-%d")

    # ISO string possibly like 2025-11-04T07:11-0500 (no colon in offset)
    s = str(date_like)
    if len(s) >= 5 and (s[-5] in "+-") and (s[-3] != ":"):
        # Insert colon in timezone offset for fromisoformat
        s = s[:-2] + ":" + s[-2:]

    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz)

    return dt.astimezone(tz).strftime("%Y-%m-%d")


def parse_payload(payload: str) -> Tuple[str, float, Optional[float]]:
    """
    Expected OpenScaleSync-style payload subsets:
    {
      "date": "2025-11-04T07:11-0500",  # or "timestamp"/"ts" (unix)
      "weight": 84.75,                  # or "weight_kg"
      "unit": "kg"                      # or "lb"
      "fat": 24.22                      # percent (0–100)
    }

    Returns:
      (date_str, weight_kg, fat_fraction_or_none)
      where:
        - date_str is "YYYY-MM-DD" in LOCAL_TZ
        - weight_kg is rounded to 2 decimals
        - fat_fraction_or_none is e.g. 0.24 (24%) rounded to 2 decimals, or None
    """
    d = json.loads(payload)

    # Date
    date_str = to_local_date(
        d.get("date") or d.get("timestamp") or d.get("ts")
    )

    # Weight
    weight = d.get("weight_kg", d.get("weight"))
    if weight is None:
        raise ValueError("Payload missing 'weight' or 'weight_kg'")

    unit = (d.get("unit") or "kg").lower()
    if unit == "kg":
        weight_kg = float(weight)
    else:
        weight_kg = lb_to_kg(float(weight))

    # Body fat
    fat_pct_val = d.get("fat")
    if fat_pct_val is not None:
        # Convert from percentage to fraction (e.g. 24.22 → 0.2422)
        fat_fraction = float(fat_pct_val) / 100.0
        fat_fraction = round(fat_fraction, 2)
    else:
        fat_fraction = None

    return date_str, round(weight_kg, 2), fat_fraction


def post_exist(date_str: str, weight_kg: float, fat_fraction: Optional[float]) -> None:
    if not EXIST_TOKEN:
        raise RuntimeError("EXIST_TOKEN is not set. Please export it or define it in your .env.")

    updates = [
        {"name": ATTR_WEIGHT, "date": date_str, "value": weight_kg}
    ]

    if fat_fraction is not None:
        updates.append({"name": ATTR_FAT, "date": date_str, "value": fat_fraction})

    logging.info("Posting to Exist.io: %s", updates)

    r = requests.post(
        EXIST_UPDATE_URL,
        headers={
            "Authorization": f"Bearer {EXIST_TOKEN}",
            "Content-Type": "application/json",
        },
        json=updates,
        timeout=30,
    )

    if r.status_code not in (200, 202):
        raise RuntimeError(f"Exist update failed {r.status_code}: {r.text[:300]}")

    try:
        logging.info("Exist response: %s", r.json())
    except Exception:
        logging.info("Exist response (non-JSON): %s", r.text[:300])


# ---------- MQTT Callbacks ----------

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        logging.info("Connected to MQTT %s:%s; subscribing to %s", MQTT_HOST, MQTT_PORT, MQTT_TOPIC)
        client.subscribe(MQTT_TOPIC, qos=1)
    else:
        logging.error("MQTT connect failed with rc=%s", rc)


def on_message(client, userdata, msg):
    payload = msg.payload.decode("utf-8", errors="replace")
    logging.info("MQTT message on %s: %s", msg.topic, payload)

    try:
        date_str, weight_kg, fat_fraction = parse_payload(payload)
        logging.info(
            "Parsed payload -> date=%s, weight_kg=%.2f, fat=%s",
            date_str,
            weight_kg,
            "None" if fat_fraction is None else f"{fat_fraction:.2f}",
        )
        post_exist(date_str, weight_kg, fat_fraction)
    except Exception as e:
        logging.error("Processing failed: %s", e, exc_info=True)


# ---------- Main ----------

def main():
    if not EXIST_TOKEN:
        raise RuntimeError("EXIST_TOKEN is not configured. Set it in .env or environment variables.")

    client = mqtt.Client(protocol=mqtt.MQTTv5)

    if MQTT_USERNAME:
        logging.info("Using MQTT username authentication")
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD or None)

    if MQTT_TLS:
        logging.info("Enabling MQTT TLS")
        client.tls_set()

    client.on_connect = on_connect
    client.on_message = on_message

    logging.info("Connecting to MQTT broker %s:%s (TLS=%s)", MQTT_HOST, MQTT_PORT, MQTT_TLS)
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_forever()


if __name__ == "__main__":
    main()
