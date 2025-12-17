import json
import random
import time
from datetime import datetime, timezone
from confluent_kafka import Producer

BOOTSTRAP = "localhost:9092"
TOPIC = "iot_smartech"

devices = [
    ("sensor-temp-001", "A", 2, "temperature", "°C", (-10, 40)),
    ("sensor-temp-003", "B", 3, "temperature", "°C", (-10, 40)),
    ("sensor-hum-002",  "B", 1, "humidity",    "%",  (0, 100)),
    ("sensor-co2-020",  "A", 2, "co2",         "ppm",(350, 2000)),
]

p = Producer({"bootstrap.servers": BOOTSTRAP})

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    # else: print(f"✅ Sent to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

while True:
    dev, bld, flr, typ, unit, (lo, hi) = random.choice(devices)
    payload = {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "device_id": dev,
        "building": bld,
        "floor": flr,
        "type": typ,
        "value": round(random.uniform(lo, hi), 2) if typ != "co2" else int(random.uniform(lo, hi)),
        "unit": unit
    }

    key = dev.encode("utf-8")
    val = json.dumps(payload).encode("utf-8")

    p.produce(TOPIC, key=key, value=val, callback=delivery_report)
    p.poll(0)
    time.sleep(0.3)
