import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SLEEP = float(os.getenv("PRODUCER_SLEEP_SECONDS", "0.2"))

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda v: v.encode("utf-8"),
)

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()

def emit_order():
    order_id = str(uuid.uuid4())
    evt = {
        "event_type": "order_created",
        "event_ts": utc_now_iso(),
        "order_id": order_id,
        "customer_id": f"C{random.randint(1000, 9999)}",
        "amount": round(random.uniform(5, 250), 2),
        "currency": "USD",
    }
    producer.send("orders", key=order_id, value=evt)

    # Emit payment for most orders
    if random.random() < 0.85:
        payment_id = str(uuid.uuid4())
        status = "PAID" if random.random() < 0.95 else "FAILED"
        pay = {
            "event_type": "payment",
            "event_ts": utc_now_iso(),
            "payment_id": payment_id,
            "order_id": order_id,
            "status": status,
            "amount": evt["amount"],
        }
        producer.send("payments", key=payment_id, value=pay)

def main():
    print(f"Producer started. bootstrap={BOOTSTRAP}")
    while True:
        emit_order()
        producer.flush()
        time.sleep(SLEEP)

if __name__ == "__main__":
    main()
