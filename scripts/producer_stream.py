import os
import json
import time
import uuid
import random
from datetime import datetime, timezone
from faker import Faker

OUTBOX = os.path.join(os.path.dirname(__file__), "..", "outbox")
os.makedirs(OUTBOX, exist_ok=True)
fake = Faker()

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def make_click_event():
    return {
        "schema_name":"web_clickstream","schema_version":"v1","source_system":"web",
        "event_id": str(uuid.uuid4()),
        "user_id": f"user-{random.randint(1,1000)}",
        "session_id": f"sess-{uuid.uuid4().hex[:8]}",
        "event_type": random.choice(["page_view","product_view","click"]),
        "event_timestamp": now_iso(),
        "page_url": f"/product/sku-{random.randint(1,500)}",
        "user_agent": fake.user_agent(),
        "device_type": random.choice(["desktop","mobile"]),
        "geo_country": fake.country_code(),
        "properties": {"product_id": f"sku-{random.randint(1,500)}"},
        "ingestion_timestamp": now_iso()
    }

def append_event_to_file(ev, filename="clickstream.jsonl"):
    path = os.path.join(OUTBOX, filename)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(ev) + "\n")

def run(rate_per_sec=1, total_events=30):
    interval = 1.0 / rate_per_sec
    for i in range(total_events):
        ev = make_click_event()
        append_event_to_file(ev)
        print(f"wrote {i+1}/{total_events} event_id={ev['event_id']}")
        time.sleep(interval)

if __name__ == "__main__":
    # default: 1 event/sec, 60 events total (adjust)
    run(rate_per_sec=1, total_events=60)

    # Optional: upload to S3 (careful with AWS costs)
    # import boto3
    # s3 = boto3.client("s3")
    # s3.upload_file(os.path.join(OUTBOX,'clickstream.jsonl'), 'your-bucket-name', 'ingest/web/clickstream.jsonl')
