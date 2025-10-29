import json
import os
import uuid
from faker import Faker
from datetime import datetime, timezone
import random

BASE_DIR = os.path.join(os.path.dirname(__file__), "..", "schemas")
OUT_DIR = os.path.join(os.path.dirname(__file__), "..", "generated")
os.makedirs(OUT_DIR, exist_ok=True)
fake = Faker()

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def write_sample(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def gen_web_clickstream(n=10):
    records = []
    for _ in range(n):
        rec = {
            "schema_name":"web_clickstream",
            "schema_version":"v1",
            "source_system":"web",
            "event_id": str(uuid.uuid4()),
            "user_id": random.choice([f"user-{random.randint(1,1000)}", None]),
            "session_id": f"sess-{uuid.uuid4().hex[:8]}",
            "event_type": random.choice(["page_view","product_view","add_to_cart","checkout","purchase","click"]),
            "event_timestamp": now_iso(),
            "page_url": f"/product/sku-{random.randint(1,500)}",
            "referrer": fake.url(),
            "user_agent": fake.user_agent(),
            "device_type": random.choice(["desktop","mobile","tablet"]),
            "geo_country": fake.country_code(),
            "properties": {"product_id": f"sku-{random.randint(1,500)}", "price": round(random.uniform(5,500),2)},
            "ingestion_timestamp": now_iso()
        }
        records.append(rec)
    write_sample(os.path.join(OUT_DIR, "web_clickstream_sample.json"), records)
    print("web_clickstream:", len(records))

def gen_mobile_events(n=10):
    records = []
    for _ in range(n):
        rec = {
            "schema_name":"mobile_event","schema_version":"v1","source_system":"mobile",
            "event_id": str(uuid.uuid4()),
            "user_id": f"user-{random.randint(1,1000)}",
            "device_id": f"device-{uuid.uuid4().hex[:8]}",
            "app_version": f"{random.randint(1,3)}.{random.randint(0,9)}.{random.randint(0,9)}",
            "event_type": random.choice(["app_open","screen_view","product_view","add_to_cart","purchase","push_open"]),
            "event_timestamp": now_iso(),
            "os": random.choice(["iOS","Android"]),
            "locale": random.choice(["en-CA","en-US","fr-CA"]),
            "properties": {"product_id": f"sku-{random.randint(1,500)}", "quantity": random.randint(1,3)},
            "ingestion_timestamp": now_iso()
        }
        records.append(rec)
    write_sample(os.path.join(OUT_DIR, "mobile_events_sample.json"), records)
    print("mobile_events:", len(records))

def gen_crm_profiles(n=10):
    records = []
    for _ in range(n):
        uid = f"user-{random.randint(1,1000)}"
        rec = {
            "schema_name":"crm_profile","schema_version":"v1","source_system":"crm",
            "user_id": uid,
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.unique.email(),
            "phone": fake.phone_number(),
            "address": {"line1": fake.street_address(), "city": fake.city(), "postal_code": fake.postcode(), "country": fake.country_code()},
            "loyalty_tier": random.choice(["bronze","silver","gold", None]),
            "created_at": now_iso(),
            "updated_at": now_iso(),
            "ingestion_timestamp": now_iso()
        }
        records.append(rec)
    write_sample(os.path.join(OUT_DIR, "crm_profiles_sample.json"), records)
    print("crm_profiles:", len(records))

def gen_orders(n=10):
    records = []
    for i in range(n):
        rec = {
            "schema_name":"order","schema_version":"v1","source_system":"orders",
            "order_id": f"ord-{datetime.now().strftime('%Y%m%d')}-{i+1:04d}",
            "user_id": f"user-{random.randint(1,1000)}",
            "order_date": now_iso(),
            "total_amount": round(random.uniform(10,500),2),
            "currency":"CAD",
            "payment_method": random.choice(["card","paypal"]),
            "status": random.choice(["created","paid","shipped","completed"]),
            "items": [{"product_id": f"sku-{random.randint(1,500)}", "quantity": random.randint(1,3), "price": round(random.uniform(5,300),2)}],
            "ingestion_timestamp": now_iso()
        }
        records.append(rec)
    write_sample(os.path.join(OUT_DIR, "orders_sample.json"), records)
    print("orders:", len(records))

def gen_marketing(n=10):
    records = []
    for _ in range(n):
        rec = {
            "schema_name":"marketing_event","schema_version":"v1","source_system":"marketing",
            "event_id": str(uuid.uuid4()),
            "campaign_id": f"camp-{random.randint(1,20)}",
            "user_id": f"user-{random.randint(1,1000)}",
            "event_type": random.choice(["email_sent","email_open","email_click","sms_sent","unsubscribe"]),
            "event_timestamp": now_iso(),
            "channel": random.choice(["email","sms"]),
            "utm_campaign": random.choice(["holiday_sale","spring_launch", None]),
            "ingestion_timestamp": now_iso()
        }
        records.append(rec)
    write_sample(os.path.join(OUT_DIR, "marketing_sample.json"), records)
    print("marketing:", len(records))

if __name__ == "__main__":
    gen_web_clickstream(20)
    gen_mobile_events(20)
    gen_crm_profiles(20)
    gen_orders(20)
    gen_marketing(20)
    print("Generated sample files under:", OUT_DIR)
