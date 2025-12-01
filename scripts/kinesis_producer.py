import json, os, time, uuid
import boto3
from jsonschema import Draft7Validator
from math import ceil

AWS_REGION = "us-east-1"
STREAM_NAME = "customer360-clickstream"
SCHEMAS_DIR = os.path.join(os.path.dirname(__file__), "..", "schemas")
GENERATED_DIR = os.path.join(os.path.dirname(__file__), "..", "generated")

kinesis = boto3.client("kinesis", region_name=AWS_REGION)

def load_schema(source):
    path = os.path.join(SCHEMAS_DIR, source, "schema_v1.json")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def validate_record(rec, validator):
    errors = list(validator.iter_errors(rec))
    return errors

def send_batch(records):
    # prepare Records list for put_records
    payloads = []
    for r in records:
        pk = r.get("user_id") or r.get("event_id") or str(uuid.uuid4())
        payloads.append({"Data": json.dumps(r).encode("utf-8"), "PartitionKey": str(pk)})
    resp = kinesis.put_records(Records=payloads, StreamName=STREAM_NAME)
    return resp

def produce_from_file(source_name, file_name, max_batch=200, rate_per_sec=5):
    schema = load_schema(source_name)
    validator = Draft7Validator(schema)
    with open(file_name, "r", encoding="utf-8") as f:
        payload = json.load(f)   # expecting array
    total = len(payload)
    i = 0
    while i < total:
        batch = payload[i:i+max_batch]
        # validate
        for rec in batch:
            errs = validate_record(rec, validator)
            if errs:
                print("Validation error, skipping record:", errs[0].message)
                batch.remove(rec)
        if not batch:
            i += max_batch
            continue
        resp = send_batch(batch)
        failed = resp.get("FailedRecordCount", 0)
        print(f"Sent batch {i}-{i+len(batch)-1}, failed={failed}")
        i += max_batch
        time.sleep(max(0.2, 1.0/ rate_per_sec))  # simple rate control

if __name__ == "__main__":
    # send web + mobile sample files
    produce_from_file("web_clickstream", os.path.join(GENERATED_DIR, "web_clickstream_sample.json"), max_batch=100, rate_per_sec=5)
    produce_from_file("mobile_events", os.path.join(GENERATED_DIR, "mobile_events_sample.json"), max_batch=100, rate_per_sec=5)
    print("Done producing.")