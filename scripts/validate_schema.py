import json
import os
from jsonschema import validate, ValidationError, Draft7Validator


BASE_DIR = os.path.join(os.path.dirname(__file__), '..',"schemas")

def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def validate_schema(schema_path, sample_path):
    """Validate one sample file against its schema"""
    schema = load_json(schema_path)
    sample = load_json(sample_path)
    validator = Draft7Validator(schema)
    errors = sorted(validator.iter_errors(sample), key=lambda e: e.path)

    if errors:
        print(f"❌ Validation failed for {sample_path}")
        for error in errors:
            print(f"  - {list(error.path)}: {error.message}")
        return False
    else:
        print(f"✅ {os.path.basename(sample_path)} is valid.")
        return True

def main():
    print("🔍 Validating all sample JSON files...\n")
    passed = 0
    failed = 0


    for root, _, files in os.walk(BASE_DIR):
        for f in files:
            if f.startswith("sample") and f.endswith(".json"):
                schema_file = os.path.join(root, f.replace("sample", "schema"))
                sample_file = os.path.join(root, f)
                if os.path.exists(schema_file):
                    if validate_schema(schema_file, sample_file):
                        passed += 1
                    else:
                        failed += 1
                else:
                    print(f"⚠️  No schema found for {sample_file}")
    print(f"\nSummary: {passed} passed, {failed} failed")


if __name__ == "__main__":
    main()
