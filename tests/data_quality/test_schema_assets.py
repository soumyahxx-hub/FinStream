import json
from pathlib import Path


def test_all_avro_assets_are_json_records() -> None:
    schemas = Path("streaming/kafka/schemas").glob("*.avsc")
    for schema in schemas:
        assert json.loads(schema.read_text(encoding="utf-8"))["type"] == "record"
