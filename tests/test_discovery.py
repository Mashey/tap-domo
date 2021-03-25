import pytest
from pymock import PyMock
import os
from tap_domo.discovery import get_schemas, discover
from dotenv import load_dotenv

config = {
    "client_id": os.getenv("client_id"),
    "client_secret": os.getenv("client_secret"),
    "data_specs": {
        "inventory": {
            "object_type": "INVENTORY",
            "table_name": "inventory_upsert",
            "data_set": "ee10a8b4-12cc-4471-92de-16ab97d6fd94",
            "replication_method": "INCREMENTAL",
            "tap_stream_id": "inventory",
            "replication_key": ""
        },
        "customer": {
            "object_type": "CUSTOMER",
            "table_name": "customer_master",
            "data_set": "2d4d8b8b-b3bc-4570-9493-f3f25cd120fd",
            "replication_method": "INCREMENTAL",
            "tap_stream_id": "customer",
            "replication_key": "last_updated_at"
        }
    },
}


def test_get_schema():
    schema, schema_metadata = get_schemas(config)

    assert len(schema) == len(config['data_specs'])
    assert len(schema_metadata) == len(config['data_specs'])
    assert schema.keys () == config['data_specs'].keys()
