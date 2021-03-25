import pytest
import os
from tap_domo.client import DOMOClient
from dotenv import load_dotenv
load_dotenv()
config = {
    "client_id": os.getenv("client_id"),
    "client_secret": os.getenv("client_secret"),
}


@pytest.mark.vcr()
def test_send_query():
    client = DOMOClient(
        client_id=config["client_id"], client_secret=config["client_secret"]
    )
    data_set = "ee10a8b4-12cc-4471-92de-16ab97d6fd94"
    query_string = "SELECT * FROM inventory_upset LIMIT 10"

    response = client.send_query(data_set=data_set, query_string=query_string)

    assert isinstance(response, list)


@pytest.mark.vcr()
def test_get_schema():
    client = DOMOClient(
        client_id=config["client_id"], client_secret=config["client_secret"]
    )
    data_set = "ee10a8b4-12cc-4471-92de-16ab97d6fd94"
    table = "inventory_upsert"

    response = client.get_schema(data_set=data_set, table_name=table)

    assert isinstance(response, dict)


def test_build_singer_schema():
    client = DOMOClient(
        client_id=config["client_id"], client_secret=config["client_secret"]
    )

    returned_schema = [
        {"name": "index", "type": "integer"},
        {"name": "unit_of_measure", "type": "string"},
        {"name": "type", "type": "string"},
        {"name": "storeid", "type": "integer"},
        {"name": "source", "type": "string"},
        {"name": "sold_units", "type": "number"},
    ]

    target_schema = {
        "type": ["object", "null"],
        "properties": {
            "index": {"type": ["integer", "null"]},
            "unit_of_measure": {"type": ["string", "null"]},
            "type": {"type": ["string", "null"]},
            "storeid": {"type": ["integer", "null"]},
            "source": {"type": ["string", "null"]},
            "sold_units": {"type": ["number", "null"]},
        },
    }

    assert client.build_singer_schema(schema_json=returned_schema) == target_schema
