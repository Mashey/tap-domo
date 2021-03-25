import json
import os
from singer import metadata, get_logger
from singer.catalog import Catalog
from tap_domo.client import DOMOClient

# from .streams import Stream

LOGGER = get_logger()


def get_schemas(config):

    client = DOMOClient(
        client_id=config["client_id"], client_secret=config["client_secret"]
    )

    schemas = {}
    schemas_metadata = {}

    for table_name, table_specs in config["data_specs"].items():
        LOGGER.info(f"Building Schema for {table_specs['object_type']}")
        stream_name = table_specs["tap_stream_id"]
        schema = client.get_schema(
            data_set=table_specs["data_set"], table_name=table_specs["table_name"]
        )
        schema["properties"].pop("index")

        meta = metadata.get_standard_metadata(
            schema=schema,
            key_properties=table_specs["key_properties"]
            if "key_properties" in table_specs
            else [],
            replication_method=table_specs["replication_method"],
        )

        meta = metadata.to_map(meta)

        if "valid_replication_keys" in table_specs:
            meta = metadata.write(
                meta,
                (),
                "valid-replication-keys",
                table_specs["valid_replication_keys"],
            )
        if "replication_key" in table_specs:
            meta = metadata.write(
                meta,
                ("properties", table_specs["replication_key"]),
                "inclusion",
                "automatic",
            )

        meta = metadata.to_list(meta)

        schemas[stream_name] = schema
        schemas_metadata[stream_name] = meta

    return schemas, schemas_metadata


def discover(config):
    schemas, schemas_metadata = get_schemas(config=config)
    streams = []

    for schema_name, schema in schemas.items():
        schema_meta = schemas_metadata[schema_name]

        catalog_entry = {
            "stream": schema_name,
            "tap_stream_id": schema_name,
            "schema": schema,
            "metadata": schema_meta,
        }

        streams.append(catalog_entry)

    return Catalog.from_dict({"streams": streams})
