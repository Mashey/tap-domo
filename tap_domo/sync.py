import singer
from singer import Transformer, metadata

# The client name needs to be filled in here
from tap_domo.client import CLIENT_CLASS_NAME
from tap_domo.streams import STREAMS

LOGGER = singer.get_logger()


def sync(config, state, catalog):
    # Any client required PARAMETERS to hit the endpoint
    client = CLIENT_CLASS_NAME(CLIENT_PARAMETERS_HERE)

    with Transformer() as transformer:
        for stream in catalog.get_selected_streams(state):
            tap_stream_id = stream.tap_stream_id
            stream_obj = STREAMS[tap_stream_id](client, state)
            # replication_key = stream_obj.replication_key
            stream_schema = stream.schema.to_dict()
            stream_metadata = metadata.to_map(stream.metadata)

            LOGGER.info('Staring sync for stream: %s', tap_stream_id)

            state = singer.set_currently_syncing(state, tap_stream_id)
            singer.write_state(state)

            singer.write_schema(
                tap_stream_id,
                stream_schema,
                stream_obj.key_properties,
                stream.replication_key
            )

            client = CLIENT_CLASS_NAME(CLIENT_PARAMETERS_HERE)
            for record in stream_obj.sync(CLIENT_PARAMETERS_HERE):
                transformed_record = transformer.transform(
                    record, stream_schema, stream_metadata)
                LOGGER.info(f"Writing record: {transformed_record}")
                singer.write_record(
                    tap_stream_id,
                    transformed_record,
                )

            # If there is a Bookmark or state based key to store
            state = singer.clear_bookmark(
                state, tap_stream_id, BOOKMARK_KEY)
            singer.write_state(state, tap_stream_id, )

    state = singer.set_currently_syncing(state, None)
    singer.write_state(state)
