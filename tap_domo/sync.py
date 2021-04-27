import singer
from singer import Transformer, metadata
import time

from tap_domo.client import DOMOClient
from tap_domo.streams import Stream


LOGGER = singer.get_logger()


def sync(config, state, catalog):
    client = DOMOClient(
        client_id=config["client_id"], client_secret=config["client_secret"]
    )

    limit = config["limit"] if "limit" in config else 1500
    total_records = []
    stream_rps = []

    with Transformer() as transformer:
        for stream in catalog.get_selected_streams(state):
            stream_start = time.perf_counter()
            record_count = 0
            tap_stream_id = stream.tap_stream_id
            data_spec = config["data_specs"][tap_stream_id]
            stream_obj = Stream(data_spec, client, state)
            replication_key = stream_obj.replication_key
            stream_schema = stream.schema.to_dict()
            stream_metadata = metadata.to_map(stream.metadata)

            LOGGER.info("Staring sync for stream: %s", tap_stream_id)

            state = singer.set_currently_syncing(state, tap_stream_id)
            singer.write_state(state)

            singer.write_schema(
                tap_stream_id,
                stream_schema,
                stream_obj.key_properties,
                stream.replication_key,
            )

            for record in stream_obj.records_sync(limit):
                transformed_record = transformer.transform(
                    record, stream_schema, stream_metadata
                )

                singer.write_record(
                    tap_stream_id,
                    transformed_record,
                )
                record_count += 1

                singer.write_bookmark(
                    state, tap_stream_id, replication_key, record[replication_key]
                )

            # If there is a Bookmark or state based key to store
            
            if record_count == 0:
                LOGGER.info(f'No records to update bookmark')

            stream_stop = time.perf_counter()

            total_records.append(record_count)
            info, rps = metrics(stream_start, stream_stop, record_count)
            stream_rps.append(rps)
            LOGGER.info(f"{info}")
            singer.write_bookmark(state, tap_stream_id, "metrics", info)

            singer.write_state(state)

    state = singer.set_currently_syncing(state, None)
    overall_rps = overall_metrics(total_records, stream_rps)
    LOGGER.info(
        f"Total Records: {sum(total_records)} / Overall RPS: {overall_rps:0.6}"
    )
    singer.write_bookmark(state, "Overall", "metrics",
                          f"Records: {sum(total_records)} / RPS: {overall_rps:0.6}")
    singer.write_state(state)


def metrics(start: float, end: float, records: int):
    elapsed_time = end - start
    rps = records / elapsed_time
    info = f"Stream runtime: {elapsed_time:0.6} seconds / Records: {records} / RPS: {rps:0.6}"
    return info, rps


def overall_metrics(records: list, rps_list: list) -> float:
    stream_count = len(records)
    total_rps = sum(rps_list)
    return total_rps / stream_count
