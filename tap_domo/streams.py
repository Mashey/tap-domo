import singer
import sys

LOGGER = singer.get_logger()


class Stream:
    def __init__(self, data_spec, client, state=None):
        self.client = client
        self.state = state
        self.tap_stream_id = data_spec["tap_stream_id"]
        self.object_type = data_spec["object_type"]
        self.data_set = data_spec["data_set"]
        self.replication_method = data_spec["replication_method"]
        self.key_properties = []
        self.replication_key = (
            data_spec["replication_key"] if "replication_key" in data_spec else ""
        )
        self.table_name = data_spec["table_name"]
        self.start_key = data_spec["start_key"] if "start_key" in data_spec else ""

    def records_sync(self, limit_rate: int):
        limit = limit_rate
        offset = 0
        record_count = limit
        batch = 1

        bookmark = singer.get_bookmark(
            self.state, self.tap_stream_id, self.replication_key, self.start_key
        )

        while record_count >= limit:
            try:
                LOGGER.info(f'Starting batch: {batch}')
                record_count = 0
                response = self.client.records_query(
                    self.data_set,
                    self.table_name,
                    limit,
                    offset,
                    self.replication_key,
                    bookmark,
                )

                if len(response) > 0:
                    for record in response:
                        record.pop("index")
                        for key in record:
                            if record[key] == "":
                                record[key] = None
                        record_count += 1
                        yield record

                batch += 1
                offset += limit

            except Exception as e:
                if 'Error creating query' in e.args[0]:
                    LOGGER.warning(f'Error occured: {e.args[0]}')
                    LOGGER.warning(f'Restarting last batch.')
                    self.client.reconnect()
                    record_count = limit
                    next
                else:
                    LOGGER.critical(f'Fatal error: {e}')
                    sys.exit(1)
