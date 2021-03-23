import singer

LOGGER = singer.get_logger()


class Stream:
    tap_stream_id = None
    key_properties = []
    replication_method = ""
    valid_replication_keys = []
    replication_key = "last_updated_at"
    object_type = ""
    selected = True

    def __init__(self, client, state):
        self.client = client
        self.state = state

    def sync(self, *args, **kwargs):
        raise NotImplementedError("Sync of child class not implemented")


class CatalogStream(Stream):
    replication_method = "INCREMENTAL"


class FullTableStream(Stream):
    replication_method = "FULL_TABLE"


class ENDPOINT1Info(FullTableStream):
    tap_stream_id = "ENDPOINT1_info"
    key_properties = ["ENDPOINT1_id"]
    object_type = "ENDPOINT1_INFO"

    def sync(self, CLIENT_ARUGMENTS):
        ## This is where to setup iteration over each end point
        response = self.client.fetch_ENDPOINT1s(ENDPOINT1_PARAMETERS)
        ENDPOINT1s = response.get("data", {}).get("ENDPOINT1_list", [])
        for ENDPOINT1 in ENDPOINT1s:
            yield ENDPOINT1


class ENDPOINT2Info(FullTableStream):
    tap_stream_id = "ENDPOINT2_info"
    key_properties = ["ENDPOINT2_id"]
    object_type = "ENDPOINT2_INFO"

    def sync(self, CLIENT_ARUGMENTS):
        ## This is where to setup iteration over each end point
        response = self.client.fetch_ENDPOINT2s(ENDPOINT2_PARAMETERS)
        ENDPOINT2s = response.get("data", [])
        for ENDPOINT2 in ENDPOINT2s:
            yield ENDPOINT2


STREAMS = {"ENDPOINT1s": ENDPOINT1Info, "ENDPOINT2s": ENDPOINT2Info}
