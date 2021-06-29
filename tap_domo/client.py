from pydomo import Domo
import json


class DOMOClient:
    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.domo = Domo(client_id, client_secret)

    def reconnect(self):
        self.domo = Domo(self.client_id, self.client_secret)
        return

    def records_query(
        self,
        data_set: str,
        table_name: str,
        limit: int,
        offset: int,
        replication_key: str = None,
        bookmark: str = None,
    ) -> list:
        if replication_key and table_name == 'inventory_upsert':
            sql = f"SELECT * FROM {table_name} WHERE {replication_key} > '{bookmark}' ORDER BY {replication_key}, id LIMIT {limit} OFFSET {offset}"
        elif replication_key:
            sql = f"SELECT * FROM {table_name} WHERE {replication_key} > '{bookmark}' ORDER BY {replication_key} LIMIT {limit} OFFSET {offset}"
        else:
            sql = f"SELECT * FROM {table_name} LIMIT {limit} OFFSET {offset}"
        return self.send_query(data_set=data_set, query_string=sql)

    def historical_records_query(
        self,
        data_set: str,
        table_name: str,
        rolling_timestamp: str
    ) -> list:
        sql = f"SELECT * FROM {table_name} WHERE dateclosed = '{rolling_timestamp}'"

        return self.send_query(data_set=data_set, query_string=sql)

    def send_query(self, data_set: str, query_string: str) -> dict:
        return json.loads(
            self.domo.ds_query(data_set, query_string).to_json(orient="table")
        )["data"]

    def get_schema(self, data_set: str, table_name: str) -> dict:
        sql = f"SELECT * FROM {table_name} LIMIT 1"
        meta = json.loads(
            self.domo.ds_query(dataset_id=data_set, query=sql).to_json(orient="table")
        )
        return self.build_singer_schema(schema_json=meta["schema"]["fields"])

    def build_singer_schema(self, schema_json: list) -> dict:
        schema = {"type": ["object", "null"], "properties": {}}
        for data in schema_json:
            schema["properties"][data["name"]] = {"type": [data["type"], "null"]}

        return schema
