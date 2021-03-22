import json
import requests

# Build the Data Resource Service Here as a class with each endpoint as a function.
# Do not iterate over paginated endpoints in this file.  Below are just samples

class RESOURCENAMEClient:
    BASE_URL = BASE_API_URL

    def __init__(self, CLIENT_PARAMETERS):
        self._client = requests.Session()


    def fetch_access_token(self, client_id, api_key):
        url = f'{self.BASE_URL}/config/api/gettokens'
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        payload_dict = {
            'client_id': client_id,
            'apikey': api_key
        }
        return self._client.post(url, headers=headers, data=payload_dict).json()['access_token']

    def fetch_ENDPOINT_1(self):
        url = f'{self.BASE_URL}/ADDITIONAL_URI_ADDRESS'
        param_payload = {
            'active': 'true',
            'pagesize': NUMBER,  # Max per page count
            'page': NUMBER  # Page will have to be iterated over in a range
        }
        return self._client.get(url, params=param_payload).json()

    def fetch_ENDPOINT_2(self):
        url = f'{self.BASE_URL}/ADDITIONAL_URI_ADDRESS'
        return self._client.get(url).json()

