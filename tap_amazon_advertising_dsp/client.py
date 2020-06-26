import requests
import requests_oauthlib
import singer
import singer.metrics

import zlib
import json
import time

LOGGER = singer.get_logger()  # noqa

TOKEN_URL = 'https://api.amazon.com/auth/o2/token'
SCOPES = ["cpc_advertising:campaign_management"]
ADS_URL = 'https://advertising-api.amazon.com/dsp/reports'


class AmazonAdvertisingClient:

    MAX_TRIES = 5

    def __init__(self, config):
        self.config = config
        self.access_token = self.get_authorization()

    def get_authorization(self):
        client_id = self.config.get('client_id')
        oauth = requests_oauthlib.OAuth2Session(
                    client_id,
                    redirect_uri=self.config.get('redirect_uri'),
                    scope=SCOPES)

        tokens = oauth.refresh_token(
                    TOKEN_URL,
                    refresh_token=self.config.get('refresh_token'),
                    client_id=self.config.get('client_id'),
                    client_secret=self.config.get('client_secret'))

        return tokens['access_token']

    def _make_request(self, method, entity, params=None, body=None, attempts=0):
        LOGGER.info("Making {} request ({})".format(method, params))

        response = requests.request(
            method,
            ADS_URL,
            headers={
                'Authorization': 'Bearer {}'.format(self.access_token),
                'Amazon-Advertising-API-ClientId': self.config.get('client_id'),
                'Amazon-Advertising-API-Scope': entity,
            },
            params=params,
            data=body)

        LOGGER.info("Received code: {}".format(response.status_code))

        if attempts < self.MAX_TRIES and response.status_code in [429, 502, 401]:
            if response.status_code == 401:
                LOGGER.info("Received unauthorized error code, retrying: {}".format(response.text))
                self.access_token = self.get_authorization()

            else:
                LOGGER.info("Received rate limit response, sleeping: {}".format(response.text))
                time.sleep(30)

            return self._make_request(url, method, params, body, attempts+1)

        if response.status_code not in [200, 201, 202]:
            raise RuntimeError(response.text)

        return response

    def make_request(self, url, method, params=None, body=None):
        return self._make_request(url, method, params, body).json()

    def download_gzip(self, url):
        resp = None
        attempts = 3
        for i in range(attempts + 1):
            try:
                resp = self._make_request(url, 'GET')
                break
            except ConnectionError as e:
                LOGGER.info("Caught error while downloading gzip, sleeping: {}".format(e))
                time.sleep(10)
        else:
            raise RuntimeError("Unable to sync gzip after {} attempts".format(attempts))

        return self.unzip(resp.content)

    @classmethod
    def unzip(cls, blob):
        extracted = zlib.decompress(blob, 16+zlib.MAX_WBITS)
        decoded = extracted.decode('utf-8')
        return json.loads(decoded)