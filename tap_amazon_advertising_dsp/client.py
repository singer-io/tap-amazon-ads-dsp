import json
import threading
import time
import zlib

import requests
import requests_oauthlib
import singer
import singer.metrics

LOGGER = singer.get_logger()  # noqa

TOKEN_URL = 'https://api.amazon.com/auth/o2/token'
SCOPES = ["cpc_advertising:campaign_management"]
ADS_URL = 'https://advertising-api.amazon.com/dsp/reports'
TOKEN_EXPIRATION_PERIOD = 3000


class Server5xxError(Exception):
    pass


class Server42xRateLimitError(Exception):
    pass


class AmazonAdvertisingClient:

    MAX_TRIES = 5

    def __init__(self, config):
        self.config = config
        self.login_timer = None
        self.session = requests.Session()

    def login(self):
        LOGGER.info(f"Refreshing token")
        client_id = self.config.get('client_id')

        try:
            oauth = requests_oauthlib.OAuth2Session(
                client_id,
                redirect_uri=self.config.get('redirect_uri'),
                scope=SCOPES)

            tokens = oauth.refresh_token(
                TOKEN_URL,
                refresh_token=self.config.get('refresh_token'),
                client_id=self.config.get('client_id'),
                client_secret=self.config.get('client_secret'))
        finally:
            self.login_timer = threading.Timer(TOKEN_EXPIRATION_PERIOD,
                                               self.login)
            self.login_timer.start()

        self.access_token = tokens['access_token']

    def _make_request(self,
                      url=None,
                      method=None,
                      entity=None,
                      job=None,
                      params=None,
                      body=None,
                      attempts=0,
                      stream=False):
        if job:
            url = ADS_URL + '/' + job
        else:
            url = ADS_URL
        LOGGER.info("Making {} request ({})".format(method, url))

        headers = {
            'Authorization': 'Bearer {}'.format(self.access_token),
            'Amazon-Advertising-API-ClientId': self.config.get('client_id'),
            'Amazon-Advertising-API-Scope': entity,
        }

        if method == "GET":
            LOGGER.info(
                f"Making {method} request to {url} with params: {params}")
            response = self.session.get(url,
                                        headers=headers,
                                        stream=stream,
                                        params=params)
        elif method == "POST":
            LOGGER.info(f"Making {method} request to {url} with body {body}")
            response = self.session.post(url,
                                         headers=headers,
                                         params=params,
                                         data=body)
        elif method == "PATCH":
            LOGGER.info(f"Making {method} request to {url} with body {body}")
            response = self.session.patch(url,
                                          headers=headers,
                                          json=body,
                                          params=params)
        else:
            raise Exception("Unsupported HTTP method")

        LOGGER.info("Received code: {}".format(response.status_code))

        if attempts < self.MAX_TRIES and response.status_code in [
                429, 502, 401
        ]:
            if response.status_code == 401:
                LOGGER.info(
                    "Received unauthorized error code, retrying: {}".format(
                        response.text))
                self.login()

            elif response.status_code == 429:
                LOGGER.info("Received rate limit response: {}".format(
                    response.headers))
                retry_after = int(response.headers['retry-after'])
                LOGGER.info(f"Sleeping for {retry_after}")
                time.sleep(retry_after)

            return self._make_request(method=method,
                                      url=url,
                                      params=params,
                                      body=body,
                                      attempts=attempts + 1,
                                      stream=stream)

        if response.status_code not in [200, 201, 202]:
            raise RuntimeError(response.text)

        return response

    def make_request(self, url, method, params, body):
        return self._make_request(url=url,
                                  method=method,
                                  params=params,
                                  body=body).json()

    def stream_report(self, url):
        LOGGER.info("Making {} request ({})".format('GET', url))
        with requests.get(url, stream=True) as response:
            for line in response.iter_lines():
                if line:
                    yield line

