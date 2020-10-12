import codecs
import csv
import threading

import backoff
import requests
import requests_oauthlib
import singer
import singer.metrics

LOGGER = singer.get_logger()  # noqa

TOKEN_URL = 'https://api.amazon.com/auth/o2/token'
SCOPES = ["cpc_advertising:campaign_management"]
ADS_URL = 'https://advertising-api.amazon.com/dsp/reports'
TOKEN_EXPIRATION_PERIOD = 3000
LOGGER = singer.get_logger()
BACKOFF_MAX_TRIES = 9
BACKOFF_FACTOR = 3


class Server5xxError(Exception):
    pass


class Server401Error(Exception):
    pass

class Server42xRateLimitError(Exception):
    pass


class AmazonAdvertisingClient:
    def __init__(self, config):
        self.config = config
        self.login_timer = None
        self.session = requests.Session()
        self.access_token = None

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

    @backoff.on_exception(
        backoff.expo,
        (Server5xxError, ConnectionError, Server42xRateLimitError, Server401Error),
        max_tries=BACKOFF_MAX_TRIES,
        factor=BACKOFF_FACTOR)
    def make_request(self,
                     url=None,
                     method=None,
                     profile=None,
                     job=None,
                     params=None,
                     body=None,
                     stream=False):
        if job:
            url = ADS_URL + '/' + job
        else:
            url = ADS_URL
        LOGGER.info("Making {} request ({})".format(method, url))

        headers = {
            'Authorization': 'Bearer {}'.format(self.access_token),
            'Amazon-Advertising-API-ClientId': self.config.get('client_id'),
            'Amazon-Advertising-API-Scope': profile,
        }

        try:
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
        except ConnectionError as ex:
            LOGGER.info(f"Connection error {ex}")
            raise ConnectionError(ex)

        LOGGER.info("Received code: {}".format(response.status_code))

        if response.status_code == 401:
            LOGGER.info(
                "Received unauthorized error code. Indicative of access issue for profile {}: {}".format(profile,
                    response.text))
            self.login()
            raise Server401Error(response.text)
        elif response.status_code == 429:
            LOGGER.info("Received rate limit response: {}".format(
                response.headers))
            raise Server42xRateLimitError()
        elif response.status_code >= 500:
            raise Server5xxError()

        if response.status_code not in [200, 201, 202]:
            raise RuntimeError(response.text)

        return response


# Stream CSV in batches of lines for transform and Singer write
@backoff.on_exception(backoff.expo, (Server5xxError, ConnectionError),
                      max_tries=BACKOFF_MAX_TRIES,
                      factor=BACKOFF_FACTOR)
def stream_csv(url, batch_size=1024):
    with requests.get(url, stream=True) as data:
        reader = csv.DictReader(
            codecs.iterdecode(data.iter_lines(chunk_size=1024), "utf-8"))
        batch = []

        for record in reader:
            batch.append(record)
            if len(batch) == batch_size:
                yield batch
                batch = []
        if batch:
            yield batch
