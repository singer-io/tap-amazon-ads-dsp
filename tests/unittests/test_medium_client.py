import unittest
from collections.abc import Generator
from unittest import mock
from unittest.mock import Mock, PropertyMock, patch

import backoff
import requests
import requests_mock
import urllib3
from requests import Session
from urllib3 import Timeout

import pytest
from pytest import raises
from tap_amazon_ads_dsp import client
from tap_amazon_ads_dsp.client import Server42xRateLimitError, Server5xxError, Server401Error
from tests.configuration.fixtures import CONFIG, ads_client


@mock.patch('time.sleep', return_value=None)
def test_request_backoff_on_retry_error(mock_sleep, ads_client):
    with requests_mock.Mocker() as m:
        m.post('https://advertising-api.amazon.com/dsp/reports', status_code=429)

        with raises(Server42xRateLimitError) as ex:
            response = ads_client.make_request(method='POST', url='https://advertising-api.amazon.com/dsp/reports')
        # Assert backoff retry count as expected    
        assert mock_sleep.call_count == client.BACKOFF_MAX_TRIES - 1

@mock.patch('time.sleep', return_value=None)
@mock.patch('tap_amazon_ads_dsp.AmazonAdvertisingClient.login')
def test_request_backoff_on_unauthorized(mock_login, mock_sleep, ads_client):
    mock_login.return_value = "None"

    with requests_mock.Mocker() as m:
        m.post('https://advertising-api.amazon.com/dsp/reports', status_code=401)

        with raises(Server401Error) as ex:
            response = ads_client.make_request(method='POST', url='https://advertising-api.amazon.com/dsp/reports')
        # Assert backoff retry count as expected
        assert mock_sleep.call_count == client.BACKOFF_MAX_TRIES - 1
