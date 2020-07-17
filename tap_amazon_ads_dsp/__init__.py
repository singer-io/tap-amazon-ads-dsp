# coding: utf-8

__author__ = 'Scott Coleman'
__email__ = 'scott.coleman@bytecode.io'

import argparse
import json
import sys

import singer
from singer import metadata, utils
from tap_amazon_ads_dsp.client import AmazonAdvertisingClient
from tap_amazon_ads_dsp.discover import discover
from tap_amazon_ads_dsp.sync import sync

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'client_id', 'client_secret', 'refresh_token', 'redirect_uri',
    'start_date', 'profiles', 'reports'
]


def do_discover(reports):
    LOGGER.info('Starting discover')
    catalog = discover(reports)
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    LOGGER.info('Finished discover')


@singer.utils.handle_top_exception(LOGGER)
def main():

    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    config = parsed_args.config

    try:
        ads_client = AmazonAdvertisingClient(config)
        ads_client.login()

        state = {}
        if parsed_args.state:
            state = parsed_args.state

        catalog = parsed_args.catalog

        reports = config.get('reports', [])
        if isinstance(reports, str):
            reports = json.loads(reports)

        if parsed_args.discover:
            do_discover(reports)
        elif parsed_args.catalog:
            sync(client=ads_client,
                 config=config,
                 catalog=catalog,
                 state=state)
    finally:
        if ads_client:
            if ads_client.login_timer:
                ads_client.login_timer.cancel()


if __name__ == '__main__':
    main()
