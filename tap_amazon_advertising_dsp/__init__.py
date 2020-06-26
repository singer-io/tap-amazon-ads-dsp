# coding: utf-8

__author__ = 'Scott Coleman'
__email__ = 'scott.coleman@bytecode.io'

import sys
import json
import argparse
import singer
from singer import metadata, utils
from tap_amazon_advertising_dsp.client import AmazonAdvertisingClient
from tap_amazon_advertising_dsp.discover import discover
from tap_amazon_advertising_dsp.sync import sync

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
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

    ads_client = AmazonAdvertisingClient(config)

    state = {}
    if parsed_args.state:
        state = parsed_args.state

    catalog = parsed_args.catalog

    reports = config.get('reports', {})

    if parsed_args.discover:
        do_discover(reports)
    elif parsed_args.catalog:
        sync(client=ads_client,
             config=config,
             catalog=catalog,
             state=state)

if __name__ == '__main__':
    main()
