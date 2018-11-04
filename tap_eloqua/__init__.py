#!/usr/bin/env python3

import json
import sys

import singer
from singer import metadata

from tap_eloqua.client import EloquaClient
from tap_eloqua.discover import discover
from tap_eloqua.sync_bulk import sync_bulk

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'start_date',
    'client_id',
    'client_secret',
    'refresh_token',
    'redirect_uri'
]

def do_discover(client):
    LOGGER.info('Starting discover')
    catalog = discover(client)
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    LOGGER.info('Finished discover')

@singer.utils.handle_top_exception(LOGGER)
def main():
    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    with EloquaClient(parsed_args.config['client_id'],
                      parsed_args.config['client_secret'],
                      parsed_args.config['refresh_token'],
                      parsed_args.config['redirect_uri']) as client:

        if parsed_args.discover:
            do_discover(client)
        elif parsed_args.catalog:
            sync_bulk(client,
                      parsed_args.catalog,
                      parsed_args.state,
                      parsed_args.config['start_date'])
