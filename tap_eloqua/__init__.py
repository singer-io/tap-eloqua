#!/usr/bin/env python3

import sys
import json
import argparse

import singer
from singer import metadata

from tap_eloqua.client import EloquaClient
from tap_eloqua.discover import discover
from tap_eloqua.sync import sync

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

##### TEMP

from singer.catalog import Catalog

@singer.utils.handle_top_exception(LOGGER)
def main():
    args = singer.parse_args(REQUIRED_CONFIG_KEYS)
    if args.dev:
        LOGGER.warning("Executing Tap in Dev mode")  
    with EloquaClient(args.config_path, args.config, args.dev) as client:
        if args.discover:
            do_discover(client)
        elif args.catalog:
            sync(client, args.catalog, args.state, args.config)

if __name__ == "__main__":
    main()