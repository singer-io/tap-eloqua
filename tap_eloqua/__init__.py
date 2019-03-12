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

def check_config(config, required_keys):
    missing_keys = [key for key in required_keys if key not in config]
    if missing_keys:
        raise Exception("Config is missing required keys: {}".format(missing_keys))

def load_json(path):
    with open(path) as fil:
        return json.load(fil)

def parse_args(required_config_keys):
    '''Parse standard command-line args.

    Parses the command-line arguments mentioned in the SPEC and the
    BEST_PRACTICES documents:

    -c,--config     Config file
    -s,--state      State file
    -d,--discover   Run in discover mode
    -p,--properties Properties file: DEPRECATED, please use --catalog instead
    --catalog       Catalog file

    Returns the parsed args object from argparse. For each argument that
    point to JSON files (config, state, properties), we will automatically
    load and parse the JSON file.
    '''
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-c', '--config',
        help='Config file',
        required=True)

    parser.add_argument(
        '-s', '--state',
        help='State file')

    parser.add_argument(
        '-p', '--properties',
        help='Property selections: DEPRECATED, Please use --catalog instead')

    parser.add_argument(
        '--catalog',
        help='Catalog file')

    parser.add_argument(
        '-d', '--discover',
        action='store_true',
        help='Do schema discovery')

    args = parser.parse_args()
    if args.config:
        setattr(args, 'config_path', args.config)
        args.config = load_json(args.config)
    if args.state:
        setattr(args, 'state_path', args.state)
        args.state = load_json(args.state)
    else:
        args.state = {}
    if args.properties:
        setattr(args, 'properties_path', args.properties)
        args.properties = load_json(args.properties)
    if args.catalog:
        setattr(args, 'catalog_path', args.catalog)
        args.catalog = Catalog.load(args.catalog)

    check_config(args.config, required_config_keys)

    return args

#####

@singer.utils.handle_top_exception(LOGGER)
def main():
    #parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    parsed_args = parse_args(REQUIRED_CONFIG_KEYS)

    with EloquaClient(parsed_args.config_path,
                      parsed_args.config['client_id'],
                      parsed_args.config['client_secret'],
                      parsed_args.config['refresh_token'],
                      parsed_args.config['redirect_uri'],
                      parsed_args.config.get('user_agent')) as client:

        if parsed_args.discover:
            do_discover(client)
        elif parsed_args.catalog:
            sync(client,
                 parsed_args.catalog,
                 parsed_args.state,
                 parsed_args.config['start_date'],
                 int(parsed_args.config.get('bulk_page_size', 5000)))
