from typing import Dict
import json
from singer import get_logger

LOGGER = get_logger()

def read_config(config_path) -> Dict:
    """
    Performs read on the provided filepath,
    returns empty dict if invalid path provided
    """
    try:
        with open(config_path,'r') as tap_config:
            return json.load(tap_config)
    except FileNotFoundError as _:
        LOGGER.fatal("Failed to load config in dev mode")
        return {}


def write_config(config_path,data :Dict) -> Dict:
    """
    Updates the provided filepath with json format of the `data` object
    does a safe write by performing a read before write, updates only specific keys, does not rewrite.
    """
    config =  read_config(config_path)
    config.update(data)
    with open(config_path,'w') as tap_config:
        json.dump(config, tap_config, indent=2)
    return config
