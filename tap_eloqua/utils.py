from typing import Dict
import json
from singer import get_logger

LOGGER = get_logger()


def write_config(config_path,data) :
    """
    Updates the provided filepath with json format of the `data` object
    does a safe write by performing a read before write, updates only specific keys, does not rewrite.
    """
    with open(config_path,'r') as tap_config:
        config = json.load(tap_config)
    config.update(data)
    with open(config_path,'w') as tap_config:
        json.dump(config, tap_config, indent=2)
    return config
