from typing import Dict
import json
from singer import get_logger

LOGGER = get_logger()

def read_config(config_path) -> Dict:
    try:
        with open(config_path,'r') as tap_config:
            return json.load(tap_config)
    except FileNotFoundError as _:
        LOGGER.fatal("Failed to load config in dev mode")
        return {}


def write_config(config_path,data :Dict) -> Dict:
    config =  read_config(config_path)
    config.update(data)
    with open(config_path,'w') as tap_config:
        json.dump(config, tap_config, indent=2)
    return config
