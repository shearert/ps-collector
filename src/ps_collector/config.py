
import os

import ConfigParser


def get_config():
    cp = ConfigParser.ConfigParser()
    config_file = os.environ.get("PS_COLLECTOR_CONFIG", "/etc/ps-collector/config.ini")
    cp.read(config_file)
    return cp

