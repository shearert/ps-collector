
import os

import ConfigParser


def get_config():
    cp = ConfigParser.ConfigParser()
    config_file = os.environ.get("PS_COLLECTOR_CONFIG", "/etc/ps-collector/config.ini")
    cp.read(config_file)
    if cp.has_section("General") and cp.has_option("General", "config_directory"):
        config_dir = cp.get("General", "config_directory")
        file_list = list(os.listdir(config_dir))
        file_list.sort()
        for fname in file_list:
            if fname.startswith(".") or fname.endswith(".rpmsave") or fname.endswith(".rpmnew"):
                continue
            cp.read(os.path.join(config_dir, fname))
    return cp

