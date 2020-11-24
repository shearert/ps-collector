
import configparser
import logging.config
import os


def get_config():
    cp = configparser.ConfigParser()
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


def setup_logging(cp):
    config_file = "/etc/ps-collector/logging-config.ini"
    if cp.has_option("General", "logging_configuration"):
        config_file = cp.get("General", "logging_configuration")
    logging.config.fileConfig(config_file)

