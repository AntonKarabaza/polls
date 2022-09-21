import pathlib
import yaml

from common.constants import BASE_DIR

config_path = BASE_DIR / 'config' / 'polls.yaml'


def get_config(path: pathlib.Path):
    with open(path) as config_file:
        config = yaml.safe_load(config_file)
    return config


config = get_config(config_path)
