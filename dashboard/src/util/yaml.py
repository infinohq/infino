import logging
import yaml

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def read_config(file_path):
    with open(file_path, "r") as stream:
        try:
            config = yaml.safe_load(stream)
            return config
        except yaml.YAMLError as exc:
            logger.critical(f"Error reading YAML file: {exc}")
            return None
