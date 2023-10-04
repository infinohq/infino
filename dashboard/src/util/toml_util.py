import logging
import toml

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def get_server_url(toml_file_path):
    # Read the TOML file
    with open(toml_file_path, "r") as toml_file:
        config = toml.load(toml_file)

    # Access the values in the configuration
    server_url = config["server"]["url"]

    return server_url
