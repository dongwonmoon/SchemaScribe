"""
This module contains general utility functions for the Data Scribe application.
"""

import yaml


def load_config(config_file: str):
    """
    Loads a configuration from a YAML file.

    Args:
        config_file: The path to the YAML configuration file.

    Returns:
        A dictionary containing the loaded configuration.

    Raises:
        FileNotFoundError: If the configuration file is not found.
        yaml.YAMLError: If there is an error parsing the YAML file.
    """
    # Open the specified file in read mode
    with open(config_file, "r") as file:
        # Use yaml.safe_load to parse the YAML content safely
        config = yaml.safe_load(file)
        return config
