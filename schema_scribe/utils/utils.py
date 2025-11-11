"""
This module contains low-level utility functions for the application, primarily
focused on handling dynamic configuration by expanding environment variables
within YAML files.
"""

import os
import re
import yaml
from typing import Dict, Any
from schema_scribe.core.exceptions import ConfigError


def expand_env_vars(content: str) -> str:
    """
    Expands environment variables of the form `${VAR}` in a string.

    This allows for dynamic configuration values to be pulled from the environment,
    which is useful for sensitive data like API keys or passwords.

    Example:
        If `os.getenv("DB_PASSWORD")` is "mysecret", the input string
        `"password: ${DB_PASSWORD}"` would become `"password: mysecret"`.

    Args:
        content: The string content in which to expand environment variables.

    Returns:
        The string with all `${VAR}` placeholders replaced by their
        corresponding environment variable values.

    Raises:
        ConfigError: If an environment variable referenced in the string is not set.
    """
    pattern = re.compile(r"\$\{([A-Za-z0-9_]+)\}")

    def replacer(match):
        var_name = match.group(1)
        var_value = os.getenv(var_name)
        if var_value is None:
            raise ConfigError(
                f"Configuration error: Environment variable '{var_name}' is not set, "
                "but is referenced in the config file."
            )
        return var_value

    return pattern.sub(replacer, content)


def load_config(config_file: str) -> Dict[str, Any]:
    """
    Loads a configuration from a YAML file and expands environment variables.

    This function performs a two-step process:
    1.  Reads the raw YAML file into a string.
    2.  Expands any `${VAR}` placeholders in the string using environment variables.
    3.  Parses the resulting string as YAML.

    This approach allows for dynamic and secure configuration management.

    Args:
        config_file: The path to the YAML configuration file.

    Returns:
        A dictionary containing the loaded and parsed configuration.

    Raises:
        FileNotFoundError: If the configuration file is not found.
        yaml.YAMLError: If there is an error parsing the YAML file.
        ConfigError: If a referenced environment variable is not set.
    """
    with open(config_file, "r", encoding="utf-8") as file:
        raw_content = file.read()

    expanded_content = expand_env_vars(raw_content)
    config = yaml.safe_load(expanded_content)
    return config
