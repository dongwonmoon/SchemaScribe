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
    with open(config_file, "r") as file:
        config = yaml.safe_load(file)
        return config
