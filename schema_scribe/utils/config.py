"""
This module manages the application's configuration settings by loading them
from environment variables.

It uses `python-dotenv` to automatically load variables from a `.env` file
(ideal for local development) and provides a singleton `settings` object for
globally consistent access to these values.
"""

import os
from dotenv import load_dotenv

# Load environment variables from a .env file into the application's environment.
# This is called at the module level to ensure that environment variables are
# available as soon as the `settings` object is imported elsewhere.
load_dotenv()


class Settings:
    """
    A centralized class for managing application settings from environment variables.

    This class provides a single, typed interface for all configuration variables
    that are loaded from the environment. It acts as a single source of truth.

    Usage:
        from schema_scribe.utils.config import settings
        api_key = settings.openai_api_key
    """

    def __init__(self):
        """
        Initializes the Settings object by loading values from the environment.

        To add a new setting, declare it as a class attribute and load it from
        the environment using `os.getenv`. For example:
        `self.new_key: str | None = os.getenv("NEW_KEY")`
        """
        # Load the OpenAI API key from the `OPENAI_API_KEY` environment variable.
        self.openai_api_key: str | None = os.getenv("OPENAI_API_KEY")

        # Load the Google API key from the `GOOGLE_API_KEY` environment variable.
        self.google_api_key: str | None = os.getenv("GOOGLE_API_KEY")


# Create a single, globally accessible instance of the Settings class.
# This singleton pattern ensures that settings are loaded only once and are
# consistently available throughout the application, preventing discrepancies.
settings = Settings()
