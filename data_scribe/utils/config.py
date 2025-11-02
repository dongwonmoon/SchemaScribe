"""
This module handles the application's settings, primarily by loading them from environment variables.

It uses the `python-dotenv` library to load variables from a `.env` file and makes them
available through a `Settings` class.
"""

import os
from dotenv import load_dotenv

# Load environment variables from a .env file into the environment.
# This allows for configuration of sensitive data like API keys without hardcoding them.
load_dotenv()


class Settings:
    """
    Manages application settings by loading them from environment variables.

    This class centralizes access to settings, making it easy to add new ones
    and to mock them for testing purposes.
    """

    def __init__(self):
        """
        Initializes the Settings class.

        This is where you can add new settings to be loaded from the environment.
        For example, to add support for other services like Anthropic's Claude, you would add:
        self.claude_api_key: str | None = os.getenv("CLAUDE_API_KEY")
        """
        # Load the OpenAI API key from the environment variable `OPENAI_API_KEY`
        self.openai_api_key: str | None = os.getenv("OPENAI_API_KEY")


# Create a single, globally accessible instance of the Settings class.
# This instance should be imported and used by other modules that need access to settings.
settings = Settings()
