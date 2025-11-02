import os
from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()


class Settings:
    """
    Manages application settings, primarily by loading them from environment variables.
    """

    def __init__(self):
        """
        Initializes the Settings class.
        This is where you can add new settings to be loaded from the environment.
        For example, to add support for other services like Claude, you would add:
        self.claude_api_key: str | None = os.getenv("CLAUDE_API_KEY")
        """
        # Load the OpenAI API key from the environment variable
        self.openai_api_key: str | None = os.getenv("OPENAI_API_KEY")


# Create a single instance of the Settings class to be used throughout the application
settings = Settings()
