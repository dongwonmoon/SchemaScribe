import google.generativeai as genai
from data_scribe.core.interfaces import BaseLLMClient
from data_scribe.utils.config import settings
from data_scribe.utils.logger import get_logger

logger = get_logger(__name__)


class GoogleGenAIClient(BaseLLMClient):
    """Client for Google Gemini LLM."""

    def __init__(self, model: str = "gemini-pro"):
        """
        Initializes the Google GenAI (Gemini) client.

        Args:
            model: The Gemini model to use, from config.yaml (e.g., 'gemini-pro')

        Raises:
            ValueError: If GOOGLE_API_KEY is not in the .env file
        """
        api_key = settings.google_api_key
        if not api_key:
            logger.error("GOOGLE_API_KEY environment variable not set.")
            raise ValueError(
                "GOOGLE_API_KEY must be set in the .env file to use GoogleGenAIClient."
            )

        try:
            logger.info(f"Initializing Google GenAI client. Model: {model}")
            genai.configure(api_key=api_key)
            self.model = genai.GenerativeModel(model)
            logger.info("Google GenAI client initialized successfully.")
        except Exception as e:
            logger.error(f"Failed to initialize Google GenAI client: {e}", exc_info=True)
            raise ValueError(f"Failed to initialize Google GenAI client: {e}")

    def get_description(self, prompt: str, max_tokens: int) -> str:
        """Generates a description using the Gemini LLM."""
        try:
            logger.info(
                f"Sending prompt to Google GenAI '{self.model.model_name}' model..."
            )

            # (Note) Gemini uses generation_config rather than directly controlling max_tokens
            # For simplicity, the max_tokens argument is not used here
            response = self.model.generate_content(prompt)

            description = response.text.strip()
            logger.info("Response received from Google GenAI.")
            return description
        except Exception as e:
            logger.error(f"Failed to generate description with Google GenAI: {e}", exc_info=True)
            return "(Google AI generation failed)"
