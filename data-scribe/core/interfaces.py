from abc import ABC, abstractmethod
from typing import List, Dict, Any


class BaseLLMClient(ABC):
    """Abstract base class for LLM clients."""

    @abstractmethod
    def get_description(self, prompt: str, max_tokens: int) -> str:
        """Generates a description for a given prompt."""
        pass


class BaseConnector(ABC):
    """Abstract base class for database connectors."""

    @abstractmethod
    def connect(self, db_params: Dict[str, Any]):
        """Connects to the database."""
        pass

    @abstractmethod
    def get_tables(self) -> List[str]:
        """Retrieves a list of table names from the database."""
        pass

    @abstractmethod
    def get_columns(self, table_name: str) -> List[Dict[str, str]]:
        """Retrieves column information for a given table."""
        pass

    @abstractmethod
    def close(self):
        """Closes the database connection."""
        pass
