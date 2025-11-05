"""
This module defines the abstract base classes (interfaces) for the core components of Data Scribe.

These interfaces (`BaseLLMClient` and `BaseConnector`) ensure that different implementations
of LLM clients and database connectors adhere to a common contract. This makes the system
pluggable and easy to extend.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any


class BaseLLMClient(ABC):
    """Abstract base class for LLM clients.

    All LLM client implementations should inherit from this class and implement the
    `get_description` method.
    """

    @abstractmethod
    def get_description(self, prompt: str, max_tokens: int) -> str:
        """Generates a description for a given prompt.

        Args:
            prompt: The prompt to send to the language model.
            max_tokens: The maximum number of tokens to generate.

        Returns:
            The AI-generated description as a string.
        """
        pass


class BaseConnector(ABC):
    """Abstract base class for database connectors.

    All database connector implementations should inherit from this class and implement
    the `connect`, `get_tables`, `get_columns`, and `close` methods.
    """

    @abstractmethod
    def connect(self, db_params: Dict[str, Any]):
        """Connects to the database using the provided parameters."""
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
    def get_views(self) -> List[Dict[str, str]]:
        """Retrives a List of views and their definitions."""

    @abstractmethod
    def get_foreign_keys(self) -> List[Dict[str, str]]:
        """Retrieves all foreign key relationships in the database."""
        pass

    @abstractmethod
    def close(self):
        """Closes the database connection."""
        pass


class BaseWriter(ABC):
    @abstractmethod
    def write(self, catalog_data: Dict[str, Any], **kwargs):
        pass
