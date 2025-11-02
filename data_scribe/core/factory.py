from typing import Dict, Type, Any

from data_scribe.core.interfaces import BaseConnector, BaseLLMClient
from data_scribe.components.db_connectors.sqlite_connector import (
    SQLiteConnector,
)
from data_scribe.components.llm_clients.openai_client import OpenAIClient
from data_scribe.utils.logger import get_logger

# Initialize logger
logger = get_logger(__name__)

# Registry for database connectors
# Maps a string identifier to a connector class.
DB_CONNECTOR_REGISTRY: Dict[str, Type[BaseConnector]] = {
    "sqlite": SQLiteConnector,
}

# Registry for LLM clients
# Maps a string identifier to a client class.
LLM_CLIENT_REGISTRY: Dict[str, Type[BaseLLMClient]] = {
    "openai": OpenAIClient,
}


def get_db_connector(type_name: str, params: Dict[str, Any]) -> BaseConnector:
    """
    Instantiates a database connector based on the provided type name.

    This factory function looks up the connector class in the DB_CONNECTOR_REGISTRY
    and returns an initialized instance.

    Args:
        type_name: The type of the database connector to create (e.g., 'sqlite').
        params: A dictionary of parameters to pass to the connector's connect method.

    Returns:
        An instance of a class that implements the BaseConnector interface.

    Raises:
        ValueError: If the specified connector type is not supported.
    """
    logger.info(f"Looking up database connector for type: {type_name}")
    connector_class = DB_CONNECTOR_REGISTRY.get(type_name)

    if not connector_class:
        logger.error(f"Unsupported database connector type: {type_name}")
        raise ValueError(f"Unsupported database connector type: {type_name}")

    logger.info(f"Instantiating {connector_class.__name__}...")
    connector = connector_class()
    connector.connect(params)
    return connector


def get_llm_client(provider_name: str, params: Dict[str, Any]) -> BaseLLMClient:
    """
    Instantiates an LLM client based on the provided provider name.

    This factory function looks up the client class in the LLM_CLIENT_REGISTRY
    and returns an initialized instance.

    Args:
        provider_name: The name of the LLM provider (e.g., 'openai').
        params: A dictionary of parameters to pass to the client's constructor.

    Returns:
        An instance of a class that implements the BaseLLMClient interface.

    Raises:
        ValueError: If the specified LLM provider is not supported.
    """
    logger.info(f"Looking up LLM client for provider: {provider_name}")
    client_class = LLM_CLIENT_REGISTRY.get(provider_name)

    if not client_class:
        logger.error(f"Unsupported LLM provider: {provider_name}")
        raise ValueError(f"Unsupported LLM provider: {provider_name}")

    logger.info(f"Instantiating {client_class.__name__}...")
    return client_class(**params)
