from typing import List, Dict, Any

from data_scribe.core.interfaces import BaseConnector, BaseLLMClient
from data_scribe.prompts import COLUMN_DESCRIPTION_PROMPT
from data_scribe.utils.logger import get_logger

# Initialize logger
logger = get_logger(__name__)


class CatalogGenerator:
    """
    Handles the core logic of generating a data catalog.

    This class orchestrates the process of scanning a database schema,
    fetching table and column information, and using an LLM to generate
    business-friendly descriptions for each column.
    """

    def __init__(self, db_connector: BaseConnector, llm_client: BaseLLMClient):
        """
        Initializes the CatalogGenerator with a database connector and an LLM client.

        Args:
            db_connector: An instance of a class that implements the BaseConnector interface.
            llm_client: An instance of a class that implements the BaseLLMClient interface.
        """
        self.db_connector = db_connector
        self.llm_client = llm_client

    def generate_catalog(
        self, db_profile_name: str
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Generates a data catalog by fetching schema information from the database
        and enriching it with descriptions from an LLM.

        Args:
            db_profile_name: The name of the database profile being used.

        Returns:
            A dictionary representing the data catalog. The keys are table names,
            and the values are lists of dictionaries, where each dictionary
            represents a column with its name, type, and AI-generated description.
        """
        catalog_data = {}
        logger.info(f"Fetching tables for database profile: {db_profile_name}")
        tables = self.db_connector.get_tables()
        logger.info(f"Found {len(tables)} tables: {tables}")

        for table_name in tables:
            logger.info(f"Processing table: {table_name}")
            columns = self.db_connector.get_columns(table_name)
            enriched_columns = []

            for column in columns:
                col_name = column["name"]
                col_type = column["type"]
                logger.info(
                    f"  - Generating description for column: {col_name} ({col_type})"
                )

                # Format the prompt for the LLM
                prompt = COLUMN_DESCRIPTION_PROMPT.format(
                    table_name=table_name, col_name=col_name, col_type=col_type
                )

                # Get the column description from the LLM
                description = self.llm_client.get_description(
                    prompt, max_tokens=50
                )

                enriched_columns.append(
                    {
                        "name": col_name,
                        "type": col_type,
                        "description": description,
                    }
                )
            catalog_data[table_name] = enriched_columns
            logger.info(f"Finished processing table: {table_name}")

        return catalog_data
