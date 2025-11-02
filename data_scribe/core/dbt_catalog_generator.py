"""
This module contains the logic for generating a data catalog from a dbt project.

It defines the DbtCatalogGenerator class, which parses a dbt manifest file,
extracts model and column information, and uses an LLM to generate descriptions.
"""

from typing import Dict, Any

from data_scribe.core.interfaces import BaseLLMClient
from data_scribe.core.dbt_parser import DbtManifestParser
from data_scribe.prompts import DBT_MODEL_PROMPT, DBT_COLUMN_PROMPT
from data_scribe.utils.logger import get_logger

# Initialize a logger for this module
logger = get_logger(__name__)


class DbtCatalogGenerator:
    """
    Parses a dbt manifest and uses an LLM to generate documentation for dbt models and columns.
    """

    def __init__(self, llm_client: BaseLLMClient):
        """
        Initializes the DbtCatalogGenerator with an LLM client.

        Args:
            llm_client: An instance of a class that implements the BaseLLMClient interface.
        """
        self.llm_client = llm_client

    def generate_catalog(self, dbt_project_dir: str) -> Dict[str, Any]:
        """
        Orchestrates the generation of a dbt data catalog.

        This method involves parsing the dbt manifest, iterating through the models,
        and generating AI-based descriptions for both the models and their columns.

        Args:
            dbt_project_dir: The root directory of the dbt project.

        Returns:
            A dictionary representing the data catalog, with model names as keys.
        """
        logger.info(f"dbt catalog generation started for {dbt_project_dir}")
        # Initialize the manifest parser with the project directory
        parser = DbtManifestParser(dbt_project_dir)
        # Parse the manifest to get a list of models
        models = parser.parse_models()

        catalog_data = {}

        # Iterate over each parsed dbt model
        for model in models:
            model_name = model["name"]
            raw_sql = model["raw_sql"]
            logger.info(f"Processing dbt model: {model_name}")

            # Generate a description for the model itself
            model_prompt = DBT_MODEL_PROMPT.format(
                model_name=model_name, raw_sql=raw_sql
            )
            model_description = self.llm_client.get_description(
                model_prompt, max_tokens=200
            )

            enriched_columns = []
            # Iterate over each column in the model
            for column in model["columns"]:
                col_name = column["name"]
                col_type = column["type"]

                # Generate a description for the column
                col_prompt = DBT_COLUMN_PROMPT.format(
                    model_name=model_name,
                    col_name=col_name,
                    col_type=col_type,
                    raw_sql=raw_sql,
                )
                col_desc = self.llm_client.get_description(
                    col_prompt, max_tokens=50
                )

                enriched_columns.append(
                    {
                        "name": col_name,
                        "type": col_type,
                        "description": col_desc,
                    }
                )

            # Store the model's description and its enriched columns in the catalog
            catalog_data[model_name] = {
                "model_description": model_description,
                "columns": enriched_columns,
            }

        logger.info("dbt catalog generation finished.")
        return catalog_data
