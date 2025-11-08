"""
This module contains the logic for generating a data catalog from a dbt project.

It defines the DbtCatalogGenerator class, which parses a dbt manifest file,
extracts model and column information, and uses an LLM to generate descriptions.
"""

from typing import Dict, Any
from ruamel.yaml import YAML

from data_scribe.core.interfaces import BaseLLMClient
from data_scribe.core.dbt_parser import DbtManifestParser
from data_scribe.prompts import (
    DBT_MODEL_PROMPT,
    DBT_COLUMN_PROMPT,
    DBT_MODEL_LINEAGE_PROMPT,
)
from data_scribe.utils.logger import get_logger

# Initialize a logger for this module
logger = get_logger(__name__)


class DbtCatalogGenerator:
    """
    Parses a dbt manifest and uses an LLM to generate documentation.

    This class is responsible for generating AI-powered descriptions for dbt models,
    structured metadata (tags, tests) for columns, and Mermaid.js lineage charts.
    """

    def __init__(self, llm_client: BaseLLMClient):
        """
        Initializes the DbtCatalogGenerator with an LLM client.

        Args:
            llm_client: An instance of a class that implements the BaseLLMClient interface.
        """
        self.llm_client = llm_client
        self.yaml_parser = YAML()
        logger.info("DbtCatalogGenerator initialized.")

    def generate_catalog(self, dbt_project_dir: str) -> Dict[str, Any]:
        """
        Orchestrates the generation of a dbt data catalog.

        This method involves parsing the dbt manifest, iterating through the models,
        and generating AI-based descriptions for models, columns, and model lineage.

        Args:
            dbt_project_dir: The root directory of the dbt project.

        Returns:
            A dictionary representing the data catalog, structured as follows:
            {
                "model_name": {
                    "model_description": "AI-generated model summary.",
                    "model_lineage_chart": "```mermaid\n...\n```",
                    "columns": [
                        {
                            "name": "column_name",
                            "type": "column_type",
                            "ai_generated": {
                                "description": "AI-generated column description.",
                                "meta": { "pii": True/False },
                                "tags": [ ... ],
                                "tests": [ ... ]
                            }
                        },
                        ...
                    ]
                },
                ...
            }
        """
        logger.info(f"dbt catalog generation started for {dbt_project_dir}")
        # Initialize the manifest parser and extract dbt models.
        parser = DbtManifestParser(dbt_project_dir)
        models = parser.models

        catalog_data = {}

        # Iterate over each parsed dbt model to generate documentation.
        for model in models:
            model_name = model["name"]
            raw_sql = model["raw_sql"]
            logger.info(f"Processing dbt model: {model_name}")

            # 1. Generate a high-level description for the dbt model.
            model_prompt = DBT_MODEL_PROMPT.format(
                model_name=model_name, raw_sql=raw_sql
            )
            model_description = self.llm_client.get_description(
                model_prompt, max_tokens=200
            )

            # 2. Generate a Mermaid.js lineage chart for the model's direct parents.
            logger.info(f"  - Generating Mermaid lineage for: {model_name}")
            lineage_prompt = DBT_MODEL_LINEAGE_PROMPT.format(
                model_name=model_name, raw_sql=raw_sql
            )
            mermaid_chart_block = self.llm_client.get_description(
                lineage_prompt, max_tokens=1000
            )

            enriched_columns = []
            # 3. For each column, generate a structured YAML block of metadata.
            for column in model["columns"]:
                col_name = column["name"]
                col_type = column["type"]

                col_prompt = DBT_COLUMN_PROMPT.format(
                    model_name=model_name,
                    col_name=col_name,
                    col_type=col_type,
                    raw_sql=raw_sql,
                )
                # The prompt asks the LLM to return a YAML snippet.
                yaml_snippet_str = self.llm_client.get_description(
                    col_prompt, max_tokens=250
                )

                # Try to parse the LLM's response as YAML to get structured data.
                # If parsing fails, the raw response is used as the description,
                # ensuring robustness against malformed AI outputs.
                try:
                    ai_data_dict = self.yaml_parser.load(yaml_snippet_str)
                    if not isinstance(ai_data_dict, dict):
                        raise ValueError(
                            "AI did not return a valid YAML mapping."
                        )
                except Exception as e:
                    logger.error(
                        f"AI YAML snippet parsing failed for {model_name}.{col_name}: {e}"
                    )
                    logger.debug(f"Failed snippet:\n{yaml_snippet_str}")
                    ai_data_dict = {"description": yaml_snippet_str.strip()}

                enriched_columns.append(
                    {
                        "name": col_name,
                        "type": col_type,
                        "ai_generated": ai_data_dict,
                    }
                )

            # 4. Assemble all generated content for the model into the catalog.
            catalog_data[model_name] = {
                "model_description": model_description,
                "model_lineage_chart": mermaid_chart_block,
                "columns": enriched_columns,
            }

        logger.info("dbt catalog generation finished.")
        return catalog_data
