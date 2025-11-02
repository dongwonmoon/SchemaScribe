from typing import Dict, Any

from data_scribe.core.interfaces import BaseLLMClient
from data_scribe.core.dbt_parser import DbtManifestParser
from data_scribe.prompts import DBT_MODEL_PROMPT, DBT_COLUMN_PROMPT
from data_scribe.utils.logger import get_logger

logger = get_logger(__name__)


class DbtCatalogGenerator:
    """
    Parses the dbt manifest and calls LLM to generate documentation for the dbt model/column.
    """

    def __init__(self, llm_client: BaseLLMClient):
        self.llm_client = llm_client

    def generate_catalog(self, dbt_project_dir: str) -> Dict[str, Any]:
        """
        Orchestrates the creation of dbt catalogs.

        Returns:
            catalog_data
        """
        logger.info(f"dbt catalog generation started for {dbt_project_dir}")
        parser = DbtManifestParser(dbt_project_dir)
        models = parser.parse_models()

        catalog_data = {}

        for model in models:
            model_name = model["name"]
            raw_sql = model["raw_sql"]
            logger.info(f"Processing dbt model: {model_name}")

            model_prompt = DBT_MODEL_PROMPT.format(
                model_name=model_name, raw_sql=raw_sql
            )
            model_description = self.llm_client.get_description(
                model_prompt, max_tokens=200
            )

            enriched_columns = []
            for column in model["columns"]:
                col_name = column["name"]
                col_type = column["type"]

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

            catalog_data[model_name] = {
                "model_description": model_description,
                "columns": enriched_columns,
            }

        logger.info("dbt catalog generation finished.")
        return catalog_data
