"""
This module provides classes for writing the generated data catalog to different output formats.

Currently, it includes `MarkdownWriter` for standard database catalogs and `DbtMarkdownWriter`
for catalogs generated from dbt projects. Both writers produce Markdown files.
"""

from typing import Dict, List, Any
import os
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap

from data_scribe.utils.logger import get_logger

# Initialize a logger for this module
logger = get_logger(__name__)


class MarkdownWriter:
    """
    Handles writing the generated database catalog to a Markdown file.
    """

    def write(
        self,
        catalog_data: Dict[str, List[Dict[str, Any]]],
        output_filename: str,
        db_profile_name: str,
    ):
        """
        Writes the catalog data to a Markdown file in a structured table format.

        Args:
            catalog_data: A dictionary where keys are table names and values are lists of column dictionaries.
            output_filename: The name of the file to write the catalog to.
            db_profile_name: The name of the database profile used, for the report title.
        """
        try:
            with open(output_filename, "w", encoding="utf-8") as f:
                logger.info(
                    f"Writing data catalog for '{db_profile_name}' to '{output_filename}'."
                )
                # Write the main title of the Markdown file
                f.write(f"# 📁 Data Catalog for {db_profile_name}\n")

                # Iterate over each table in the catalog data
                for table_name, columns in catalog_data.items():
                    f.write(f"\n## 📄 Table: `{table_name}`\n\n")
                    # Write the header of the column table
                    f.write("| Column Name | Data Type | AI-Generated Description |\n")
                    f.write("| :--- | :--- | :--- |\n")

                    # Write a row for each column
                    for column in columns:
                        col_name = column["name"]
                        col_type = column["type"]
                        description = column["description"]
                        f.write(f"| `{col_name}` | `{col_type}` | {description} |\n")
            logger.info("Finished writing catalog file.")
        except IOError as e:
            logger.error(
                f"Error writing to file '{output_filename}': {e}", exc_info=True
            )
            # Re-raise the exception to be handled by the CLI
            raise


class DbtMarkdownWriter:
    """
    Handles writing the generated dbt catalog to a Markdown file.
    """

    def write(
        self,
        catalog_data: Dict[str, Any],
        output_filename: str,
        project_name: str,
    ):
        """
        Writes the dbt catalog data to a Markdown file, including model summaries and column details.

        Args:
            catalog_data: A dictionary containing the dbt catalog data.
            output_filename: The name of the file to write the catalog to.
            project_name: The name of the dbt project, for the report title.
        """
        try:
            with open(output_filename, "w", encoding="utf-8") as f:
                logger.info(
                    f"Writing dbt catalog for '{project_name}' to '{output_filename}'."
                )
                # Write the main title of the Markdown file
                f.write(f"# 🧬 Data Catalog for {project_name} (dbt)\n")

                # Iterate over each model in the catalog data
                for model_name, model_data in catalog_data.items():
                    f.write(f"\n## 🚀 Model: `{model_name}`\n\n")

                    # Write the AI-generated summary for the model
                    f.write("### AI-Generated Model Summary\n")
                    f.write(
                        f"{model_data.get('model_description', '(No summary available)')}\n\n"
                    )

                    # Write the header for the column details table
                    f.write("### Column Details\n")
                    f.write("| Column Name | Data Type | AI-Generated Description |\n")
                    f.write("| :--- | :--- | :--- |\n")

                    columns = model_data.get("columns", [])
                    if not columns:
                        f.write("| (No columns found) | | |\n")
                        continue

                    # Write a row for each column in the model
                    for column in columns:
                        col_name = column["name"]
                        col_type = column["type"]
                        description = column["description"]
                        f.write(f"| `{col_name}` | `{col_type}` | {description} |\n")

            logger.info("Finished writing dbt catalog file.")
        except IOError as e:
            logger.error(
                f"Error writing to file '{output_filename}': {e}", exc_info=True
            )
            # Re-raise the exception to be handled by the CLI
            raise


class DbtYamlWriter:
    """
    Handles reading dbt schema.yml files, updating them
    with AI-generated descriptions, and writing them back
    while preserving comments and formatting.
    """

    def __init__(self, dbt_project_dir: str):
        self.dbt_project_dir = dbt_project_dir
        self.yaml = YAML()
        self.yaml.preserve_quotes = True
        self.yaml.indent(mapping=2, sequence=4, offset=2)
        logger.info("DbtYamlWriter initialized.")

    def find_schema_files(self) -> List[str]:
        """Finds all 'schema.yml' (or '.yml') files in the dbt 'models' directories."""
        model_path = os.path.join(self.dbt_project_dir, "models")
        schema_files = []
        for root, _, files in os.walk(model_path):
            for file in files:
                if file.endswith((".yml", ".yaml")):
                    if "schema" in file or "sources" in file:
                        schema_files.append(os.path.join(root, file))

        logger.info(f"Found schema files to check: {schema_files}")
        return schema_files

    def update_yaml_files(self, catalog_data: Dict[str, Any]):
        """
        Finds and updates all relevant schema.yml files with the catalog data.
        """
        schema_files = self.find_schema_files()
        if not schema_files:
            logger.warning("No schema.yml files found in 'models' directories.")
            return

        for file_path in schema_files:
            try:
                self._update_single_file(file_path, catalog_data)
            except Exception as e:
                logger.error(f"Error processing {file_path}: {e}", exc_info=True)

    def _update_single_file(self, file_path: str, catalog_data: Dict[str, Any]):
        """Updates a single schema.yml file."""
        with open(file_path, "r", encoding="utf-8") as f:
            data = self.yaml.load(f)

        if not data:
            logger.info(f"Skipping empty YAML file: {file_path}")
            return

        models = data.get("models", [])
        if not models:
            logger.info(f"No models found in YAML file: {file_path}")
            return

        logger.info(f"Checking '{file_path}' for models to update...")
        file_updated = False

        for model_config in models:
            model_name = model_config.get("name")

            if model_name in catalog_data:
                logger.info(f"  -> Found model '{model_name}' in YAML.")
                ai_model_data = catalog_data[model_name]

                if "columns" in model_config:
                    for column_config in model_config["columns"]:
                        column_name = column_config.get("name")

                        ai_column = next(
                            (
                                c
                                for c in ai_model_data["columns"]
                                if c["name"] == column_name
                            ),
                            None,
                        )

                        if ai_column:
                            if not column_config.get("description"):
                                logger.info(
                                    f"    - Updating description for column '{column_name}'"
                                )
                                column_config["description"] = ai_column["description"]
                                file_updated = True

        if file_updated:
            try:
                with open(file_path, "w", encoding="utf-8") as f:
                    self.yaml.dump(data, f)
                logger.info(f"Successfully updated '{file_path}' with AI descriptions.")
            except Exception as e:
                logger.error(f"Failed to write updates to '{file_path}': {e}")
        else:
            logger.info(
                f"No missing descriptions found in '{file_path}'. No changes made."
            )
