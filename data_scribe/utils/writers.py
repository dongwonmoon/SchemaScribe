"""
This module provides classes for writing the generated data catalog to different output formats.

Currently, it includes `MarkdownWriter` for standard database catalogs and `DbtMarkdownWriter`
for catalogs generated from dbt projects. Both writers produce Markdown files.
"""

from typing import Dict, List, Any
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
                    f.write(
                        "| Column Name | Data Type | AI-Generated Description |\n"
                    )
                    f.write("| :--- | :--- | :--- |\n")

                    # Write a row for each column
                    for column in columns:
                        col_name = column["name"]
                        col_type = column["type"]
                        description = column["description"]
                        f.write(
                            f"| `{col_name}` | `{col_type}` | {description} |\n"
                        )
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
                    f.write(
                        "| Column Name | Data Type | AI-Generated Description |\n"
                    )
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
                        f.write(
                            f"| `{col_name}` | `{col_type}` | {description} |\n"
                        )

            logger.info("Finished writing dbt catalog file.")
        except IOError as e:
            logger.error(
                f"Error writing to file '{output_filename}': {e}", exc_info=True
            )
            # Re-raise the exception to be handled by the CLI
            raise
