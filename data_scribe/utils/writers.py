from typing import Dict, List, Any
from data_scribe.utils.logger import get_logger

# Initialize logger
logger = get_logger(__name__)


class MarkdownWriter:
    """
    Handles writing the generated data catalog to a Markdown file.
    """

    def write(
        self,
        catalog_data: Dict[str, List[Dict[str, Any]]],
        output_filename: str,
        db_profile_name: str,
    ):
        """
        Writes the catalog data to a Markdown file.

        Args:
            catalog_data: A dictionary containing the catalog data.
            output_filename: The name of the file to write the catalog to.
            db_profile_name: The name of the database profile used.
        """
        try:
            with open(output_filename, "w", encoding="utf-8") as f:
                logger.info(
                    f"Writing data catalog for '{db_profile_name}' to '{output_filename}'."
                )
                f.write(f"# 📁 Data Catalog for {db_profile_name}\n")

                for table_name, columns in catalog_data.items():
                    f.write(f"\n## 📄 Table: `{table_name}`\n\n")
                    f.write(
                        "| Column Name | Data Type | AI-Generated Description |\n"
                    )
                    f.write("| :--- | :--- | :--- |\n")

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
        Writes the dbt catalog data to a Markdown file.

        Args:
            catalog_data: A dictionary containing the dbt catalog data.
            output_filename: The name of the file to write the catalog to.
            project_name: The name of the dbt project (for the title).
        """
        try:
            with open(output_filename, "w", encoding="utf-8") as f:
                logger.info(
                    f"Writing dbt catalog for '{project_name}' to '{output_filename}'."
                )
                f.write(f"# 🧬 Data Catalog for {project_name} (dbt)\n")

                for model_name, model_data in catalog_data.items():
                    f.write(f"\n## 🚀 Model: `{model_name}`\n\n")

                    f.write("### AI-Generated Model Summary\n")
                    f.write(
                        f"{model_data.get('model_description', '(No summary available)')}\n\n"
                    )

                    f.write("### Column Details\n")
                    f.write(
                        "| Column Name | Data Type | AI-Generated Description |\n"
                    )
                    f.write("| :--- | :--- | :--- |\n")

                    columns = model_data.get("columns", [])
                    if not columns:
                        f.write("| (No columns found) | | |\n")
                        continue

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
            raise
