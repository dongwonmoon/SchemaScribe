"""
This module provides `NotionWriter`, an implementation of `BaseWriter` for Notion.

It uploads a generated data catalog to a new page within a specified parent
page in Notion, using the `notion-client` library. It dynamically transforms
catalog data into appropriate Notion blocks.
"""

import os
from typing import Dict, Any, List, Optional
from notion_client import Client, APIResponseError

from schema_scribe.core.interfaces import BaseWriter
from schema_scribe.core.exceptions import WriterError, ConfigError
from schema_scribe.utils.logger import get_logger

logger = get_logger(__name__)


class NotionWriter(BaseWriter):
    """
    Implements `BaseWriter` to write a data catalog to a new Notion page.

    This writer connects to the Notion API and constructs a new page with the
    catalog content. It orchestrates the process by:
    1.  Connecting to the Notion API.
    2.  Dynamically generating a list of Notion blocks based on the catalog
        type (distinguishing between DB and dbt project catalogs).
    3.  Creating a new page under a specified parent with the generated blocks.
    """

    def __init__(self):
        """Initializes the NotionWriter."""
        self.notion: Optional[Client] = None
        self.params: Dict[str, Any] = {}
        logger.info("NotionWriter initialized")

    def _connect(self):
        """
        Initializes the connection to the Notion API using the provided token.

        It resolves the API token, which can be provided directly or as an
        environment variable reference (e.g., `${NOTION_API_KEY}`).
        """
        token = self.params.get("api_token")
        if token and token.startswith("${") and token.endswith("}"):
            env_var = token[2:-1]
            token = os.getenv(env_var)
            if not token:
                raise ConfigError(
                    f"Environment variable '{env_var}' is required but not set."
                )

        if not token:
            raise ConfigError(
                "'api_token' (or env var) is required for NotionWriter."
            )

        try:
            self.notion = Client(auth=token)
            logger.info("Successfully connected to Notion API.")
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Notion: {e}") from e

    def write(self, catalog_data: Dict[str, Any], **kwargs):
        """
        Writes the catalog data to a new Notion page.

        This is the main entry point that orchestrates the connection, block
        generation, and page creation process.

        Args:
            catalog_data: The structured data catalog to be written.
            **kwargs: Configuration parameters. Must include `api_token` and
                      `parent_page_id`. `project_name` is optional.

        Raises:
            ConfigError: If required configuration is missing.
            WriterError: If there's an error generating blocks or creating the page.
        """
        self.params = kwargs
        self._connect()

        parent_page_id = self.params.get("parent_page_id")
        if not parent_page_id:
            raise ConfigError("'parent_page_id' is required for NotionWriter.")

        project_name = kwargs.get("project_name", "Data Catalog")
        page_title = f"Data Catalog - {project_name}"

        try:
            blocks = self._generate_notion_blocks(catalog_data)
            logger.info(f"Creating new Notion page: '{page_title}'")
            self.notion.pages.create(
                parent={"page_id": parent_page_id},
                properties={
                    "title": [{"type": "text", "text": {"content": page_title}}]
                },
                children=blocks,
            )
            logger.info("Successfully created Notion page.")
        except APIResponseError as e:
            raise WriterError(
                f"Failed to create Notion page. Check API key and Page ID permissions: {e}"
            ) from e
        except Exception as e:
            raise WriterError(
                f"An unexpected error occurred during Notion page creation: {e}"
            ) from e

    def _text_cell(self, content: str) -> List[Dict[str, Any]]:
        """Creates a Notion table cell with plain text content."""
        return [{"type": "text", "text": {"content": content or ""}}]

    def _H2(self, text: str) -> Dict[str, Any]:
        """Creates a Notion Heading 2 block."""
        return {
            "object": "block",
            "type": "heading_2",
            "heading_2": {"rich_text": [{"text": {"content": text}}]},
        }

    def _H3(self, text: str) -> Dict[str, Any]:
        """Creates a Notion Heading 3 block."""
        return {
            "object": "block",
            "type": "heading_3",
            "heading_3": {"rich_text": [{"text": {"content": text}}]},
        }

    def _Para(self, text: str) -> Dict[str, Any]:
        """Creates a Notion Paragraph block."""
        return {
            "object": "block",
            "type": "paragraph",
            "paragraph": {"rich_text": [{"text": {"content": text}}]},
        }

    def _Code(self, text: str, lang: str = "sql") -> Dict[str, Any]:
        """Creates a Notion Code block."""
        return {
            "object": "block",
            "type": "code",
            "code": {
                "rich_text": [{"text": {"content": text}}],
                "language": lang,
            },
        }

    def _clean_mermaid_code(self, code: str) -> str:
        """Removes Mermaid code fences (```mermaid ... ```) if they exist."""
        return code.replace("```mermaid", "").replace("```", "").strip()

    def _generate_notion_blocks(
        self, catalog_data: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Dynamically generates Notion blocks by detecting the catalog structure.

        It heuristically determines if the catalog is from a 'db' or 'dbt'
        workflow and calls the appropriate block generator.

        Args:
            catalog_data: The structured data catalog.

        Returns:
            A list of dictionaries, where each is a valid Notion block.
        """
        if "tables" in catalog_data and "views" in catalog_data:
            logger.info(
                "Detected 'db' catalog structure. Generating DB blocks."
            )
            return self._generate_db_blocks(catalog_data)
        elif any(
            isinstance(v, dict) and "columns" in v
            for v in catalog_data.values()
        ):
            logger.info(
                "Detected 'dbt' catalog structure. Generating dbt blocks."
            )
            return self._generate_dbt_blocks(catalog_data)
        else:
            logger.warning(
                "Unknown catalog structure. Generating basic blocks."
            )
            return [self._Para("Unknown catalog structure provided.")]

    def _create_column_table(
        self, columns: List[Dict[str, Any]], is_dbt: bool = False
    ) -> Dict[str, Any]:
        """
        Creates a Notion Table block to display column details.

        Args:
            columns: A list of column dictionaries.
            is_dbt: If True, expects descriptions to be nested under 'ai_generated'.

        Returns:
            A dictionary representing a Notion Table block.
        """
        header = {
            "type": "table_row",
            "table_row": {
                "cells": [
                    self._text_cell("Column Name"),
                    self._text_cell("Data Type"),
                    self._text_cell("AI-Generated Description"),
                ]
            },
        }
        rows = [header]
        for col in columns:
            desc = (
                col.get("ai_generated", {}).get("description", "(N/A)")
                if is_dbt
                else col.get("description", "N/A")
            )
            rows.append(
                {
                    "type": "table_row",
                    "table_row": {
                        "cells": [
                            self._text_cell(col.get("name")),
                            self._text_cell(col.get("type")),
                            self._text_cell(desc),
                        ]
                    },
                }
            )
        return {
            "object": "block",
            "type": "table",
            "table": {
                "table_width": 3,
                "has_column_header": True,
                "children": rows,
            },
        }

    def _generate_mermaid_erd(self, foreign_keys: List[Dict[str, str]]) -> str:
        """
        Generates Mermaid ERD code from foreign key data.
        """
        if not foreign_keys:
            return "erDiagram\n"

        code = ["erDiagram"]
        for fk in foreign_keys:
            source_table = fk["source_table"]
            target_table = fk["target_table"]
            label = f"{fk['source_column']} to {fk['target_column']}"
            code.append(
                f'    "{source_table}" ||--o{{ "{target_table}" : "{label}"'
            )
        return "\n".join(code)

    def _generate_db_blocks(
        self, catalog_data: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Generates a list of Notion blocks for a traditional database catalog.
        """
        blocks = []
        blocks.append(self._H2("🚀 Entity Relationship Diagram (ERD)"))
        mermaid_code = self._generate_mermaid_erd(
            catalog_data.get("foreign_keys", [])
        )
        blocks.append(self._Code(mermaid_code, "mermaid"))

        blocks.append(self._H2("🔎 Views"))
        views = catalog_data.get("views", [])
        if not views:
            blocks.append(self._Para("No views found in this database."))
        else:
            for view in views:
                blocks.append(self._H3(f"View: {view['name']}"))
                blocks.append(
                    self._Para(f"AI Summary: {view.get('ai_summary', 'N/A')}")
                )
                blocks.append(
                    self._Code(view.get("definition", "N/A"), lang="sql")
                )

        blocks.append(self._H2("🗂️ Tables"))
        tables = catalog_data.get("tables", [])
        if not tables:
            blocks.append(self._Para("No tables found in this database."))
        else:
            for table in tables:
                blocks.append(self._H3(f"Table: {table['name']}"))
                if table.get("ai_summary"):
                    blocks.append(
                        self._Para(f"AI Summary: {table['ai_summary']}")
                    )
                blocks.append(
                    self._create_column_table(
                        table.get("columns", []), is_dbt=False
                    )
                )
        return blocks

    def _generate_dbt_blocks(
        self, catalog_data: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Generates a list of Notion blocks for a dbt project catalog.
        """
        blocks = []
        for model_name, model_data in catalog_data.items():
            blocks.append(self._H2(f"🧬 Model: {model_name}"))
            blocks.append(self._H3("AI-Generated Model Summary"))
            blocks.append(
                self._Para(
                    model_data.get(
                        "model_description", "(No summary available)"
                    )
                )
            )
            blocks.append(self._H3("AI-Generated Lineage (Mermaid)"))
            mermaid_code = model_data.get(
                "model_lineage_chart", "graph TD; A[N/A];"
            )
            cleaned_code = self._clean_mermaid_code(mermaid_code)
            blocks.append(self._Code(cleaned_code, "mermaid"))
            blocks.append(self._H3("Column Details"))
            blocks.append(
                self._create_column_table(
                    model_data.get("columns", []), is_dbt=True
                )
            )
        return blocks
