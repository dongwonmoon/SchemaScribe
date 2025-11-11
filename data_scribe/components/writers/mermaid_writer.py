"""
This module provides a simple writer for saving a raw Mermaid
string to a Markdown file.
"""

from typing import Dict, Any

from data_scribe.utils.logger import get_logger
from data_scribe.core.interfaces import BaseWriter
from data_scribe.core.exceptions import WriterError, ConfigError

logger = get_logger(__name__)

class MermaidWriter(BaseWriter):
    """
    Handles writing a single Mermaid chart string to a .md file.
    """

    def write(self, catalog_data: Dict[str, Any], **kwargs):
        """
        Writes the Mermaid chart to a Markdown file.

        Args:
            catalog_data: A dictionary expected to have a "mermaid_graph" key.
            **kwargs: Expects 'output_filename'.
        """
        output_filename = kwargs.get("output_filename")
        if not output_filename:
            logger.error("MermaidWriter 'write' method missing 'output_filename'.")
            raise ConfigError("Missing required kwargs for MermaidWriter.")

        mermaid_graph = catalog_data.get("mermaid_graph")
        if not mermaid_graph:
            logger.warning("No 'mermaid_graph' key found in catalog data. Writing empty file.")
            mermaid_graph = "graph TD;\n  A[No lineage data found];"

        try:
            with open(output_filename, "w", encoding="utf-8") as f:
                logger.info(f"Writing global lineage to '{output_filename}'...")
                f.write("# 🌐 Global Data Lineage\n\n")
                f.write("```mermaid\n")
                f.write(mermaid_graph)
                f.write("\n```\n")
            logger.info("Finished writing lineage file.")
        except IOError as e:
            logger.error(f"Error writing to file '{output_filename}': {e}", exc_info=True)
            raise WriterError(f"Error writing to file '{output_filename}': {e}") from e