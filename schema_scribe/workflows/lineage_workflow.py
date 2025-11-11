"""
This module defines the LineageWorkflow, which orchestrates the
global lineage generation process using injected components.

Design Rationale:
This workflow combines physical (DB FKs) and logical (dbt) lineage.
Adhering to DI principles, it receives pre-initialized `db_connector`
and `writer` instances, making it modular and testable. It can be
executed via `run()` to save to a file (CLI) or via `generate_catalog()`
to return data directly (API).
"""

import typer
from typing import List, Dict, Any, Optional

from schema_scribe.core.interfaces import BaseConnector, BaseWriter
from schema_scribe.services.dbt_parser import DbtManifestParser
from schema_scribe.services.lineage_generator import GlobalLineageGenerator
from schema_scribe.utils.logger import get_logger

logger = get_logger(__name__)


class LineageWorkflow:
    """
    Orchestrates the global lineage generation workflow using
    injected component instances.
    """

    def __init__(
        self,
        db_connector: BaseConnector,
        writer: Optional[BaseWriter],
        dbt_project_dir: str,
        db_profile_name: str,
        output_profile_name: Optional[str],
        writer_params: Dict[str, Any],
    ):
        """
        Initializes the LineageWorkflow with all necessary dependencies.

        Args:
            db_connector: An initialized database connector instance.
            writer: An initialized writer instance (optional).
            dbt_project_dir: The absolute path to the dbt project directory.
            db_profile_name: The name of the database profile, for logging.
            output_profile_name: The name of the output profile, for logging (optional).
            writer_params: Additional parameters for the writer's `write` method.
        """
        self.db_connector = db_connector
        self.writer = writer
        self.dbt_project_dir = dbt_project_dir
        self.db_profile_name = db_profile_name
        self.output_profile_name = output_profile_name
        self.writer_params = writer_params

    def generate_catalog(self) -> Dict[str, Any]:
        """
        Generates lineage data in both Mermaid and JSON formats.

        This is the core data generation step, used by both `run()`
        and the API server.

        Returns:
            A dictionary containing 'mermaid_graph' (str) and 'graph_json' (dict).
        """
        # 1. Fetch Physical Lineage
        logger.info(f"Connecting to DB '{self.db_profile_name}' for FK scan...")
        db_fks = self.db_connector.get_foreign_keys()
        logger.info(f"Found {len(db_fks)} foreign key relationships.")

        # 2. Fetch Logical Lineage
        dbt_models = self._parse_dbt_models()

        # 3. Generate graph data (Service Layer)
        generator = GlobalLineageGenerator(db_fks, dbt_models)

        # Generate both formats
        mermaid_string = generator.generate_mermaid_string()
        graph_json = generator.generate_graph_json()

        catalog_data = {
            "mermaid_graph": mermaid_string,  # For MermaidWriter compatibility
            "graph_json": graph_json,  # For the new UI API
        }

        return catalog_data

    def run(self):
        """
        Executes the lineage generation and writes the result to a file
        (if a writer is provided).
        """
        catalog_data = None
        try:
            # 1. Generate data
            catalog_data = self.generate_catalog()

            # 2. Write to file (only if writer is injected)
            if not self.writer:
                logger.info(
                    "Lineage data generated. No writer provided, skipping file output."
                )
                return

            logger.info(
                f"Writing lineage graph using output profile: '{self.output_profile_name}'."
            )
            # The MermaidWriter will find the "mermaid_graph" key it needs
            self.writer.write(catalog_data, **self.writer_params)

            logger.info("Global lineage graph written successfully.")

        except Exception as e:
            logger.error(
                f"Failed to write lineage graph using profile '{self.output_profile_name}': {e}"
            )
            raise typer.Exit(code=1)
        finally:
            # 3. Cleanup resources
            if self.db_connector:
                logger.info(
                    f"Closing DB connection for {self.db_profile_name}..."
                )
                self.db_connector.close()

    def _parse_dbt_models(self) -> List[Dict[str, Any]]:
        """Internal helper to parse the dbt manifest."""
        logger.info(
            f"Parsing dbt project at '{self.dbt_project_dir}' for dependencies..."
        )
        parser = DbtManifestParser(self.dbt_project_dir)
        models = parser.models
        logger.info(f"Parsed {len(models)} dbt models.")
        return models
