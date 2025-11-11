"""
This module defines the workflow for the `schema-scribe lineage` command.

Its key innovation is the ability to merge two different types of lineage into
a single, unified graph:
1.  **Physical Lineage**: Foreign key relationships from the live database.
2.  **Logical Lineage**: `ref()` and `source()` dependencies from a dbt project.

The result is a comprehensive, end-to-end data lineage graph that provides a
holistic view of data flow across the entire system.
"""

import typer
from typing import List, Dict, Any, Set

from schema_scribe.core.factory import get_db_connector, get_writer
from schema_scribe.core.dbt_parser import DbtManifestParser
from schema_scribe.core.workflow_helpers import load_config
from schema_scribe.utils.logger import get_logger

logger = get_logger(__name__)


class GlobalLineageGenerator:
    """
    Builds a global, unified lineage graph from multiple sources.

    This class takes a list of physical foreign key relationships and a list of
    parsed dbt models (containing logical dependencies) and merges them into a
    single graph structure.

    It uses a style priority system to ensure nodes are displayed correctly.
    For example, a dbt model that is also a database table will always be
    styled as a "model" (highest priority) rather than a generic table.
    The final output is a string formatted for Mermaid.js.
    """

    def __init__(
        self, db_fks: List[Dict[str, str]], dbt_models: List[Dict[str, Any]]
    ):
        """
        Initializes the GlobalLineageGenerator.

        Args:
            db_fks: A list of foreign key relationships from the database, as
                    returned by a `BaseConnector`.
            dbt_models: A list of parsed dbt models from `DbtManifestParser`,
                        including their `dependencies`.
        """
        self.db_fks = db_fks
        self.dbt_models = dbt_models
        self.nodes: Dict[str, str] = {}  # Stores nodes and their assigned style
        self.edges: Set[str] = (
            set()
        )  # Stores unique edges to prevent duplicates

    def _get_style_priority(self, style: str) -> int:
        """Assigns a priority to a node style. Higher numbers win."""
        if style == "box":
            return 3  # dbt model (highest priority)
        if style == "source":
            return 2  # dbt source
        if style == "db":
            return 1  # db table (lowest priority)
        return 0

    def _add_node(self, name: str, style: str):
        """
        Adds a node to the graph, applying style based on priority.

        If a node already exists, its style is only updated if the new style
        has a higher priority. This ensures a dbt model is always styled as a
        'model' (`box`), not as a generic 'db' table.
        """
        current_style = self.nodes.get(name)
        current_priority = (
            self._get_style_priority(current_style) if current_style else -1
        )
        new_priority = self._get_style_priority(style)

        if new_priority > current_priority:
            self.nodes[name] = style

    def _add_edge(self, from_node: str, to_node: str, label: str = ""):
        """Adds a unique, formatted edge to the graph's edge set."""
        if label:
            self.edges.add(f'    {from_node} -- "{label}" --> {to_node}')
        else:
            self.edges.add(f"    {from_node} --> {to_node}")

    def generate_graph(self) -> str:
        """
        Generates the complete Mermaid.js graph definition as a string.

        The method follows a clear three-step process:
        1.  **Process Physical Lineage**: Adds nodes and edges from database
            foreign keys, styling them as low-priority 'db' tables.
        2.  **Process Logical Lineage**: Adds nodes and edges from dbt model
            dependencies. The style priority system ensures that any nodes
            that are dbt models or sources are styled correctly, overwriting
            the 'db' style if necessary.
        3.  **Assemble Graph**: Combines the unique, prioritized nodes and
            edges into a single, valid Mermaid.js graph string.

        Returns:
            A string containing the full Mermaid.js graph definition.
        """
        logger.info("Generating global lineage graph...")

        # 1. Process DB Foreign Keys (Physical Lineage)
        for fk in self.db_fks:
            from_table = fk["source_table"]
            to_table = fk["target_table"]
            self._add_node(from_table, "db")
            self._add_node(to_table, "db")
            self._add_edge(from_table, to_table, "FK")

        # 2. Process dbt Model Dependencies (Logical Lineage)
        for model in self.dbt_models:
            model_name = model["name"]
            self._add_node(model_name, "box")  # dbt models are highest priority

            for dep in model.get("dependencies", []):
                # A dependency with a dot is a source (e.g., 'jaffle_shop.customers')
                if "." in dep:
                    self._add_node(dep, "source")
                    self._add_edge(dep, model_name)
                else:  # Otherwise, it's another dbt model (a ref)
                    self._add_node(dep, "box")
                    self._add_edge(dep, model_name)

        # 3. Combine into a Mermaid string
        graph_lines = ["graph TD;"]
        node_definitions = []
        for name, style in self.nodes.items():
            if style == "box":
                node_definitions.append(f'    {name}["{name}"]')  # dbt model
            elif style == "db":
                node_definitions.append(f'    {name}[("{name}")]')  # DB table
            elif style == "source":
                node_definitions.append(f'    {name}(("{name}"))')  # dbt source

        graph_lines.extend(sorted(node_definitions))
        graph_lines.append("")
        graph_lines.extend(sorted(list(self.edges)))

        return "\n".join(graph_lines)


class LineageWorkflow:
    """
    Manages the end-to-end workflow for the `schema-scribe lineage` command.

    This class orchestrates the process of fetching data from both the database
    and dbt, generating the unified lineage graph, and writing it to a file.
    """

    def __init__(
        self,
        config_path: str,
        db_profile: str,
        dbt_project_dir: str,
        output_profile: str,
    ):
        """
        Initializes the LineageWorkflow with parameters from the CLI.

        Args:
            config_path: The path to the configuration file.
            db_profile: The DB profile for scanning physical foreign keys.
            dbt_project_dir: The path to the dbt project for logical lineage.
            output_profile: The output profile (must be 'mermaid' type).
        """
        self.config_path = config_path
        self.db_profile_name = db_profile
        self.dbt_project_dir = dbt_project_dir
        self.output_profile_name = output_profile
        self.config = load_config(config_path)

    def run(self):
        """
        Executes the full lineage generation and writing workflow.

        This method follows four main steps:
        1.  **Get Physical Lineage**: Connects to the database to fetch all
            foreign key relationships.
        2.  **Get Logical Lineage**: Parses the dbt `manifest.json` to get all
            model dependencies (`ref` and `source`).
        3.  **Generate Graph**: Instantiates `GlobalLineageGenerator` with both
            sets of lineage data to produce a unified Mermaid.js graph string.
        4.  **Write to File**: Uses a `MermaidWriter` to save the graph to the
            specified output file.
        """
        # 1. Get Physical Lineage (FKs) from DB
        db_fks = self._fetch_database_foreign_keys()

        # 2. Get Logical Lineage (refs) from dbt
        dbt_models = self._parse_dbt_models()

        # 3. Generate Graph
        generator = GlobalLineageGenerator(db_fks, dbt_models)
        mermaid_graph = generator.generate_graph()
        catalog_data = {"mermaid_graph": mermaid_graph}

        # 4. Write to file
        self._write_output(catalog_data)

    def _fetch_database_foreign_keys(self) -> List[Dict[str, str]]:
        """Connects to the DB and retrieves foreign key relationships."""
        db_connector = None
        try:
            logger.info(
                f"Connecting to DB '{self.db_profile_name}' for FK scan..."
            )
            db_params = self.config["db_connections"][self.db_profile_name]
            db_type = db_params.pop("type")
            db_connector = get_db_connector(db_type, db_params)
            fks = db_connector.get_foreign_keys()
            logger.info(f"Found {len(fks)} foreign key relationships.")
            return fks
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise typer.Exit(code=1)
        finally:
            if db_connector:
                db_connector.close()

    def _parse_dbt_models(self) -> List[Dict[str, Any]]:
        """Parses the dbt manifest to get model information."""
        logger.info(
            f"Parsing dbt project at '{self.dbt_project_dir}' for dependencies..."
        )
        parser = DbtManifestParser(self.dbt_project_dir)
        models = parser.models
        logger.info(f"Parsed {len(models)} dbt models.")
        return models

    def _write_output(self, catalog_data: Dict[str, Any]):
        """Writes the generated graph to a file using a MermaidWriter."""
        try:
            writer_params = self.config["output_profiles"][
                self.output_profile_name
            ]
            writer_type = writer_params.pop("type")
            if writer_type != "mermaid":
                logger.warning(
                    f"Output profile '{self.output_profile_name}' is not type 'mermaid'. "
                    "The lineage workflow requires a 'mermaid' writer. Using it anyway."
                )

            writer = get_writer("mermaid")  # Force MermaidWriter
            writer.write(catalog_data, **writer_params)
            logger.info(
                f"Global lineage graph written successfully using output profile: '{self.output_profile_name}'."
            )
        except Exception as e:
            logger.error(
                f"Failed to write lineage graph using profile '{self.output_profile_name}': {e}"
            )
            raise typer.Exit(code=1)
