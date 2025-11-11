"""
This module defines the core business logic for generating a unified global
lineage graph by combining information from both dbt projects
and database foreign keys (FKs).

Design Rationale:
Data lineage is often fragmented. This module consolidates physical (DB)
and logical (dbt) lineage into a single, coherent view, available as
both a Mermaid string (for static files) and a JSON object (for interactive UIs).
"""

from typing import List, Dict, Any
from schema_scribe.utils.logger import get_logger

logger = get_logger(__name__)


class GlobalLineageGenerator:
    """
    Merges physical (FK) and logical (dbt) lineage to create a unified
    graph structure, accessible as a Mermaid string or a JSON object.
    """

    def __init__(
        self, db_fks: List[Dict[str, str]], dbt_models: List[Dict[str, Any]]
    ):
        """
        Initializes the GlobalLineageGenerator.

        Args:
            db_fks: A list of FK relationships from a BaseConnector.
            dbt_models: A list of parsed dbt models from DbtManifestParser.
        """
        self.db_fks = db_fks
        self.dbt_models = dbt_models

        # Internal storage for processed nodes and edges
        # { 'node_id': { 'id': ..., 'label': ..., 'style': ... } }
        self.nodes: Dict[str, Dict[str, Any]] = {}
        # [ { 'id': ..., 'source': ..., 'target': ..., 'label': ... } ]
        self.edges: List[Dict[str, Any]] = []
        self._processed = False  # Flag to ensure processing runs only once

    def _get_style_priority(self, style: str) -> int:
        """Assigns priority to node styles. Higher numbers win."""
        if style == "box":
            return 3  # dbt model (highest priority)
        if style == "source":
            return 2  # dbt source
        if style == "db":
            return 1  # db table (lowest priority)
        return 0

    def _add_node(self, name: str, style: str):
        """
        Adds a node or updates its style based on priority.
        Ensures a dbt model style ('box') always overrides a
        generic DB table style ('db').
        """
        current_style_data = self.nodes.get(name)
        current_priority = (
            self._get_style_priority(current_style_data["style"])
            if current_style_data
            else -1
        )
        new_priority = self._get_style_priority(style)

        if new_priority > current_priority:
            self.nodes[name] = {"id": name, "label": name, "style": style}

    def _add_edge(self, from_node: str, to_node: str, label: str = ""):
        """Adds a unique edge dictionary to the graph."""
        edge_id = f"{from_node}-{to_node}"
        if label:
            edge_id = f"{edge_id}-{label}"

        new_edge = {
            "id": edge_id,
            "source": from_node,
            "target": to_node,
            "label": label,
        }
        # Add edge if not already present
        if new_edge not in self.edges:
            self.edges.append(new_edge)

    def _process_lineage(self):
        """
        Internal method to process all lineage data.
        Runs only once.
        """
        if self._processed:
            return

        logger.info("Processing physical and logical lineage...")

        # 1. Process Physical Lineage (DB Foreign Keys)
        for fk in self.db_fks:
            from_table = fk["source_table"]
            to_table = fk["target_table"]
            self._add_node(from_table, "db")
            self._add_node(to_table, "db")
            self._add_edge(from_table, to_table, "FK")

        # 2. Process Logical Lineage (dbt Model Dependencies)
        for model in self.dbt_models:
            model_name = model["name"]
            self._add_node(model_name, "box")  # dbt models are highest priority

            for dep in model.get("dependencies", []):
                if "." in dep:
                    self._add_node(dep, "source")
                    self._add_edge(dep, model_name)
                else:
                    self._add_node(dep, "box")
                    self._add_edge(dep, model_name)

        self._processed = True

    def generate_mermaid_string(self) -> str:
        """
        Generates the complete Mermaid.js graph definition as a string.
        (Maintains compatibility with MermaidWriter)
        """
        self._process_lineage()  # Ensure data is processed
        logger.info("Generating Mermaid string output...")

        graph_lines = ["graph TD;"]
        node_definitions = []
        # Sort nodes for consistent output
        for name, data in sorted(self.nodes.items()):
            style = data["style"]
            if style == "box":
                node_definitions.append(f'    {name}["{name}"]')
            elif style == "db":
                node_definitions.append(f'    {name}[("{name}")]')
            elif style == "source":
                node_definitions.append(f'    {name}(("{name}"))')

        graph_lines.extend(node_definitions)
        graph_lines.append("")

        # Sort edges for consistent output
        for edge in sorted(self.edges, key=lambda x: x["id"]):
            if edge["label"]:
                graph_lines.append(
                    f'    {edge["source"]} -- "{edge["label"]}" --> {edge["target"]}'
                )
            else:
                graph_lines.append(f'    {edge["source"]} --> {edge["target"]}')

        return "\n".join(graph_lines)

    def generate_graph_json(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Generates a structured JSON object for UI libraries like react-flow.
        (New feature for UI consumption)
        """
        self._process_lineage()  # Ensure data is processed
        logger.info("Generating JSON graph output for UI...")

        # Format nodes for react-flow
        formatted_nodes = []
        for node in self.nodes.values():
            formatted_nodes.append(
                {
                    "id": node["id"],
                    "data": {"label": node["label"]},
                    "type": node[
                        "style"
                    ],  # UI can use this for custom rendering
                    # 'position' will be handled by the UI layout engine
                }
            )

        # Edges are already in a compatible format
        return {"nodes": formatted_nodes, "edges": self.edges}
