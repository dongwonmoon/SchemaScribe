"""
This module defines the workflow for the 'lineage' command,
which combines physical DB lineage (FKs) with logical
dbt lineage (refs/sources).
"""
import typer
from typing import List, Dict, Any, Set

from data_scribe.core.factory import get_db_connector, get_writer
from data_scribe.core.dbt_parser import DbtManifestParser
from data_scribe.core.workflow_helpers import load_config
from data_scribe.utils.logger import get_logger

logger = get_logger(__name__)

class GlobalLineageGenerator:
    """
    Merges DB foreign keys and dbt dependencies into a single
    Mermaid graph string.
    """
    def __init__(self, db_fks: List[Dict[str, str]], dbt_models: List[Dict[str, Any]]):
        self.db_fks = db_fks
        self.dbt_models = dbt_models
        self.nodes: Set[str] = set()
        self.edges: List[str] = []

    def _add_node(self, name: str, style: str = "box"):
        """Adds a node to the graph if it doesn't exist."""
        if name not in self.nodes:
            if style == "box":
                self.nodes.add(f'    {name}["{name}"]') # dbt model
            elif style == "db":
                self.nodes.add(f'    {name}[("{name}")]') # DB table
            elif style == "source":
                self.nodes.add(f'    {name}(("{name}"))') # dbt source
            self.nodes.add(name)

    def generate_graph(self) -> str:
        """Generates the full Mermaid graph string."""
        logger.info("Generating global lineage graph...")
        
        # 1. Process DB Foreign Keys (Physical Lineage)
        for fk in self.db_fks:
            from_table = fk["from_table"]
            to_table = fk["to_table"]
            
            # Style DB tables
            self._add_node(from_table, "db")
            self._add_node(to_table, "db")
            
            self.edges.append(f'    {from_table} -- FK --> {to_table}')

        # 2. Process dbt Model Dependencies (Logical Lineage)
        for model in self.dbt_models:
            model_name = model["name"]
            self._add_node(model_name, "box") # Style dbt models
            
            for dep in model.get("dependencies", []):
                if "." in dep: # This is a source (e.g., 'jaffle_shop.customers')
                    self._add_node(dep, "source")
                    self.edges.append(f'    {dep} --> {model_name}')
                else: # This is another dbt model (a ref)
                    self._add_node(dep, "box")
                    self.edges.append(f'    {dep} --> {model_name}')

        # 3. Combine into a Mermaid string
        graph_lines = ["graph TD;"]
        graph_lines.extend(sorted(list(self.nodes))) # Add all unique node definitions
        graph_lines.append("") # Spacer
        graph_lines.extend(sorted(list(self.edges))) # Add all unique edges
        
        return "\n".join(graph_lines)


class LineageWorkflow:
    """
    Manages the workflow for the 'lineage' command.
    """
    def __init__(
        self,
        config_path: str,
        db_profile: str,
        dbt_project_dir: str,
        output_profile: str,
    ):
        self.config_path = config_path
        self.db_profile_name = db_profile
        self.dbt_project_dir = dbt_project_dir
        self.output_profile_name = output_profile
        self.config = load_config(config_path)

    def run(self):
        """Executes the lineage generation workflow."""
        
        # 1. Get Physical Lineage (FKs) from DB
        db_connector = None
        db_fks = []
        try:
            logger.info(f"Connecting to DB '{self.db_profile_name}' for FK scan...")
            db_params = self.config["db_connections"][self.db_profile_name]
            db_type = db_params.pop("type")
            db_connector = get_db_connector(db_type, db_params)
            db_fks = db_connector.get_foreign_keys()
            logger.info(f"Found {len(db_fks)} foreign key relationships.")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise typer.Exit(code=1)
        finally:
            if db_connector:
                db_connector.close()
                
        # 2. Get Logical Lineage (refs) from dbt
        logger.info(f"Parsing dbt project at '{self.dbt_project_dir}' for dependencies...")
        parser = DbtManifestParser(self.dbt_project_dir)
        dbt_models = parser.models # This now contains 'dependencies'
        logger.info(f"Parsed {len(dbt_models)} dbt models.")
        
        # 3. Generate Graph
        generator = GlobalLineageGenerator(db_fks, dbt_models)
        mermaid_graph = generator.generate_graph()
        
        catalog_data = {"mermaid_graph": mermaid_graph}
        
        # 4. Write to file
        try:
            writer_params = self.config["output_profiles"][self.output_profile_name]
            writer_type = writer_params.pop("type")
            if writer_type != "mermaid":
                 logger.warning(f"Output profile '{self.output_profile_name}' is not type 'mermaid'. Using MermaidWriter anyway.")
            
            writer = get_writer("mermaid") # Force MermaidWriter
            
            writer.write(catalog_data, **writer_params)
            logger.info(
                f"Global lineage graph written successfully using output profile: '{self.output_profile_name}'."
            )
        except Exception as e:
            logger.error(
                f"Failed to write lineage graph using profile '{self.output_profile_name}': {e}"
            )
            raise typer.Exit(code=1)