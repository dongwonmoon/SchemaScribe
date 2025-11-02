import json
import os
from typing import List, Dict, Any
from data_scribe.utils.logger import get_logger

logger = get_logger(__name__)


class DbtManifestParser:
    """
    Parse the dbt 'manifest.json' file to extract the model and column information needed for documentation.
    """

    def __init__(self, dbt_project_dir: str):
        self.manifest_path = os.path.join(
            dbt_project_dir, "target", "manifest.json"
        )
        self.manifest_data = self._load_manifest()

    def _load_manifest(self) -> Dict[str, Any]:
        """
        Load 'manifast.json'
        """
        logger.info(f"Loading manifest from: {self.manifest_path}")
        try:
            with open(self.manifest_path, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            logger.error(f"Manifest file not found at: {self.manifest_path}")
            raise

    def parse_models(self) -> List[Dict[str, Any]]:
        """
        Parses all 'model' nodes in the manifest and extracts key information (name, SQL, columns).

        Returns:
            A list of model dictionaries.
            [
                {
                    "name": "model_name",
                    "raw_sql": "SELECT ...",
                    "columns": [
                        {"name": "col1", "type": "text"},
                        {"name": "col2", "type": "int"}
                    ]
                },
                ...
            ]
        """
        models = []
        nodes = self.manifest_data.get("nodes", {})
        logger.info(f"Parsing {len(nodes)} nodes from manifest...")

        for node in nodes.values():
            # Filter only 'models' that are generated as actual tables/views
            if (
                node["resource_type"] == "model"
                and node["config"]["materialized"] != "ephemeral"
            ):
                model_name = node["name"]
                raw_code = node.get("raw_code", "-- SQL code not available --")

                column_nodes = node.get("columns", {})

                parsed_columns = []
                for col_name, col_info in column_nodes.items():
                    parsed_columns.append(
                        {
                            "name": col_name,
                            "type": col_info.get("data_type", "N/A"),
                        }
                    )

                models.append(
                    {
                        "name": model_name,
                        "raw_sql": raw_code,
                        "columns": parsed_columns,
                    }
                )

        logger.info(f"Found and parsed {len(models)} models.")
        return models
