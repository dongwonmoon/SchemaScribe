"""
Integration test for the 'lineage' command workflow.
"""

import pytest
from unittest.mock import patch, MagicMock

# Import the class we are testing
from schema_scribe.workflows.lineage_workflow import LineageWorkflow
from schema_scribe.core.interfaces import BaseConnector, BaseWriter


@patch("schema_scribe.workflows.lineage_workflow.DbtManifestParser")
def test_lineage_workflow_e2e(
    mock_parser_constructor: MagicMock,
    tmp_path: str,
):
    """
    Tests the end-to-end lineage workflow by mocking all external
    dependencies (DB, Parser, Writer).

    Validates that:
    1. The correct components are called.
    2. Physical (FK) and Logical (dbt) dependencies are merged
       into a single Mermaid graph.
    """
    # 1. ARRANGE

    # --- Mock DB Connector (Physical Lineage) ---
    mock_db_connector = MagicMock(spec=BaseConnector)
    mock_db_connector.get_foreign_keys.return_value = [
        {
            "source_table": "stg_orders",  # This is also a dbt model
            "source_column": "order_id",
            "target_table": "raw_orders",  # This is just a DB table
            "target_column": "id",
        }
    ]
    mock_db_connector.db_profile_name = "test_db"  # Add this for logging
    mock_db_connector.close.return_value = None

    # --- Mock dbt Parser (Logical Lineage) ---
    mock_dbt_models = [
        {
            "name": "fct_orders",
            # Depends on two models
            "dependencies": ["stg_orders", "stg_customers"],
        },
        {
            "name": "stg_orders",
            # Depends on a dbt source
            "dependencies": ["jaffle_shop.raw_orders"],
        },
        {
            "name": "stg_customers",
            # Depends on a dbt source
            "dependencies": ["jaffle_shop.raw_customers"],
        },
    ]
    mock_parser_instance = MagicMock()
    mock_parser_instance.models = mock_dbt_models
    mock_parser_constructor.return_value = mock_parser_instance

    # --- Mock Writer ---
    mock_writer = MagicMock(spec=BaseWriter)
    mock_writer.write.return_value = None

    dummy_dbt_dir = str(tmp_path / "dbt_project")
    output_filename = str(tmp_path / "lineage.md")
    writer_params = {"output_filename": output_filename}

    # 2. ACT
    workflow = LineageWorkflow(
        db_connector=mock_db_connector,
        writer=mock_writer,
        dbt_project_dir=dummy_dbt_dir,
        db_profile_name="test_db",
        output_profile_name="test_output",
        writer_params=writer_params,
    )
    workflow.run()

    # 3. ASSERT

    # Assert components were called correctly
    mock_db_connector.get_foreign_keys.assert_called_once()
    mock_parser_constructor.assert_called_once_with(dummy_dbt_dir)
    mock_writer.write.assert_called_once()
    mock_db_connector.close.assert_called_once()

    # Assert the generated graph is correct
    # Get the data passed to the writer's 'write' method
    captured_catalog_data = mock_writer.write.call_args[0][0]
    captured_writer_params = mock_writer.write.call_args[1]

    assert "mermaid_graph" in captured_catalog_data
    assert captured_writer_params == writer_params

    mermaid_graph = captured_catalog_data["mermaid_graph"]

    # Check for the graph type
    assert "graph TD;" in mermaid_graph

    # Check for all nodes with correct styling
    # DB Table (rounded box)
    assert '    raw_orders[("raw_orders")]' in mermaid_graph
    # dbt Sources (stadium shape)
    assert (
        '    jaffle_shop.raw_orders(("jaffle_shop.raw_orders"))'
        in mermaid_graph
    )
    assert (
        '    jaffle_shop.raw_customers(("jaffle_shop.raw_customers"))'
        in mermaid_graph
    )
    # dbt Models (rectangular box)
    assert '    stg_orders["stg_orders"]' in mermaid_graph
    assert '    stg_customers["stg_customers"]' in mermaid_graph
    assert '    fct_orders["fct_orders"]' in mermaid_graph

    # Check for all edges (Physical + Logical)
    # Physical FK
    assert '    stg_orders -- "FK" --> raw_orders' in mermaid_graph
    # Logical (dbt refs and sources)
    assert "    jaffle_shop.raw_orders --> stg_orders" in mermaid_graph
    assert "    jaffle_shop.raw_customers --> stg_customers" in mermaid_graph
    assert "    stg_orders --> fct_orders" in mermaid_graph
    assert "    stg_customers --> fct_orders" in mermaid_graph
