import pytest
from unittest.mock import MagicMock, call

from data_scribe.components.writers import (
    PostgresCommentWriter,
)
from data_scribe.core.exceptions import WriterError, ConfigError
from data_scribe.components.db_connectors import PostgresConnector, SQLiteConnector


@pytest.fixture
def mock_db_catalog_data():
    """Provides a mock catalog data structure for standard DB connections."""
    return {
        "tables": [
            {
                "name": "users",
                "columns": [
                    {"name": "id", "type": "INTEGER", "description": "User ID"},
                    {
                        "name": "email",
                        "type": "TEXT",
                        "description": "User email",
                    },
                ],
            }
        ],
        "views": [
            {
                "name": "user_views",
                "ai_summary": "A summary of the view.",
                "definition": "SELECT * FROM users",
            }
        ],
        "foreign_keys": [
            {
                "from_table": "orders",
                "to_table": "users",
                "from_column": "user_id",
                "to_column": "id",
            }
        ],
    }


@pytest.fixture
def mock_postgres_connector() -> MagicMock:
    """
    Creates a mock PostgresConnector object with mock cursor and connection.
    """
    # Use 'spec=PostgresConnector' to ensure the mock behaves like
    # the real connector and fails if unexpected attributes are accessed.
    mock_connector = MagicMock(spec=PostgresConnector)
    mock_connector.cursor = MagicMock()
    mock_connector.connection = MagicMock()
    mock_connector.schema_name = "public"  # Set a schema name
    return mock_connector


def test_postgres_comment_writer_success(
    mock_postgres_connector: MagicMock, mock_db_catalog_data: dict
):
    """
    Tests that PostgresCommentWriter correctly calls execute() with
    COMMENT ON SQL statements for views and columns, and then calls commit().
    """
    writer = PostgresCommentWriter()

    # Call the write method with the mock connector
    writer.write(
        catalog_data=mock_db_catalog_data, db_connector=mock_postgres_connector
    )

    # --- Verify the SQL calls ---
    cursor_calls = mock_postgres_connector.cursor.execute.call_args_list

    # 1. Check the VIEW comment
    # From mock_db_catalog_data: view 'user_views'
    expected_view_sql = 'COMMENT ON VIEW "public"."user_views" IS %s;'
    expected_view_args = ("A summary of the view.",)
    assert call(expected_view_sql, expected_view_args) in cursor_calls

    # 2. Check the COLUMN comments
    # From mock_db_catalog_data: table 'users', cols 'id' and 'email'
    expected_col1_sql = 'COMMENT ON COLUMN "public"."users"."id" IS %s;'
    expected_col1_args = ("User ID",)
    assert call(expected_col1_sql, expected_col1_args) in cursor_calls

    expected_col2_sql = 'COMMENT ON COLUMN "public"."users"."email" IS %s;'
    expected_col2_args = ("User email",)
    assert call(expected_col2_sql, expected_col2_args) in cursor_calls

    # 3. Check total calls
    assert mock_postgres_connector.cursor.execute.call_count == 3

    # --- Verify Transaction ---
    # 4. Check that commit was called
    mock_postgres_connector.connection.commit.assert_called_once()
    # 5. Check that rollback was NOT called
    mock_postgres_connector.connection.rollback.assert_not_called()


def test_postgres_comment_writer_failure_rolls_back(
    mock_postgres_connector: MagicMock, mock_db_catalog_data: dict
):
    """
    Tests that the writer calls rollback() if a SQL execution fails.
    """
    writer = PostgresCommentWriter()

    # Simulate a database error
    mock_postgres_connector.cursor.execute.side_effect = Exception("Test SQL Error")

    # The writer should catch the error and re-raise it as a WriterError
    with pytest.raises(WriterError, match="Test SQL Error"):
        writer.write(
            catalog_data=mock_db_catalog_data, db_connector=mock_postgres_connector
        )

    # --- Verify Transaction ---
    # 1. Check that rollback was called
    mock_postgres_connector.connection.rollback.assert_called_once()
    # 2. Check that commit was NOT called
    mock_postgres_connector.connection.commit.assert_not_called()


def test_postgres_comment_writer_config_errors(mock_db_catalog_data: dict):
    """
    Tests that the writer raises ConfigError if the connector is missing or
    is of the wrong type.
    """
    writer = PostgresCommentWriter()

    # 1. Test missing 'db_connector'
    with pytest.raises(ConfigError, match="requires 'db_connector'"):
        writer.write(catalog_data=mock_db_catalog_data)  # No connector

    # 2. Test wrong connector type (e.g., SQLite)
    mock_sqlite_connector = MagicMock(spec=SQLiteConnector)
    with pytest.raises(ConfigError, match="only compatible with 'postgres'"):
        writer.write(
            catalog_data=mock_db_catalog_data, db_connector=mock_sqlite_connector
        )
