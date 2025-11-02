"""
This module provides a concrete implementation of the BaseConnector for SQLite databases.

It handles the connection to a SQLite database file, extraction of table and column metadata,
and closing the connection.
"""

import sqlite3
from typing import List, Dict, Any

from data_scribe.core.interfaces import BaseConnector
from data_scribe.utils.logger import get_logger

# Initialize a logger for this module
logger = get_logger(__name__)


class SQLiteConnector(BaseConnector):
    """Connector for SQLite databases.

    This class implements the BaseConnector interface to provide
    connectivity and schema extraction for SQLite databases.
    """

    def __init__(self):
        """Initializes the SQLiteConnector, setting connection and cursor to None."""
        self.connection: sqlite3.Connection | None = None
        self.cursor: sqlite3.Cursor | None = None

    def connect(self, db_params: Dict[str, Any]):
        """Connects to the SQLite database using the provided file path.

        Args:
            db_params: A dictionary containing the database path.
                       Example: {"path": "my_database.db"}

        Raises:
            ValueError: If the 'path' parameter is missing from db_params.
            ConnectionError: If the connection to the database fails.
        """
        db_path = db_params.get("path")
        if not db_path:
            logger.error("Missing 'path' parameter for SQLiteConnector.")
            raise ValueError("Missing 'path' parameter for SQLiteConnector.")

        try:
            logger.info(f"Connecting to SQLite database at: {db_path}")
            # Establish the database connection
            self.connection = sqlite3.connect(db_path)
            # Create a cursor for executing queries
            self.cursor = self.connection.cursor()
            logger.info("Successfully connected to SQLite database.")
        except sqlite3.Error as e:
            logger.error(
                f"Failed to connect to SQLite database: {e}", exc_info=True
            )
            raise ConnectionError(
                f"Failed to connect to SQLite database: {e}"
            ) from e

    def get_tables(self) -> List[str]:
        """Retrieves a list of all table names in the connected database.

        Returns:
            A list of strings, where each string is a table name.

        Raises:
            RuntimeError: If the database connection has not been established.
        """
        if not self.cursor:
            raise RuntimeError(
                "Database connection not established. Call connect() first."
            )

        logger.info("Fetching table names from the database.")
        # Query the sqlite_master table to get the names of all tables
        self.cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table';"
        )
        # Extract the table names from the query result
        tables = [table[0] for table in self.cursor.fetchall()]
        logger.info(f"Found {len(tables)} tables.")
        return tables

    def get_columns(self, table_name: str) -> List[Dict[str, str]]:
        """Retrieves column information (name and type) for a given table.

        Args:
            table_name: The name of the table to inspect.

        Returns:
            A list of dictionaries, where each dictionary represents a column
            and contains its name and data type.

        Raises:
            RuntimeError: If the database connection has not been established.
        """
        if not self.cursor:
            raise RuntimeError(
                "Database connection not established. Call connect() first."
            )

        logger.info(f"Fetching columns for table: {table_name}")
        # Use the PRAGMA table_info command to get column metadata
        self.cursor.execute(f"PRAGMA table_info('{table_name}');")
        # The result of PRAGMA table_info is a tuple: (cid, name, type, notnull, dflt_value, pk)
        # We extract just the name (index 1) and type (index 2).
        columns = [
            {"name": col[1], "type": col[2]} for col in self.cursor.fetchall()
        ]
        logger.info(f"Found {len(columns)} columns in table {table_name}.")
        return columns

    def close(self):
        """Closes the database connection if it is open."""
        if self.connection:
            logger.info("Closing SQLite database connection.")
            self.connection.close()
            # Reset connection and cursor attributes to None
            self.connection = None
            self.cursor = None
            logger.info("SQLite database connection closed.")
