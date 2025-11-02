import sqlite3
from typing import List, Dict, Any

from data_scribe.core.interfaces import BaseConnector
from data_scribe.utils.logger import get_logger

# Initialize logger
logger = get_logger(__name__)


class SQLiteConnector(BaseConnector):
    """Connector for SQLite databases.

    This class implements the BaseConnector interface to provide
    connectivity and schema extraction for SQLite databases.
    """

    def __init__(self):
        """Initializes the SQLiteConnector."""
        self.connection: sqlite3.Connection | None = None
        self.cursor: sqlite3.Cursor | None = None

    def connect(self, db_params: Dict[str, Any]):
        """Connects to the SQLite database.

        Args:
            db_params: A dictionary containing the database path.
                       Example: {"db_path": "my_database.db"}

        Raises:
            ValueError: If the 'db_path' parameter is missing.
            ConnectionError: If the connection to the database fails.
        """
        db_path = db_params.get("path")
        if not db_path:
            logger.error("Missing 'path' parameter for SQLiteConnector.")
            raise ValueError("Missing 'path' parameter for SQLiteConnector.")

        try:
            logger.info(f"Connecting to SQLite database at: {db_path}")
            self.connection = sqlite3.connect(db_path)
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
        """Retrieves a list of all table names in the database."""
        if not self.cursor:
            raise RuntimeError(
                "Database connection not established. Call connect() first."
            )

        logger.info("Fetching table names from the database.")
        self.cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table';"
        )
        tables = [table[0] for table in self.cursor.fetchall()]
        logger.info(f"Found {len(tables)} tables.")
        return tables

    def get_columns(self, table_name: str) -> List[Dict[str, str]]:
        """Retrieves column information (name and type) for a given table."""
        if not self.cursor:
            raise RuntimeError(
                "Database connection not established. Call connect() first."
            )

        logger.info(f"Fetching columns for table: {table_name}")
        self.cursor.execute(f"PRAGMA table_info('{table_name}');")
        # PRAGMA table_info returns: (cid, name, type, notnull, dflt_value, pk)
        columns = [
            {"name": col[1], "type": col[2]} for col in self.cursor.fetchall()
        ]
        logger.info(f"Found {len(columns)} columns in table {table_name}.")
        return columns

    def close(self):
        """Closes the database connection."""
        if self.connection:
            logger.info("Closing SQLite database connection.")
            self.connection.close()
            self.connection = None
            self.cursor = None
            logger.info("SQLite database connection closed.")
