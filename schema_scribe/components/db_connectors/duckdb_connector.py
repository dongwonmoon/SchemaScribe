"""
This module provides a `BaseConnector` implementation for DuckDB.

Its key feature is its versatility, allowing it to read data from persistent
database files, in-memory databases, local file systems (including globs),
and even S3, presenting them all through a unified table/column interface.
"""

import duckdb
from typing import List, Dict, Any, Optional

from schema_scribe.core.interfaces import BaseConnector
from schema_scribe.core.exceptions import ConnectorError
from schema_scribe.utils.logger import get_logger

logger = get_logger(__name__)


class DuckDBConnector(BaseConnector):
    """
    A versatile connector for reading data using DuckDB.

    This connector implements the `BaseConnector` interface and can operate in
    one of three modes based on the `path` parameter provided to `connect`:
    1.  **Persistent DB Mode**: If `path` ends in `.db` or `.duckdb`, it connects
        to a standard DuckDB database file and can introspect tables, views,
        and foreign keys.
    2.  **File/Directory Scan Mode**: If `path` is a file pattern (e.g., `*.csv`),
        a local directory (`./data/`), or an S3 directory (`s3://...`), it uses
        an in-memory DuckDB instance. In this mode, files are treated as tables.
    3.  **Single File Mode**: If `path` is a single file, it is treated as a
        single table.

    It intelligently routes its methods (`get_tables`, `get_columns`) based on
    the connection type to provide a unified interface for these different sources.
    """

    def __init__(self):
        """Initializes the DuckDBConnector and its state attributes."""
        self.connection: Optional[duckdb.DuckDBPyConnection] = None
        self.cursor: Optional[duckdb.DuckDBCursor] = None
        self.base_path: str = ""
        self.is_directory_scan: bool = False
        self.is_s3: bool = False

    def connect(self, db_params: Dict[str, Any]):
        """
        Initializes a DuckDB connection based on the provided path.

        The connection type is determined by the `path` parameter:
        - If it ends in `.db` or `.duckdb`, it connects to a persistent file.
        - Otherwise, it uses an in-memory database to query a file, pattern, or directory.
        - If the path starts with `s3://`, it automatically installs and loads the `httpfs` extension.

        Args:
            db_params: A dictionary containing the 'path' to the database file
                       or file/directory pattern to be read.

        Raises:
            ValueError: If the 'path' parameter is missing.
            ConnectorError: If the connection fails for any reason.
        """
        path = db_params.get("path")
        if not path:
            raise ValueError("Missing 'path' parameter for DuckDBConnector.")

        self.base_path = path
        db_file = ":memory:"
        read_only = False

        try:
            if path.endswith((".db", ".duckdb")):
                db_file = path
                read_only = True
                logger.info(f"Connecting to persistent DuckDB file: '{path}'")
            else:
                logger.info(
                    f"Connecting to in-memory DuckDB for path: '{path}'"
                )
                if path.endswith("/") or "*" in path:
                    self.is_directory_scan = True

            self.connection = duckdb.connect(
                database=db_file, read_only=read_only
            )
            self.cursor = self.connection.cursor()

            if path.startswith("s3://"):
                self.is_s3 = True
                self.cursor.execute("INSTALL httpfs; LOAD httpfs;")
                logger.info("Installed and loaded httpfs for S3 access.")

            logger.info("Successfully connected to DuckDB.")
        except Exception as e:
            logger.error(f"Failed to connect to DuckDB: {e}", exc_info=True)
            raise ConnectorError(f"Failed to connect to DuckDB: {e}") from e

    def _get_full_path(self, table_name: str) -> str:
        """
        Constructs the full, queryable path for a given file name ("table").

        In a directory scan, it joins the base directory path with the file name.
        Otherwise, it returns the original base path, which is already the full path.
        """
        if self.is_directory_scan:
            return f"{self.base_path.rstrip('/')}/{table_name}"
        return self.base_path

    def get_tables(self) -> List[str]:
        """
        Returns a list of "tables" based on the connection type.

        - **Persistent DB Mode**: Returns actual tables and views from the DB.
        - **Directory Scan Mode**: Returns file names within the target directory.
        - **Single File Mode**: Returns the file path itself as a single "table".

        Returns:
            A list of strings, where each is a table, view, or file name.
        """
        if not self.cursor:
            raise ConnectorError("Not connected to a DuckDB database.")

        if self.base_path.endswith((".db", ".duckdb")):
            logger.info(
                f"Fetching tables and views from DB: '{self.base_path}'"
            )
            self.cursor.execute("SHOW ALL TABLES;")
            return [row[0] for row in self.cursor.fetchall()]

        if self.is_directory_scan:
            glob_func = "s3_glob" if self.is_s3 else "glob"
            glob_path = self.base_path
            if not glob_path.endswith(("*", "*/")):
                glob_path = f"{glob_path.rstrip('/')}/*.*"

            query = (
                f"SELECT basename(file_name) FROM {glob_func}('{glob_path}')"
            )
            logger.info(f"Globbing for files using query: {query}")
            try:
                self.cursor.execute(query)
                return [row[0] for row in self.cursor.fetchall()]
            except Exception as e:
                raise ConnectorError(
                    f"Failed to list files at '{self.base_path}': {e}"
                )

        return [self.base_path]

    def get_columns(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Describes the columns of a table, view, or file-based dataset.

        This implementation fetches column name, data type, nullability, and
        primary key status to conform to the `BaseConnector` interface.

        Args:
            table_name: The name of the table, view, or file to describe.

        Returns:
            A list of dictionaries, each representing a column.
        """
        if not self.cursor:
            raise ConnectorError("Not connected to a DuckDB database.")

        try:
            if self.base_path.endswith((".db", ".duckdb")):
                query = f'DESCRIBE "{table_name}";'
            else:
                full_path = self._get_full_path(table_name)
                logger.info(f"Fetching columns for file: '{full_path}'")
                query = f"DESCRIBE SELECT * FROM read_auto('{full_path}', SAMPLE_SIZE=50000);"

            self.cursor.execute(query)
            # Row format: (column_name, column_type, null, key, default, extra)
            columns = [
                {
                    "name": row[0],
                    "type": row[1],
                    "description": "",
                    "is_nullable": row[2] == "YES",
                    "is_pk": row[3] == "PRI",
                }
                for row in self.cursor.fetchall()
            ]
            logger.info(f"Fetched {len(columns)} columns for: '{table_name}'")
            return columns
        except Exception as e:
            raise ConnectorError(
                f"Failed to fetch columns for '{table_name}': {e}"
            ) from e

    def get_column_profile(
        self, table_name: str, column_name: str
    ) -> Dict[str, Any]:
        """
        Generates profile stats for a column in a DuckDB table or file.

        This method dynamically builds a query to calculate statistics, using
        `read_auto` for file-based sources.

        Args:
            table_name: The name of the table, view, or file.
            column_name: The name of the column to profile.

        Returns:
            A dictionary of statistics.
        """
        if not self.cursor:
            raise ConnectorError("Not connected to a DuckDB database.")

        source_query = ""
        if self.base_path.endswith((".db", ".duckdb")):
            source_query = f'"{table_name}"'
        else:
            full_path = self._get_full_path(table_name)
            source_query = f"(SELECT * FROM read_auto('{full_path}'))"

        query = f"""
        SELECT
            COUNT(*) AS total_count,
            SUM(CASE WHEN "{column_name}" IS NULL THEN 1 ELSE 0 END) AS null_count,
            COUNT(DISTINCT "{column_name}") AS distinct_count
        FROM {source_query} t
        """
        try:
            self.cursor.execute(query)
            row = self.cursor.fetchone()
            if not row:
                raise ConnectorError(
                    "Column profiling query returned no results."
                )

            total_count, null_count, distinct_count = row
            total_count = total_count or 0
            null_count = null_count or 0
            distinct_count = distinct_count or 0

            if total_count == 0:
                return {
                    "null_ratio": 0.0,
                    "distinct_count": 0,
                    "is_unique": True,
                }

            null_ratio = null_count / total_count
            is_unique = (distinct_count == total_count) and (null_count == 0)

            return {
                "null_ratio": round(null_ratio, 2),
                "distinct_count": distinct_count,
                "is_unique": is_unique,
            }
        except Exception as e:
            logger.warning(
                f"Could not profile column '{table_name}.{column_name}': {e}"
            )
            return {
                "null_ratio": "N/A",
                "distinct_count": "N/A",
                "is_unique": "N/A",
            }

    def get_views(self) -> List[Dict[str, str]]:
        """
        Retrieves a list of all views and their SQL definitions.
        This is only supported for persistent `.db` file connections.
        """
        if not self.cursor:
            raise ConnectorError("Not connected to a DuckDB database.")

        if not self.base_path.endswith((".db", ".duckdb")):
            logger.info("Views are not supported for file/directory scans.")
            return []

        logger.info("Fetching views from the database.")
        self.cursor.execute("SELECT view_name, sql FROM duckdb_views();")
        return [
            {"name": row[0], "definition": row[1]}
            for row in self.cursor.fetchall()
        ]

    def get_foreign_keys(self) -> List[Dict[str, str]]:
        """
        Retrieves all foreign key relationships.
        This is only supported for persistent `.db` file connections.
        """
        if not self.cursor:
            raise ConnectorError("Not connected to a DuckDB database.")

        if not self.base_path.endswith((".db", ".duckdb")):
            logger.info(
                "Foreign keys are not supported for file/directory scans."
            )
            return []

        logger.info("Fetching foreign key relationships...")
        try:
            # DuckDB >= 0.9.0
            self.cursor.execute(
                """
                SELECT
                    fk.table_name AS source_table,
                    fk.column_names[1] AS source_column,
                    pk.table_name AS target_table,
                    pk.column_names[1] AS target_column
                FROM duckdb_constraints() fk
                JOIN duckdb_constraints() pk ON fk.primary_key_index = pk.constraint_index
                WHERE fk.constraint_type = 'FOREIGN KEY'
            """
            )
            return [
                {
                    "source_table": row[0],
                    "source_column": row[1],
                    "target_table": row[2],
                    "target_column": row[3],
                }
                for row in self.cursor.fetchall()
            ]
        except duckdb.CatalogException:
            # Fallback for older DuckDB versions
            logger.warning(
                "Using legacy foreign key query for older DuckDB version."
            )
            self.cursor.execute("SELECT * FROM duckdb_foreign_keys();")
            return [
                {
                    "source_table": row[0],
                    "source_column": row[1],
                    "target_table": row[2],
                    "target_column": row[3],
                }
                for row in self.cursor.fetchall()
            ]

    def close(self):
        """Closes the database connection if it is open."""
        if self.connection:
            logger.info("Closing DuckDB database connection.")
            self.connection.close()
            self.connection = None
            self.cursor = None
            logger.info("DuckDB database connection closed.")
