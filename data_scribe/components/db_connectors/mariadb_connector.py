import mysql.connector
from typing import List, Dict, Any

from data_scribe.core.interfaces import BaseConnector
from data_scribe.utils.logger import get_logger

logger = get_logger(__name__)


class MariaDBConnector(BaseConnector):
    """Connector for MariaDB/MySQL databases."""

    def __init__(self):
        self.connection = None
        self.cursor = None
        self.dbname = None  # Stores dbname for schema queries

    def connect(self, db_params: Dict[str, Any]):
        """Connects to the MariaDB/MySQL database."""
        try:
            self.dbname = db_params.get("dbname")
            if not self.dbname:
                raise ValueError(
                    "'dbname' (database name) parameter is required."
                )

            self.connection = mysql.connector.connect(
                host=db_params.get("host", "localhost"),
                port=db_params.get("port", 3306),
                user=db_params.get("user"),
                password=db_params.get("password"),
                database=self.dbname,  # Use 'database' key
            )
            self.cursor = self.connection.cursor()
            logger.info(
                f"Successfully connected to MariaDB/MySQL DB '{self.dbname}'."
            )
        except mysql.connector.Error as e:
            logger.error(f"MariaDB/MySQL connection failed: {e}", exc_info=True)
            raise ConnectionError(
                f"MariaDB/MySQL connection failed: {e}"
            ) from e

    def get_tables(self) -> List[str]:
        if not self.cursor or not self.dbname:
            raise RuntimeError("Must connect to the DB first.")

        # Query tables using information_schema
        self.cursor.execute(
            """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = %s;
        """,
            (self.dbname,),
        )

        tables = [table[0] for table in self.cursor.fetchall()]
        logger.info(f"Found {len(tables)} tables in '{self.dbname}' schema.")
        return tables

    def get_columns(self, table_name: str) -> List[Dict[str, str]]:
        """Retrieves column information (name, type) for a specific table."""
        if not self.cursor or not self.dbname:
            raise RuntimeError("Must connect to the DB first.")

        self.cursor.execute(
            """
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = %s AND table_name = %s;
        """,
            (self.dbname, table_name),
        )

        columns = [
            {"name": col[0], "type": col[1]} for col in self.cursor.fetchall()
        ]
        logger.info(f"Found {len(columns)} columns in table {table_name}.")
        return columns

    def close(self):
        """Closes the database connection."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("MariaDB/MySQL connection closed.")
