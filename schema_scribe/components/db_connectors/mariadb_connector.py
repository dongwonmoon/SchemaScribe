"""
This module provides a concrete implementation of the `SqlBaseConnector` for
MariaDB and MySQL databases.

It uses the `mysql-connector-python` library to handle the connection and
relies on the parent class for all `information_schema`-based metadata
extraction.
"""

import mysql.connector
from typing import Dict, Any

from .sql_base_connector import SqlBaseConnector
from schema_scribe.core.exceptions import ConnectorError
from schema_scribe.utils.logger import get_logger

logger = get_logger(__name__)


class MariaDBConnector(SqlBaseConnector):
    """
    A concrete connector for MariaDB and MySQL databases.

    This class extends `SqlBaseConnector` and implements the `connect` method
    using the `mysql-connector-python` library.

    A key detail for this connector is that for MySQL/MariaDB, the 'schema' is
    synonymous with the 'database'. Therefore, the `dbname` parameter is used
    to set the `schema_name` required by the base class.
    """

    def __init__(self):
        """Initializes the MariaDBConnector by calling the parent constructor."""
        super().__init__()

    def connect(self, db_params: Dict[str, Any]):
        """
        Connects to a MariaDB/MySQL database using the provided parameters.

        This method fulfills the contract required by `SqlBaseConnector` by
        setting the `self.connection`, `self.cursor`, `self.dbname`, and
        `self.schema_name` attributes upon a successful connection.

        Args:
            db_params: A dictionary of connection parameters. Expected keys
                       include `user`, `password`, `dbname`, and optionally:
                       - `host` (defaults to 'localhost')
                       - `port` (defaults to 3306)

        Raises:
            ValueError: If the 'dbname' parameter is missing.
            ConnectorError: If the database connection fails.
        """
        logger.info("Connecting to MariaDB/MySQL database...")
        try:
            # For MySQL/MariaDB, the schema is the database itself.
            self.dbname = db_params.get("dbname")
            self.schema_name = self.dbname
            if not self.dbname:
                raise ValueError(
                    "'dbname' (database name) parameter is required for MariaDB/MySQL."
                )

            self.connection = mysql.connector.connect(
                host=db_params.get("host", "localhost"),
                port=db_params.get("port", 3306),
                user=db_params.get("user"),
                password=db_params.get("password"),
                database=self.dbname,
            )
            self.cursor = self.connection.cursor()
            logger.info(
                f"Successfully connected to MariaDB/MySQL DB '{self.dbname}'."
            )
        except mysql.connector.Error as e:
            logger.error(f"MariaDB/MySQL connection failed: {e}", exc_info=True)
            raise ConnectorError(f"MariaDB/MySQL connection failed: {e}") from e
