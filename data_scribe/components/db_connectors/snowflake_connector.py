import snowflake.connector
from typing import List, Dict, Any

from data_scribe.core.interfaces import BaseConnector
from data_scribe.utils.logger import get_logger

# Initialize a logger for this module
logger = get_logger(__name__)


class SnowflakeConnector(BaseConnector):
    def __init__(self):
        self.connection = None
        self.cursor = None
        self.dbname = None
        self.schema_name = None

    def connect(self, db_params: Dict[str, Any]):
        try:
            self.dbname = db_params.get("database")
            self.schema_name = db_params.get("schema", "public")

            if not self.dbname:
                raise ValueError("'database' parameter is required.")

            self.connection = snowflake.connector.connect(
                user=db_params.get("user"),
                password=db_params.get("password"),
                account=db_params.get("account"),
                warehouse=db_params.get("warehouse"),
                database=self.dbname,
                schema=self.schema_name,
            )
            self.cursor = self.connection.cursor()
            logger.info(f"Successfully connected to Snowflake DB '{self.dbname}'.")
        except Exception as e:
            logger.error(f"Snowflake connection failed: {e}", exc_info=True)
            raise ConnectionError(f"Snowflake connection failed: {e}")

    def get_tables(self) -> List[str]:
        if not self.cursor:
            raise RuntimeError("Must connect to the DB first.")

        logger.info(f"Fetching tables from schema: {self.schema_name}")

        query = f"""
            SELECT table_name
            FROM "{self.dbname}".information_schema.tables
            WHERE table_schema = %s AND table_type = 'BASE TABLE;        """
        self.cursor.execute(query, (self.schema_name,))

        tables = [table[0] for table in self.cursor.fetchall()]
        logger.info(f"Found {len(tables)} tables.")
        return tables

    def get_columns(self, table_name: str) -> List[Dict[str, str]]:
        if not self.cursor:
            raise RuntimeError("Must connect to the DB first.")

        query = f"""
            SELECT column_name, data_type 
            FROM "{self.dbname}".information_schema.columns 
            WHERE table_schema = %s AND table_name = %s;
        """
        self.cursor.execute(query, (self.schema_name, table_name))

        columns = [{"name": col[0], "type": col[1]} for col in self.cursor.fetchall()]
        logger.info(f"Found {len(columns)} columns in table {table_name}.")
        return columns

    def get_views(self) -> List[Dict[str, str]]:
        if not self.cursor:
            raise RuntimeError("Must connect to the DB first.")

        query = f"""
            SELECT table_name, view_definition 
            FROM "{self.dbname}".information_schema.views 
            WHERE table_schema = %s;
        """
        self.cursor.execute(query, (self.schema_name,))

        views = [
            {"name": view[0], "definition": view[1]} for view in self.cursor.fetchall()
        ]
        logger.info(f"Found {len(views)} views.")
        return views

    def get_foreign_keys(self) -> List[Dict[str, str]]:
        """모든 외래 키 관계를 가져옵니다."""
        if not self.cursor:
            raise RuntimeError("DB에 먼저 연결해야 합니다.")

        logger.info("Fetching foreign key relationships from Snowflake...")
        self.cursor.execute(f'USE SCHEMA "{self.dbname}"."{self.schema_name}"')
        self.cursor.execute("SHOW IMPORTED KEYS;")

        foreign_keys = []
        for fk in self.cursor.fetchall():
            foreign_keys.append(
                {
                    "from_table": fk[6],
                    "from_column": fk[7],
                    "to_table": fk[2],
                    "to_column": fk[3],
                }
            )

        logger.info(f"Found {len(foreign_keys)} foreign key relationships.")
        return foreign_keys

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("Snowflake connection closed.")
