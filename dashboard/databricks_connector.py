"""
Databricks Connector
====================
Connect to Databricks using OAuth (service principal) to extract data from
Databricks tables.  Supports both:
  - databricks-sql-connector  (SQL queries → pandas DataFrames)
  - databricks-sdk            (workspace / Unity Catalog APIs)

Configuration is loaded from a .env file (or environment variables).

Usage
-----
    from databricks_connector import DatabricksConnector

    connector = DatabricksConnector()          # reads .env automatically
    df = connector.query("SELECT * FROM my_catalog.my_schema.my_table LIMIT 100")
    print(df.head())

    # Or fetch a full table into a DataFrame
    df = connector.get_table("my_catalog", "my_schema", "my_table")

    # List available tables
    tables = connector.list_tables("my_catalog", "my_schema")

    # Export query results to CSV
    connector.query_to_csv("SELECT * FROM t", "output.csv")
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Optional

import pandas as pd
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Load environment variables from .env
# ---------------------------------------------------------------------------
_ENV_PATH = Path(__file__).resolve().parent / ".env"
load_dotenv(_ENV_PATH)


class DatabricksConnector:
    """Thin wrapper around Databricks SQL Connector and SDK."""

    def __init__(
        self,
        host: Optional[str] = None,
        http_path: Optional[str] = None,
        access_token: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
    ):
        self.host = host or os.getenv("DATABRICKS_HOST", "")
        self.http_path = http_path or os.getenv("DATABRICKS_HTTP_PATH", "")
        self.access_token = access_token or os.getenv("DATABRICKS_TOKEN", "")
        self.client_id = client_id or os.getenv("DATABRICKS_CLIENT_ID", "")
        self.client_secret = client_secret or os.getenv("DATABRICKS_CLIENT_SECRET", "")
        self.catalog = catalog or os.getenv("DATABRICKS_CATALOG", "main")
        self.schema = schema or os.getenv("DATABRICKS_SCHEMA", "default")

        self._validate_config()

    # ------------------------------------------------------------------
    # Configuration helpers
    # ------------------------------------------------------------------
    def _validate_config(self) -> None:
        """Raise early if required settings are missing."""
        missing = []
        if not self.host:
            missing.append("DATABRICKS_HOST")
        if not self.http_path:
            missing.append("DATABRICKS_HTTP_PATH")
        # Require either a PAT or OAuth client credentials
        if not self.access_token and not (self.client_id and self.client_secret):
            missing.append("DATABRICKS_TOKEN (or DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET)")
        if missing:
            raise EnvironmentError(
                f"Missing required Databricks config: {', '.join(missing)}. "
                "Set them in .env or pass them as arguments."
            )

    # ------------------------------------------------------------------
    # SQL Connector – run queries
    # ------------------------------------------------------------------
    def _get_sql_connection(self):
        """Create a new SQL connection (PAT or OAuth M2M)."""
        from databricks import sql as dbsql

        hostname = self.host.replace("https://", "").rstrip("/")

        if self.access_token:
            return dbsql.connect(
                server_hostname=hostname,
                http_path=self.http_path,
                access_token=self.access_token,
            )

        # Fall back to OAuth service principal
        from databricks.sdk.core import Config, oauth_service_principal

        cfg = Config(
            host=self.host,
            client_id=self.client_id,
            client_secret=self.client_secret,
        )

        def credential_provider():
            return oauth_service_principal(cfg)

        return dbsql.connect(
            server_hostname=hostname,
            http_path=self.http_path,
            credentials_provider=credential_provider,
        )

    def query(self, sql: str, params: Optional[dict] = None) -> pd.DataFrame:
        """Execute *sql* and return results as a pandas DataFrame.

        Parameters
        ----------
        sql : str
            SQL statement to execute.
        params : dict, optional
            Named parameters to bind into the query.

        Returns
        -------
        pd.DataFrame
        """
        conn = self._get_sql_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql, parameters=params)
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                return pd.DataFrame(rows, columns=columns)
        finally:
            conn.close()

    def execute(self, sql: str, params: Optional[dict] = None) -> None:
        """Execute a statement that does not return rows (DDL / DML)."""
        conn = self._get_sql_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql, parameters=params)
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Convenience data-access helpers
    # ------------------------------------------------------------------
    def get_table(
        self,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        table: str = "",
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """Fetch an entire table (or the first *limit* rows) as a DataFrame.

        Parameters
        ----------
        catalog : str, optional
            Unity Catalog catalog name (defaults to instance setting).
        schema : str, optional
            Schema / database name (defaults to instance setting).
        table : str
            Table name.
        limit : int, optional
            Maximum rows to fetch.  ``None`` means all rows.
        """
        cat = catalog or self.catalog
        sch = schema or self.schema
        fqn = f"`{cat}`.`{sch}`.`{table}`"
        sql = f"SELECT * FROM {fqn}"
        if limit:
            sql += f" LIMIT {int(limit)}"
        return self.query(sql)

    def query_to_csv(
        self,
        sql: str,
        output_path: str,
        params: Optional[dict] = None,
        index: bool = False,
    ) -> Path:
        """Run a query and save results to a CSV file.

        Returns the resolved output path.
        """
        df = self.query(sql, params=params)
        out = Path(output_path).resolve()
        df.to_csv(out, index=index)
        print(f"Saved {len(df)} rows to {out}")
        return out

    # ------------------------------------------------------------------
    # Databricks SDK – workspace / Unity Catalog APIs
    # ------------------------------------------------------------------
    def _get_workspace_client(self):
        """Return an authenticated ``WorkspaceClient`` (databricks-sdk)."""
        from databricks.sdk import WorkspaceClient

        if self.access_token:
            return WorkspaceClient(
                host=self.host,
                token=self.access_token,
            )

        return WorkspaceClient(
            host=self.host,
            client_id=self.client_id,
            client_secret=self.client_secret,
        )

    def list_catalogs(self) -> list[str]:
        """Return a list of catalog names visible to the service principal."""
        w = self._get_workspace_client()
        return [c.name for c in w.catalogs.list()]

    def list_schemas(self, catalog: Optional[str] = None) -> list[str]:
        """Return schema names within a catalog."""
        cat = catalog or self.catalog
        w = self._get_workspace_client()
        return [s.name for s in w.schemas.list(catalog_name=cat)]

    def list_tables(
        self, catalog: Optional[str] = None, schema: Optional[str] = None
    ) -> list[str]:
        """Return table names within a catalog.schema."""
        cat = catalog or self.catalog
        sch = schema or self.schema
        w = self._get_workspace_client()
        return [t.name for t in w.tables.list(catalog_name=cat, schema_name=sch)]

    def get_table_info(
        self,
        table: str,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
    ):
        """Return Unity Catalog ``TableInfo`` for *table*."""
        cat = catalog or self.catalog
        sch = schema or self.schema
        fqn = f"{cat}.{sch}.{table}"
        w = self._get_workspace_client()
        return w.tables.get(full_name=fqn)

    # ------------------------------------------------------------------
    # repr
    # ------------------------------------------------------------------
    def __repr__(self) -> str:
        return (
            f"DatabricksConnector(host='{self.host}', "
            f"catalog='{self.catalog}', schema='{self.schema}')"
        )


# ---------------------------------------------------------------------------
# Quick CLI smoke-test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    connector = DatabricksConnector()
    print(connector)

    if len(sys.argv) > 1:
        sql = " ".join(sys.argv[1:])
        print(f"\nRunning: {sql}\n")
        df = connector.query(sql)
        print(df.to_string(index=False))
    else:
        # List available catalogs first
        print("\nAvailable catalogs:")
        for c in connector.list_catalogs():
            print(f"  - {c}")

        # Then try to list tables in the configured catalog.schema
        try:
            print(f"\nTables in {connector.catalog}.{connector.schema}:")
            for t in connector.list_tables():
                print(f"  - {t}")
        except Exception as e:
            print(f"\nCould not list tables in {connector.catalog}.{connector.schema}: {e}")
            print("Update DATABRICKS_CATALOG and DATABRICKS_SCHEMA in .env to a valid catalog/schema from the list above.")
