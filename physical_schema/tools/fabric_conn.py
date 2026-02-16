"""
Fabric Data Warehouse connection manager.

Uses pyodbc + azure-identity to connect to Microsoft Fabric via Azure AD.
The InteractiveBrowserCredential pops up a browser window for login on first use;
subsequent calls reuse the cached token until it expires.

Usage:
    conn = FabricConnection(server="...", database="RDMWarehouse")
    conn.connect()          # triggers browser login
    df = conn.execute(sql)  # returns pandas DataFrame
    conn.close()
"""

from __future__ import annotations

import os
import struct
from typing import Optional

import pandas as pd
import pyodbc
from azure.identity import InteractiveBrowserCredential

# Default Fabric connection settings (override via env vars)
DEFAULT_SERVER = os.getenv(
    "FABRIC_SERVER",
    "2znmjl7mstiu5llfjeqmjr2dhe-q2dw4ssa56cu7m2sosqamokrpq.datawarehouse.fabric.microsoft.com",
)
DEFAULT_DATABASE = os.getenv("FABRIC_DATABASE", "RDMWarehouse")
DEFAULT_DRIVER = os.getenv("FABRIC_DRIVER", "ODBC Driver 17 for SQL Server")
DEFAULT_ROW_LIMIT = int(os.getenv("FABRIC_ROW_LIMIT", "10000"))

# Azure AD token scope for SQL endpoints
_SQL_TOKEN_SCOPE = "https://database.windows.net/.default"


def _token_to_pyodbc_attrs(token: str) -> bytes:
    """Convert an Azure AD access token string to the binary format pyodbc expects."""
    encoded = token.encode("UTF-16-LE")
    return struct.pack(f"<I{len(encoded)}s", len(encoded), encoded)


class FabricConnection:
    """Manages a pyodbc connection to Microsoft Fabric with Azure AD auth."""

    def __init__(
        self,
        server: str = DEFAULT_SERVER,
        database: str = DEFAULT_DATABASE,
        driver: str = DEFAULT_DRIVER,
        row_limit: int = DEFAULT_ROW_LIMIT,
    ):
        self.server = server
        self.database = database
        self.driver = driver
        self.row_limit = row_limit
        self._conn: Optional[pyodbc.Connection] = None
        self._credential: Optional[InteractiveBrowserCredential] = None

    def connect(self) -> None:
        """Acquire Azure AD token and open a pyodbc connection to Fabric."""
        if self._credential is None:
            self._credential = InteractiveBrowserCredential()

        token = self._credential.get_token(_SQL_TOKEN_SCOPE)
        token_bytes = _token_to_pyodbc_attrs(token.token)

        conn_str = (
            f"Driver={{{self.driver}}};"
            f"Server={self.server};"
            f"Database={self.database};"
            "Encrypt=Yes;"
            "TrustServerCertificate=No;"
        )

        # SQL_COPT_SS_ACCESS_TOKEN = 1256
        self._conn = pyodbc.connect(conn_str, attrs_before={1256: token_bytes})

    def is_connected(self) -> bool:
        """Check if the connection is alive."""
        if self._conn is None:
            return False
        try:
            self._conn.execute("SELECT 1")
            return True
        except Exception:
            return False

    def execute(self, sql: str, row_limit: Optional[int] = None) -> pd.DataFrame:
        """Execute SQL and return results as a pandas DataFrame.

        Wraps the query in a TOP clause if it doesn't already have one
        to prevent accidentally pulling millions of rows.
        """
        if self._conn is None:
            raise RuntimeError("Not connected. Call connect() first.")

        limit = row_limit if row_limit is not None else self.row_limit

        # Wrap with TOP if the query doesn't already limit rows
        safe_sql = sql.strip()
        if limit and "TOP " not in safe_sql.upper().split("FROM")[0]:
            # Insert TOP after the first SELECT
            idx = safe_sql.upper().find("SELECT")
            if idx >= 0:
                insert_pos = idx + len("SELECT")
                safe_sql = safe_sql[:insert_pos] + f" TOP {limit}" + safe_sql[insert_pos:]

        return pd.read_sql(safe_sql, self._conn)

    def close(self) -> None:
        """Close the pyodbc connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None
