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

    # Phrases that indicate the TCP connection was dropped server-side.
    # Checked case-insensitively against str(exception).
    _STALE_CONNECTION_PHRASES = (
        "communication link failure",
        "broken pipe",
        "transport-level error",
        "08s01",
    )

    def is_connected(self) -> bool:
        """Check if the connection is alive."""
        if self._conn is None:
            return False
        try:
            self._conn.execute("SELECT 1")
            return True
        except Exception:
            return False

    def _is_stale_connection_error(self, exc: Exception) -> bool:
        """Return True if *exc* looks like a dropped/stale TCP connection."""
        err_lower = str(exc).lower()
        return any(phrase in err_lower for phrase in self._STALE_CONNECTION_PHRASES)

    def execute(self, sql: str, row_limit: Optional[int] = None) -> pd.DataFrame:
        """Execute SQL and return results as a pandas DataFrame.

        If the underlying TCP connection has gone stale (e.g. after idle time),
        a single transparent reconnect is attempted before raising the error.

        NOTE: Automatic TOP insertion has been DISABLED because it causes issues with:
        - CTEs (Common Table Expressions)
        - DISTINCT clauses
        - Complex queries with multiple SELECT statements

        If row limiting is needed, include TOP explicitly in your SQL.
        """
        if self._conn is None:
            raise RuntimeError("Not connected. Call connect() first.")

        # DISABLED: Automatic TOP insertion
        # limit = row_limit if row_limit is not None else self.row_limit
        # if limit and "TOP " not in sql.upper().split("FROM")[0]:
        #     idx = sql.upper().find("SELECT")
        #     if idx >= 0:
        #         insert_pos = idx + len("SELECT")
        #         sql = sql[:insert_pos] + f" TOP {limit}" + sql[insert_pos:]

        try:
            return pd.read_sql(sql, self._conn)
        except Exception as first_exc:
            if not self._is_stale_connection_error(first_exc):
                raise
            # Stale connection — attempt one silent reconnect (uses cached Azure AD
            # token; only opens a browser if the refresh token itself has expired).
            self._conn = None
            self.connect()
            return pd.read_sql(sql, self._conn)

    def close(self) -> None:
        """Close the pyodbc connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None
