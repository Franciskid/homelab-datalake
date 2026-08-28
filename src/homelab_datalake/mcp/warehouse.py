"""The connection the tools share, and the one subtlety in holding it open.

The serving copy is replaced by an atomic rename every night. A process that opened
the file once and kept the handle would go on reading the *old* inode indefinitely --
the rename does not disturb an open file descriptor, which is exactly why the swap is
safe for in-flight queries and exactly why a long-lived reader would quietly serve
last week's numbers forever. Nothing would error. The answers would simply stop
changing, which is the worst way for a data service to fail.

So the identity of the file is checked before each query and the connection is
reopened when it has been replaced.
"""

from __future__ import annotations

import os
import threading

import duckdb

from homelab_datalake import serving


class Warehouse:
    """One read-only connection, reopened when the file underneath it is swapped."""

    def __init__(self, path: str | None = None) -> None:
        self._path = path or serving.SERVING_PATH
        self._con: duckdb.DuckDBPyConnection | None = None
        self._identity: tuple[int, int] | None = None
        self._lock = threading.Lock()

    def _file_identity(self) -> tuple[int, int]:
        stat = os.stat(self._path)
        return (stat.st_dev, stat.st_ino)

    def connection(self) -> duckdb.DuckDBPyConnection:
        with self._lock:
            identity = self._file_identity()
            if self._con is None or identity != self._identity:
                if self._con is not None:
                    self._con.close()
                self._con = serving.connect(self._path)
                self._identity = identity
            return self._con

    def cursor(self) -> duckdb.DuckDBPyConnection:
        """A cursor per query.

        DuckDB connections are not safe to use from several threads at once, and the
        MCP server can have more than one call in flight. `cursor()` gives each its
        own handle onto the same database.

        A cursor inherits neither the default catalog nor the session settings from
        the connection it came from, so both are set again here. Missing the timezone
        would be the quiet one: dates would render in the host's local zone and a
        23:30 UTC row would land on the wrong day, which is the same bug the rest of
        the pipeline pins UTC to avoid.
        """
        cursor = self.connection().cursor()
        cursor.execute(f"USE {serving.SERVING_ALIAS};")
        cursor.execute("SET TimeZone='UTC';")
        return cursor

    def close(self) -> None:
        with self._lock:
            if self._con is not None:
                self._con.close()
                self._con = None
                self._identity = None
