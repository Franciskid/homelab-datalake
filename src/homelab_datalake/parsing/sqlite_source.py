"""Reading another application's live SQLite database without touching it.

Radarr, Sonarr and Jellyseerr keep their state in SQLite in WAL mode, and the datalake
mounts their config directories read-only. SQLite cannot open a WAL database on a
read-only mount -- even a reader needs to write the `-shm` index -- and mounting it
writable would let a pipeline corrupt the application it reads.

So the database, its `-wal` and its `-shm` are copied to scratch space and the copy is
opened. A copy taken while the application writes can be torn, which SQLite reports on
`quick_check`; the copy is retried, and a database whose files changed size or mtime
during the copy is treated the same way. Measured on primary: all three pass
`quick_check` on a live copy, Sonarr's [volume withheld] included.
"""

from __future__ import annotations

import shutil
import sqlite3
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path


class TornCopy(RuntimeError):
    """The database changed underneath every attempt to copy it."""


def _signature(path: Path) -> tuple:
    out = []
    for suffix in ("", "-wal", "-shm"):
        candidate = Path(str(path) + suffix)
        if candidate.exists():
            stat = candidate.stat()
            out.append((suffix, stat.st_size, stat.st_mtime_ns))
    return tuple(out)


@contextmanager
def snapshot(path: str | Path, attempts: int = 5, pause: float = 2.0):
    """A consistent private copy of a live SQLite database, as an open connection."""
    path = Path(path)
    if not path.is_file():
        raise FileNotFoundError(f"no database at {path}; is its directory mounted?")
    last_error: Exception | None = None
    for _attempt in range(attempts):
        with tempfile.TemporaryDirectory() as scratch:
            copy = Path(scratch) / path.name
            before = _signature(path)
            for suffix in ("", "-wal", "-shm"):
                source = Path(str(path) + suffix)
                if source.exists():
                    shutil.copyfile(source, Path(str(copy) + suffix))
            if _signature(path) != before:
                last_error = TornCopy(f"{path} changed while it was being copied")
                time.sleep(pause)
                continue
            connection = sqlite3.connect(copy)
            try:
                verdict = connection.execute("PRAGMA quick_check").fetchone()[0]
                if verdict != "ok":
                    last_error = TornCopy(f"{path}: quick_check said {verdict!r}")
                    connection.close()
                    time.sleep(pause)
                    continue
                yield connection
                return
            finally:
                connection.close()
    raise TornCopy(f"could not take a consistent copy of {path}: {last_error}")


def table_columns(connection: sqlite3.Connection, table: str) -> tuple[list[str], tuple[str, ...]]:
    """(columns in declared order, primary-key columns in key order)."""
    info = connection.execute(f'PRAGMA table_info("{table}")').fetchall()
    if not info:
        raise KeyError(f"table {table!r} does not exist")
    columns = [row[1] for row in info]
    key = tuple(row[1] for row in sorted((r for r in info if r[5]), key=lambda r: r[5]))
    return columns, key


def read_table(connection: sqlite3.Connection, table: str, exclude: frozenset[str] = frozenset()) -> tuple[list[dict], tuple[str, ...], list[str]]:
    """Every row of a table, minus excluded columns. Returns (rows, key, columns read)."""
    columns, key = table_columns(connection, table)
    selected = [c for c in columns if c not in exclude]
    quoted = ", ".join(f'"{c}"' for c in selected)
    cursor = connection.execute(f'SELECT {quoted} FROM "{table}"')  # noqa: S608 - names from PRAGMA
    rows = [dict(zip(selected, record, strict=True)) for record in cursor]
    return rows, tuple(k for k in key if k in selected), selected
