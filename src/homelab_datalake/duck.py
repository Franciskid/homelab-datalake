"""Every DuckDB connection the pipeline opens, bounded.

The datalake daemon runs under an 8 GB memory cap (docker-compose.yml). DuckDB does
not know about it: unbounded, each connection takes up to 80% of the host's RAM, and
on 21 Nov 2026 a full rebuild ran two steps side by side, filled the server's 31 GB
and its swap, and nearly brought the host down. So each connection gets an explicit
memory limit and a temp directory on disk to spill to, and the analytics job runs at
most two steps at a time (definitions.py): two connections at this limit, plus the
daemon itself, stay under the cap.
"""

import os
from pathlib import Path

import duckdb

MEMORY_LIMIT = os.environ.get("DUCKDB_MEMORY_LIMIT", "2500MB")
THREADS = int(os.environ.get("DUCKDB_THREADS", "4"))
TEMP_DIRECTORY = os.environ.get("DUCKDB_TEMP_DIRECTORY") or str(
    Path(os.environ.get("DUCKDB_PATH", "warehouse/datalake.duckdb")).resolve().parent / "duckdb_tmp"
)


def settings() -> dict[str, str | int]:
    config: dict[str, str | int] = {"memory_limit": MEMORY_LIMIT, "threads": THREADS}
    # The readers (MCP, API) mount the warehouse read-only and never spill; they get
    # the memory limit and DuckDB's own default temp location.
    try:
        Path(TEMP_DIRECTORY).mkdir(parents=True, exist_ok=True)
    except OSError:
        return config
    if os.access(TEMP_DIRECTORY, os.W_OK):
        config["temp_directory"] = TEMP_DIRECTORY
    return config


def connect(database: str = ":memory:", read_only: bool = False) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(database, read_only=read_only, config=settings())
