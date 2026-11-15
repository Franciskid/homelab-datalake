# Vendored from the politico repository (scripts/positions/db.py, e83b8f0). Do not edit here: change it there and run scripts/vendor-positions.py.
"""SQLite connection and migrations for research/positions/positions.sqlite.

Migrations are scripts/positions/schema/NNN_name.sql, applied in order inside one transaction
each; the applied version is PRAGMA user_version and a row in schema_migrations.
"""
from __future__ import annotations

import re
import sqlite3
from pathlib import Path

from common import DB_PATH, HERE, now

SCHEMA_DIR = HERE / 'schema'


def migrations(schema_dir: Path = SCHEMA_DIR) -> list[tuple[int, str, Path]]:
    out = []
    for p in sorted(schema_dir.glob('*.sql')):
        m = re.match(r'(\d+)_(.+)\.sql$', p.name)
        if m:
            out.append((int(m.group(1)), m.group(2), p))
    versions = [v for v, _, _ in out]
    if versions != list(range(1, len(versions) + 1)):
        raise RuntimeError(f'migrations must be numbered 1..n without gaps, got {versions}')
    return out


def version(conn: sqlite3.Connection) -> int:
    return conn.execute('PRAGMA user_version').fetchone()[0]


def migrate(conn: sqlite3.Connection, schema_dir: Path = SCHEMA_DIR) -> list[int]:
    applied = []
    current = version(conn)
    for v, name, path in migrations(schema_dir):
        if v <= current:
            continue
        sql = path.read_text()
        conn.execute('BEGIN')
        try:
            # executescript would commit implicitly; run statement by statement instead.
            for stmt in split_sql(sql):
                conn.execute(stmt)
            conn.execute('INSERT INTO schema_migrations (version, name, applied_at) VALUES (?,?,?)', (v, name, now()))
            conn.execute(f'PRAGMA user_version = {v}')
            conn.execute('COMMIT')
        except Exception:
            conn.execute('ROLLBACK')
            raise
        applied.append(v)
    return applied


def split_sql(sql: str) -> list[str]:
    sql = re.sub(r'--[^\n]*', '', sql)
    out, buf = [], ''
    for piece in sql.split(';'):
        buf += piece
        if sqlite3.complete_statement(buf + ';'):
            if buf.strip():
                out.append(buf.strip())
            buf = ''
        else:
            buf += ';'
    if buf.strip():
        raise ValueError(f'incomplete SQL statement: {buf[:80]}')
    return out


def connect(path: Path | str = DB_PATH, migrate_now: bool = True) -> sqlite3.Connection:
    if str(path) != ':memory:':
        Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), isolation_level=None)  # autocommit; explicit BEGIN where needed
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA foreign_keys = ON')
    conn.execute('PRAGMA busy_timeout = 120000')   # a long collector may be writing in another process
    conn.execute('PRAGMA journal_mode = WAL') if str(path) != ':memory:' else None
    if migrate_now:
        migrate(conn)
    return conn


class transaction:
    """with transaction(conn): ... — BEGIN/COMMIT, ROLLBACK on error."""

    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        self.conn.execute('BEGIN')
        return self.conn

    def __exit__(self, exc_type, *_):
        self.conn.execute('ROLLBACK' if exc_type else 'COMMIT')
        return False
