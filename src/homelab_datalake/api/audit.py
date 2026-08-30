"""What was run, by whom, and what came back.

A write that cannot be traced is worse than a write that cannot be made. The
workbench can be switched into a mode where arbitrary SQL reaches the lake, and the
lake holds raw data that is not rebuildable -- the reverse-proxy logs rotate off the
disk and the camera serves only a recent window. When something is wrong a fortnight
later, the only question that matters is *which statement did that*, and there is no
way to answer it after the fact unless every statement was written down as it ran.

So the log records refusals too. A refused statement is the interesting half of the
record: it is what somebody tried, and a run of refusals against `raw/` is a signal
that no successful-statements-only log would contain.

**JSON Lines on disk, not a table.** The audit must survive the thing it is auditing.
A statement that damages the lake must not be able to damage the record of itself,
and an append-only file on a different volume is the cheapest way to get that. It is
also readable with `tail` at three in the morning, which a DuckDB file is not.

**It is bounded.** The host this runs on is [rate withheld] full. The file rotates by size and
keeps a fixed number of generations, so an audit log cannot be the thing that fills
the disk -- an outcome that would take the pipeline down to preserve a record of the
pipeline running.
"""

from __future__ import annotations

import json
import logging
import os
import threading
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_PATH = os.environ.get("DATALAKE_AUDIT_PATH", "/audit/queries.jsonl")

# [volume withheld] per generation, four generations. At roughly 400 bytes a record that is some
# [count withheld] statements in the current file and a third of a million in total, which is
# years of a workbench one person uses -- while costing at most [volume withheld] of a disk that
# has [volume withheld] free.
MAX_BYTES = 32 * 1024 * 1024
GENERATIONS = 3

# The statement travels into the record so the log answers "what did that" without a
# second lookup. Capped because a generated statement can be enormous, and an audit
# line that does not fit on a screen is one nobody reads.
MAX_SQL_CHARS = 4000


@dataclass
class Entry:
    """One statement, as it will be written down."""

    client_id: str
    user: str
    sql: str
    statement_type: str
    # The switch, as the caller asked for it -- not as it was honoured. A request to
    # write that was refused is exactly what this field exists to make findable.
    read_only: bool
    outcome: str  # ok | refused | error
    rows: int = 0
    truncated: bool = False
    elapsed_ms: float = 0.0
    detail: str = ""
    at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())

    def as_dict(self) -> dict:
        return {
            "at": self.at,
            "client_id": self.client_id,
            "user": self.user,
            "read_only": self.read_only,
            "statement_type": self.statement_type,
            "outcome": self.outcome,
            "rows": self.rows,
            "truncated": self.truncated,
            "elapsed_ms": round(self.elapsed_ms, 1),
            "detail": self.detail,
            "sql": self.sql[:MAX_SQL_CHARS],
        }


class AuditLog:
    """Append-only, rotated by size, safe to call from several request threads."""

    def __init__(self, path: str = DEFAULT_PATH, max_bytes: int = MAX_BYTES,
                 generations: int = GENERATIONS) -> None:
        self.path = Path(path)
        self.max_bytes = max_bytes
        self.generations = generations
        self._lock = threading.Lock()

    def write(self, entry: Entry) -> None:
        line = json.dumps(entry.as_dict(), ensure_ascii=False, default=str)
        # Mirrored to the process log as well as the file. The file is the record;
        # the log line is what shows up in `docker logs` beside the error somebody is
        # already looking at.
        logger.info("query %s: %s", entry.outcome, line)
        with self._lock:
            try:
                self._rotate_if_needed()
                self.path.parent.mkdir(parents=True, exist_ok=True)
                with open(self.path, "a", encoding="utf-8") as handle:
                    handle.write(line + "\n")
            except OSError as error:
                # Deliberately not raised. An unwritable audit volume must not turn
                # every query into a 500 -- but it must be loud, because the guarantee
                # this module exists to provide is gone until somebody fixes it.
                logger.error(
                    "AUDIT LOG UNWRITABLE at %s (%s). Statements are running "
                    "untraced. Record: %s", self.path, error, line,
                )

    def _rotate_if_needed(self) -> None:
        try:
            size = self.path.stat().st_size
        except OSError:
            return
        if size < self.max_bytes:
            return
        # Oldest first, so nothing is overwritten before it has been shifted along.
        oldest = self.path.with_suffix(self.path.suffix + f".{self.generations}")
        oldest.unlink(missing_ok=True)
        for generation in range(self.generations - 1, 0, -1):
            source = self.path.with_suffix(self.path.suffix + f".{generation}")
            if source.exists():
                source.replace(self.path.with_suffix(self.path.suffix + f".{generation + 1}"))
        self.path.replace(self.path.with_suffix(self.path.suffix + ".1"))
        logger.info("audit log rotated at %d bytes", size)
