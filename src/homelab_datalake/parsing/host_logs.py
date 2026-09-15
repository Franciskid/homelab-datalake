"""primary's own logs: the systemd journal, and auth.log.

**The journal is the complete record.** Everything rsyslog writes to syslog, auth.log
and kern.log comes out of journald first, so the journal is ingested whole: every entry,
every field. Measured on 2026-09-13: 222 [count withheld] entries and [volume withheld] of JSON in one day, [rate withheld]
of them `the container platform`. The journal on primary is capped at [volume withheld] and reached
back to 2026-08-24 on 2026-09-15.

It is read with `journalctl -D <dir> -o json` from a read-only mount of /var/log. The
host runs systemd 249 with zstd-compressed, keyed-hash journal files; the image's
journalctl reads both.

**auth.log is kept as well**, because logrotate keeps four weeks of it and the journal
three: on 2026-09-15 it held sign-ins and sudo back to 2026-08-16. Its lines carry no
year and are in the host's timezone (Etc/UTC on primary), so the year is taken from the
file's own modification time.
"""

from __future__ import annotations

import gzip
import json
import re
import subprocess
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pyarrow as pa

JOURNAL_SCHEMA = pa.schema([
    ("dt", pa.date32()),
    ("ts", pa.timestamp("us", tz="UTC")),
    ("cursor", pa.string()),
    ("boot_id", pa.string()),
    ("hostname", pa.string()),
    ("transport", pa.string()),
    ("priority", pa.int32()),
    ("facility", pa.int32()),
    ("syslog_identifier", pa.string()),
    ("pid", pa.int64()),
    ("uid", pa.int64()),
    ("gid", pa.int64()),
    ("comm", pa.string()),
    ("exe", pa.string()),
    ("cmdline", pa.string()),
    ("systemd_unit", pa.string()),
    ("systemd_slice", pa.string()),
    ("message_id", pa.string()),
    ("message", pa.string()),
    # Every field the entry had that is not a column above, as JSON. The journal's
    # field set is open-ended (CODE_FILE, EXIT_STATUS, JOB_RESULT, ...), and a lake
    # that keeps everything keeps those too.
    ("fields_json", pa.string()),
])

_COLUMN_FIELDS = {
    "__CURSOR": "cursor", "_BOOT_ID": "boot_id", "_HOSTNAME": "hostname", "_TRANSPORT": "transport",
    "SYSLOG_IDENTIFIER": "syslog_identifier", "_COMM": "comm", "_EXE": "exe", "_CMDLINE": "cmdline",
    "_SYSTEMD_UNIT": "systemd_unit", "_SYSTEMD_SLICE": "systemd_slice", "MESSAGE_ID": "message_id",
}
_INT_FIELDS = {"PRIORITY": "priority", "SYSLOG_FACILITY": "facility", "_PID": "pid", "_UID": "uid", "_GID": "gid"}
_OMITTED = {"__REALTIME_TIMESTAMP", "MESSAGE", *_COLUMN_FIELDS, *_INT_FIELDS}

AUTH_SCHEMA = pa.schema([
    ("dt", pa.date32()),
    ("ts", pa.timestamp("s", tz="UTC")),
    ("hostname", pa.string()),
    ("program", pa.string()),
    ("pid", pa.int64()),
    ("message", pa.string()),
    ("source_file", pa.string()),
    ("line_number", pa.int64()),
])


def _text(value) -> str | None:
    """journald emits a field as an array of byte values when it is not valid UTF-8."""
    if value is None:
        return None
    if isinstance(value, list):
        try:
            return bytes(value).decode("utf-8", errors="replace")
        except (TypeError, ValueError):
            return json.dumps(value)
    return str(value)


def _as_int(value) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def journal_row(entry: dict) -> dict | None:
    realtime = _as_int(entry.get("__REALTIME_TIMESTAMP"))
    if realtime is None:
        return None
    ts = datetime.fromtimestamp(realtime / 1_000_000, UTC)
    row = {"dt": ts.date(), "ts": ts, "message": _text(entry.get("MESSAGE"))}
    for source, column in _COLUMN_FIELDS.items():
        row[column] = _text(entry.get(source))
    for source, column in _INT_FIELDS.items():
        row[column] = _as_int(entry.get(source))
    rest = {k: v for k, v in entry.items() if k not in _OMITTED}
    row["fields_json"] = json.dumps(rest, sort_keys=True, ensure_ascii=False) if rest else None
    return row


def read_journal_day(journal_dir: str, day: date, batch_size: int = 100_000) -> Iterator[pa.Table]:
    """One UTC day of the journal, as tables of at most `batch_size` rows."""
    start = datetime(day.year, day.month, day.day, tzinfo=UTC)
    end = start + timedelta(days=1)
    command = [
        "journalctl", "-D", journal_dir, "--utc", "--no-pager", "-o", "json",
        f"--since=@{int(start.timestamp())}", f"--until=@{int(end.timestamp())}",
    ]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)  # noqa: S603
    rows: list[dict] = []
    if process.stdout is None:
        raise RuntimeError("journalctl started without a stdout pipe")
    for line in process.stdout:
        try:
            entry = json.loads(line)
        except ValueError:
            continue
        row = journal_row(entry)
        # --until is inclusive of its second; the partition is [start, end).
        if row is None or not (start <= row["ts"] < end):
            continue
        rows.append(row)
        if len(rows) >= batch_size:
            yield pa.Table.from_pylist(rows, schema=JOURNAL_SCHEMA)
            rows = []
    stderr = process.stderr.read().decode(errors="replace") if process.stderr else ""
    if process.wait() != 0:
        raise RuntimeError(f"journalctl exited {process.returncode}: {stderr.strip()[:500]}")
    if rows:
        yield pa.Table.from_pylist(rows, schema=JOURNAL_SCHEMA)


_SYSLOG_LINE = re.compile(
    r"^(?P<month>[A-Z][a-z]{2}) +(?P<day>\d{1,2}) (?P<time>\d{2}:\d{2}:\d{2}) "
    r"(?P<host>\S+) (?P<program>[^:\[\s]+)(?:\[(?P<pid>\d+)\])?: ?(?P<message>.*)$"
)
_MONTHS = {m: i for i, m in enumerate(
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], start=1)}


@dataclass
class AuthLog:
    rows_by_day: dict[date, list[dict]] = field(default_factory=dict)
    unparsed: int = 0


def _open(path: Path):
    return gzip.open(path, "rt", errors="replace") if path.suffix == ".gz" else open(path, errors="replace")


def read_auth_logs(log_dir: str) -> AuthLog:
    """Every line of auth.log and its rotations, grouped by UTC day."""
    out = AuthLog()
    for path in sorted(Path(log_dir).glob("auth.log*")):
        modified = datetime.fromtimestamp(path.stat().st_mtime, UTC)
        with _open(path) as handle:
            for number, line in enumerate(handle, start=1):
                match = _SYSLOG_LINE.match(line.rstrip("\n"))
                if not match:
                    out.unparsed += 1
                    continue
                month = _MONTHS[match["month"]]
                # A line cannot be newer than the file that holds it, so a month after
                # the file's own month belongs to the previous year (a December line
                # in a file rotated in January).
                year = modified.year - (1 if month > modified.month else 0)
                ts = datetime.strptime(
                    f"{year}-{month:02d}-{int(match['day']):02d} {match['time']}", "%Y-%m-%d %H:%M:%S"
                ).replace(tzinfo=UTC)
                out.rows_by_day.setdefault(ts.date(), []).append({
                    "dt": ts.date(), "ts": ts, "hostname": match["host"], "program": match["program"],
                    "pid": int(match["pid"]) if match["pid"] else None, "message": match["message"],
                    "source_file": path.name, "line_number": number,
                })
    return out
