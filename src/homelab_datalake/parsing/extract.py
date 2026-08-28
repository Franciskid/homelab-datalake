"""Turning a directory of NPM logs into one day's rows.

The hard part is that NPM's filenames carry a proxy-host id, not a date, and
logrotate moves files weekly. A single day's lines can therefore sit in the live
`.log`, or in any of `.log.1.gz` ... `.log.4.gz`, depending on when this runs.

So the partition is defined by the timestamp *inside* each line and never by the
file it came from. That is slower than trusting a filename and it is the only
version that survives a rotation landing mid-run: same date filter, same input
lines, same output, wherever they were found.
"""

from __future__ import annotations

import glob
import gzip
import logging
import os
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta

from homelab_datalake.geo import GeoEnricher
from homelab_datalake.parsing.npm_log import ParsedLine, ParseStats, parse_line
from homelab_datalake.privacy import redact_client, strip_query

logger = logging.getLogger(__name__)

ACCESS_LOG_GLOBS = ("*_access.log", "*_access.log.*.gz", "*_access.log.*")


@dataclass
class ExtractResult:
    rows: list[dict]
    rejects: list[dict]
    stats: ParseStats
    files_read: int = 0
    files_skipped: int = 0
    geo_enabled: bool = False
    out_of_partition: int = 0
    scanned_files: list[str] = field(default_factory=list)


def _open_log(path: str):
    if path.endswith(".gz"):
        return gzip.open(path, "rt", errors="replace")
    return open(path, errors="replace")


def discover_logs(log_dir: str) -> list[str]:
    """Every access log in the directory, live and rotated, deduplicated."""
    found: set[str] = set()
    for pattern in ACCESS_LOG_GLOBS:
        found.update(glob.glob(os.path.join(log_dir, pattern)))
    # *_error.log siblings match none of the globs above, but be explicit anyway:
    # an error log has a different format and would be pure reject noise.
    return sorted(p for p in found if "_error.log" not in os.path.basename(p))


def _can_contain(path: str, day_start: datetime) -> bool:
    """Could this file hold lines from the partition day?

    A file last modified before the day began cannot contain lines from that day.
    That prunes most rotated archives on a normal run -- on secondary it is the
    difference between reading [volume withheld] and reading the ~[volume withheld] that is actually live.
    The reverse is not true: a current log holds old lines too, so nothing is ever
    skipped for being too new.
    """
    try:
        mtime = datetime.fromtimestamp(os.path.getmtime(path), tz=UTC)
    except OSError:
        return True  # unreadable stat is not a reason to silently skip data
    return mtime >= day_start


def iter_partition_lines(log_dir: str, day: date) -> Iterator[tuple[str, str]]:
    """Yield `(path, line)` for files that could hold lines from `day`."""
    day_start = datetime.combine(day, time.min, tzinfo=UTC)
    for path in discover_logs(log_dir):
        if not _can_contain(path, day_start):
            continue
        try:
            with _open_log(path) as fh:
                for line in fh:
                    yield path, line
        except OSError:
            logger.exception("could not read %s; continuing with the rest", path)


def extract_partition(
    log_dir: str,
    day: date,
    host: str,
    salt: str,
    geo: GeoEnricher | None = None,
) -> ExtractResult:
    """Parse, enrich, redact and filter one day of logs from one host.

    Ordering is deliberate and is the point of the whole module: parse, then look up
    geography while the address is still in hand, then redact it, then keep the row.
    No raw address is ever put in `rows`, so nothing downstream can persist one.
    """
    geo = geo or GeoEnricher.from_env()
    day_start = datetime.combine(day, time.min, tzinfo=UTC)
    day_end = day_start + timedelta(days=1)

    result = ExtractResult(rows=[], rejects=[], stats=ParseStats(), geo_enabled=geo.enabled)
    all_logs = discover_logs(log_dir)
    considered = [p for p in all_logs if _can_contain(p, day_start)]
    result.files_read = len(considered)
    result.files_skipped = len(all_logs) - len(considered)
    result.scanned_files = [os.path.basename(p) for p in considered]

    for path, line in iter_partition_lines(log_dir, day):
        result.stats.total += 1
        parsed = parse_line(line)

        if not isinstance(parsed, ParsedLine):
            # A reject is only recorded if it might belong to this partition -- which
            # cannot be known, since it has no usable timestamp. Recording every reject
            # in every partition would multiply them by the number of days backfilled,
            # so they are attributed to the day being built and deduplicated by hash.
            result.stats.rejected += 1
            result.rejects.append(
                {
                    "dt": day,
                    "host": host,
                    "source_file": os.path.basename(path),
                    "reason": parsed.reason,
                    "raw_line": parsed.raw[:2000],
                }
            )
            continue

        result.stats.parsed += 1
        fmt = parsed.log_format
        result.stats.by_format[fmt] = result.stats.by_format.get(fmt, 0) + 1

        ts_utc = parsed.timestamp.astimezone(UTC)
        if not (day_start <= ts_utc < day_end):
            result.out_of_partition += 1
            continue

        info = geo.lookup(parsed.client)
        redacted = redact_client(parsed.client, salt)

        result.rows.append(
            {
                "dt": day,
                "host_source": host,
                "timestamp": ts_utc,
                "log_format": fmt,
                "request_host": parsed.host,
                "method": parsed.method,
                "scheme": parsed.scheme,
                "path": strip_query(parsed.uri),
                "status": parsed.status,
                "upstream_status": parsed.upstream_status,
                "cache_status": parsed.cache_status,
                "bytes_sent": parsed.bytes_sent,
                "user_agent": parsed.user_agent,
                "sent_to": parsed.sent_to,
                "client_hash": redacted.client_hash,
                "client_network": redacted.client_network,
                "client_trust": redacted.client_trust,
                "client_version": redacted.client_version,
                "client_is_private": redacted.is_private,
                "geo_country_code": info.country_code,
                "geo_country_name": info.country_name,
                "geo_city": info.city,
                "geo_asn": info.asn,
                "geo_as_org": info.as_org,
                "source_file": os.path.basename(path),
            }
        )

    # Rejects are deduplicated: the same unparseable line found in both the live log
    # and its rotated copy is one problem, not two.
    seen: set[tuple] = set()
    unique: list[dict] = []
    for reject in result.rejects:
        key = (reject["reason"], reject["raw_line"])
        if key not in seen:
            seen.add(key)
            unique.append(reject)
    result.rejects = unique

    return result
