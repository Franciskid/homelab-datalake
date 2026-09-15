"""Turning a source of NPM-shaped lines into one day's rows.

The hard part is that NPM's filenames carry a proxy-host id, not a date, and
logrotate moves files weekly. A single day's lines can therefore sit in the live
`.log`, or in any of `.log.1.gz` ... `.log.4.gz`, depending on when this runs.

So the partition is defined by the timestamp *inside* each line and never by the
file it came from. That is slower than trusting a filename and it is the only
version that survives a rotation landing mid-run: same date filter, same input
lines, same output, wherever they were found. It is also what makes re-reading the
same file safe, which is the property the lake-inbox path below depends on.

There are now three ways lines reach this module, and all three end up producing
rows through the same `_build_row` / `_extract_lines` core, so redaction, geo and the
row shape cannot drift between them:

  * `extract_partition` -- a local directory, mounted read-only (primary). Rotation-
    aware, mtime-pruned, NPM's three line formats.
  * `extract_partition_from_lake` -- the same NPM line formats, mirrored hourly into
    the lake bucket instead of mounted locally (secondary -- see WS-41.E2 part B). No mtime
    pruning: the mirror is small and overwritten hourly, so re-reading all of it every
    run is what keeps a rotation landing mid-sync safe, the same way mtime-pruning
    keeps a rotation landing mid-run safe on the mounted path.
  * `extract_mcp_api_partition` -- one JSON file per UTC day, no rotation at all
    (mcp.ai.example.test's own access log -- WS-41.E2 part C).
"""

from __future__ import annotations

import fnmatch
import glob
import gzip
import logging
import os
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta

from homelab_datalake.geo import GeoEnricher
from homelab_datalake.parsing.npm_log import ParsedLine, ParseStats, Rejected, parse_line
from homelab_datalake.privacy import query_param_names, raw_query_string, redact_client, strip_query

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
    # How many raw reject lines were dropped as duplicates of one already in
    # `rejects` -- the same unparseable line found in a live log and its rotated
    # copy. Tracked so `reconciles()` below can account for every line the source
    # produced without re-deriving the raw count from the deduplicated list.
    duplicate_rejects: int = 0


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


def _is_access_log_key(key: str) -> bool:
    """The inbox equivalent of `discover_logs`'s filter, applied to an object key.

    The mirror preserves secondary's own filenames, so the same glob shapes
    that select access logs out of a mounted directory select them out of the
    inbox -- matched against the basename, since an object key carries the whole
    `inbox/npm/secondary/...` prefix and a glob pattern like `*_access.log` is not meant
    to match across the slashes in that.
    """
    name = key.rsplit("/", 1)[-1]
    if "_error.log" in name:
        return False
    return any(fnmatch.fnmatch(name, pattern) for pattern in ACCESS_LOG_GLOBS)


def _iter_lake_inbox_lines(lake, keys: list[str]) -> Iterator[tuple[str, str]]:
    """Yield `(key, line)` for every access-log object under an inbox prefix.

    No mtime pruning here, unlike `iter_partition_lines`. The inbox is a small
    hourly mirror of secondary's own log directory, so the cost of reading all
    of it every run is small, and pruning by an object's own last-modified time
    would be the wrong optimisation anyway: the mirror overwrites objects in place,
    so an object's mtime says when it was last *synced*, not which lines it holds --
    a file that has not changed in three days can still hold today's lines if
    nothing has been written to its live tail since. Reading everything is what
    keeps a sync landing mid-run safe, the same way mtime-pruning keeps a rotation
    landing mid-run safe on the mounted path.
    """
    for key in keys:
        body = lake.read_bytes(key)
        if body is None:
            continue
        if key.endswith(".gz"):
            try:
                body = gzip.decompress(body)
            except OSError:
                logger.exception("could not decompress inbox object %s; skipping", key)
                continue
        text = body.decode("utf-8", errors="replace")
        for line in text.splitlines():
            yield key, line


def _iter_single_file_lines(path: str) -> Iterator[tuple[str, str]]:
    """Yield `(path, line)` for one file, or nothing if it does not exist.

    mcp-api writes one file per UTC day and never rotates it, so there is no
    directory to scan and no mtime pruning to do -- a missing file for a given day
    just means that day produced no lines yet (or never will, for a day before the
    log existed), which the caller tells apart from a broken mount by checking
    whether *any* `access-*.log` file exists at all.
    """
    if not os.path.exists(path):
        return
    try:
        with _open_log(path) as fh:
            for line in fh:
                yield path, line
    except OSError:
        logger.exception("could not read %s", path)


def _build_row(parsed: ParsedLine, day: date, host: str, salt: str, geo: GeoEnricher, source_file: str) -> dict:
    """One parsed line, enriched and redacted, as a row for `_SCHEMA`.

    Ordering is deliberate and is the point of the whole module: geography is
    looked up while the address is still in hand, then the address is redacted --
    both happen before the row exists at all. `client_address` and `query_string`
    below are the one exception to "nothing raw survives": the 2026-09-14 decision
    (see `homelab_datalake.privacy`) keeps the raw values too, in the same row as
    their redacted counterparts, for the private warehouse only.
    """
    ts_utc = parsed.timestamp.astimezone(UTC)
    info = geo.lookup(parsed.client)
    redacted = redact_client(parsed.client, salt)
    return {
        "dt": day,
        "host_source": host,
        "timestamp": ts_utc,
        "log_format": parsed.log_format,
        "request_host": parsed.host,
        "method": parsed.method,
        "scheme": parsed.scheme,
        "path": strip_query(parsed.uri),
        # The names only; `strip_query` above still drops the query string from
        # `path`. Without these the query string vanished entirely, and with it any
        # way to ask which endpoints take parameters at all.
        "has_query": bool(parsed.uri and "?" in parsed.uri),
        "query_param_names": query_param_names(parsed.uri),
        # Raw, private-warehouse-only. See homelab_datalake.privacy for the 2026-09-14
        # decision and homelab_datalake.mcp.semantic.WEB_TRAFFIC for from which date
        # these are populated.
        "query_string": raw_query_string(parsed.uri),
        "status": parsed.status,
        "upstream_status": parsed.upstream_status,
        "cache_status": parsed.cache_status,
        "bytes_sent": parsed.bytes_sent,
        "user_agent": parsed.user_agent,
        # Parsed from every line since this pipeline was written, carried on
        # ParsedLine, and then left out of the schema -- so it was read and
        # discarded on all [count withheld] lines a day. It is how one tells a link
        # somebody followed from a URL somebody typed.
        "referer": parsed.referer,
        "gzip_ratio": parsed.gzip_ratio,
        "sent_to": parsed.sent_to,
        # Raw, private-warehouse-only -- see the note on query_string above.
        "client_address": parsed.client,
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
        "source_file": source_file,
    }


def _extract_lines(
    lines: Iterator[tuple[str, str]],
    day: date,
    host: str,
    salt: str,
    geo: GeoEnricher,
    parse_line_fn,
    result: ExtractResult,
) -> ExtractResult:
    """The shared core: parse, filter by day, enrich, redact, dedupe rejects.

    Every one of the three public entry points below builds its own line iterator
    and its own `files_read`/`scanned_files` metadata, then hands both to this
    function -- which is what guarantees a lake-mirrored secondary line and a
    locally-mounted primary line turn into byte-identical rows.
    """
    day_start = datetime.combine(day, time.min, tzinfo=UTC)
    day_end = day_start + timedelta(days=1)

    for source_label, line in lines:
        result.stats.total += 1
        parsed = parse_line_fn(line)

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
                    "source_file": os.path.basename(source_label),
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

        result.rows.append(_build_row(parsed, day, host, salt, geo, os.path.basename(source_label)))

    # Rejects are deduplicated: the same unparseable line found in both the live log
    # and its rotated copy is one problem, not two. The count removed by that is kept
    # on the result, so `reconciles()` can still account for every line seen.
    raw_reject_count = result.stats.rejected
    seen: set[tuple] = set()
    unique: list[dict] = []
    for reject in result.rejects:
        key = (reject["reason"], reject["raw_line"])
        if key not in seen:
            seen.add(key)
            unique.append(reject)
    result.duplicate_rejects = raw_reject_count - len(unique)
    result.rejects = unique

    return result


def extract_partition(
    log_dir: str,
    day: date,
    host: str,
    salt: str,
    geo: GeoEnricher | None = None,
    parse_line_fn=parse_line,
) -> ExtractResult:
    """Parse, enrich, redact and filter one day of logs from a mounted directory.

    `parse_line_fn` defaults to NPM's own three-format parser; nothing about this
    function is NPM-specific otherwise, which is what lets mcp-api reuse the mtime
    pruning and rotation handling below it by passing a different one in -- except
    mcp-api's single day-named file has no rotation to handle, so it uses
    `extract_mcp_api_partition` instead, which skips straight to `_extract_lines`.
    """
    geo = geo or GeoEnricher.from_env()
    day_start = datetime.combine(day, time.min, tzinfo=UTC)

    result = ExtractResult(rows=[], rejects=[], stats=ParseStats(), geo_enabled=geo.enabled)
    all_logs = discover_logs(log_dir)
    considered = [p for p in all_logs if _can_contain(p, day_start)]
    result.files_read = len(considered)
    result.files_skipped = len(all_logs) - len(considered)
    result.scanned_files = [os.path.basename(p) for p in considered]

    return _extract_lines(iter_partition_lines(log_dir, day), day, host, salt, geo, parse_line_fn, result)


def extract_partition_from_lake(
    lake,
    prefix: str,
    day: date,
    host: str,
    salt: str,
    geo: GeoEnricher | None = None,
    parse_line_fn=parse_line,
) -> ExtractResult:
    """Parse, enrich, redact and filter one day of logs mirrored into the lake.

    Used for secondary: secondary's own NPM logs are mirrored hourly into
    `inbox/npm/secondary/` by `infra/secondary-log-sync/` rather than mounted, because the
    datalake containers cannot reach that host directly. Same line formats, same
    redaction, same day filter as `extract_partition` -- only where the bytes come
    from differs, via `lake.list_keys`/`lake.read_bytes` instead of the filesystem.
    """
    geo = geo or GeoEnricher.from_env()
    result = ExtractResult(rows=[], rejects=[], stats=ParseStats(), geo_enabled=geo.enabled)
    keys = [k for k in lake.list_keys(prefix) if _is_access_log_key(k)]
    result.files_read = len(keys)
    result.scanned_files = [k.rsplit("/", 1)[-1] for k in keys]

    return _extract_lines(_iter_lake_inbox_lines(lake, keys), day, host, salt, geo, parse_line_fn, result)


def extract_mcp_api_partition(
    log_dir: str,
    day: date,
    salt: str,
    geo: GeoEnricher | None = None,
) -> ExtractResult:
    """Parse, enrich, redact and filter one day of mcp.ai.example.test's own log.

    One JSON file per UTC day, `access-YYYY-MM-DD.log`, no rotation -- so there is
    no directory scan and no mtime pruning, just the one file this day names. A
    missing file is not an error here: the caller (`assets.npm_access.mcp_api_access_raw`)
    is what tells a genuinely broken mount apart from a day whose file has not been
    written yet, by checking whether the directory holds *any* `access-*.log` file.
    """
    from homelab_datalake.parsing.mcp_api_log import parse_line as parse_mcp_api_line

    geo = geo or GeoEnricher.from_env()
    result = ExtractResult(rows=[], rejects=[], stats=ParseStats(), geo_enabled=geo.enabled)
    path = os.path.join(log_dir, f"access-{day.isoformat()}.log")
    result.files_read = 1 if os.path.exists(path) else 0
    result.scanned_files = [os.path.basename(path)] if result.files_read else []

    return _extract_lines(
        _iter_single_file_lines(path), day, "mcp-api", salt, geo, parse_mcp_api_line, result
    )


def reconciles(result: ExtractResult) -> bool:
    """True when every line the source produced is accounted for.

    Every line seen while building a partition lands in exactly one of four places:
    a written row, a written reject, a reject whose exact text had already been
    written for this partition (a duplicate, from a rotated copy or a re-synced
    mirror object), or a line whose own timestamp put it in a different day. If the
    total stops matching that sum, something in the pipeline silently dropped a
    line rather than accounting for it -- which is the failure this exists to catch
    before it becomes a wrong number several models downstream. See
    `AssetCheckResult` for `CHECK_RECONCILED` in `assets.npm_access`, which runs
    this per host and day.
    """
    accounted = (
        len(result.rows) + len(result.rejects) + result.duplicate_rejects + result.out_of_partition
    )
    return result.stats.total == accounted


# Re-exported so callers that only need the shape of a parse failure -- the
# reconciliation check's tests, for instance -- do not have to import npm_log too.
__all__ = [
    "ExtractResult",
    "Rejected",
    "discover_logs",
    "extract_mcp_api_partition",
    "extract_partition",
    "extract_partition_from_lake",
    "iter_partition_lines",
    "reconciles",
]
