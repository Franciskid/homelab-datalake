"""Extraction tests: partition boundaries, rotation, and the privacy ordering.

These use synthetic logs written into a tmp directory, including a gzipped
"rotated" file, because rotation crossing a partition boundary is the case the
whole design exists to survive.
"""

import gzip
import os
from datetime import date, datetime, timezone

from homelab_datalake.geo import GeoEnricher
from homelab_datalake.parsing.extract import discover_logs, extract_partition

SALT = "test-salt"
NO_GEO = GeoEnricher(None, None)


def _proxy_line(ts: str, client: str = "203.0.113.7", path: str = "/", host: str = "a.example.test"):
    return (
        f'[{ts}] - 200 200 - GET https {host} "{path}" [Client {client}] '
        f'[Length 97] [Gzip -] [Sent-to 192.0.2.10] "UA/1.0" "-"\n'
    )


def _write(tmp_path, name, lines, gzipped=False):
    p = tmp_path / name
    data = "".join(lines)
    if gzipped:
        with gzip.open(p, "wt") as fh:
            fh.write(data)
    else:
        p.write_text(data)
    return p


def test_discover_finds_live_and_rotated_but_not_error_logs(tmp_path):
    _write(tmp_path, "proxy-host-1_access.log", [_proxy_line("27/Aug/2026:10:00:00 +0000")])
    _write(tmp_path, "proxy-host-1_access.log.1.gz", [""], gzipped=True)
    _write(tmp_path, "proxy-host-1_error.log", ["some error\n"])
    found = {os.path.basename(p) for p in discover_logs(str(tmp_path))}
    assert found == {"proxy-host-1_access.log", "proxy-host-1_access.log.1.gz"}


def test_only_lines_from_the_partition_day_are_kept(tmp_path):
    _write(tmp_path, "proxy-host-1_access.log", [
        _proxy_line("26/Aug/2026:23:59:59 +0000"),
        _proxy_line("27/Aug/2026:00:00:00 +0000"),
        _proxy_line("27/Aug/2026:23:59:59 +0000"),
        _proxy_line("28/Aug/2026:00:00:00 +0000"),
    ])
    r = extract_partition(str(tmp_path), date(2026, 8, 27), "test", SALT, NO_GEO)
    assert len(r.rows) == 2
    assert r.out_of_partition == 2
    assert all(row["timestamp"].date() == date(2026, 8, 27) for row in r.rows)


def test_a_day_split_across_a_rotation_is_reassembled(tmp_path):
    """The case the whole design exists for: logrotate ran mid-day."""
    _write(tmp_path, "proxy-host-1_access.log.1.gz",
           [_proxy_line("27/Aug/2026:08:00:00 +0000", path="/early")], gzipped=True)
    _write(tmp_path, "proxy-host-1_access.log",
           [_proxy_line("27/Aug/2026:20:00:00 +0000", path="/late")])
    r = extract_partition(str(tmp_path), date(2026, 8, 27), "test", SALT, NO_GEO)
    assert {row["path"] for row in r.rows} == {"/early", "/late"}


def test_non_utc_offsets_are_normalised_before_the_day_filter(tmp_path):
    """23:30 in +0200 is 21:30 UTC the same day; 01:30 in +0200 is the previous day."""
    _write(tmp_path, "proxy-host-1_access.log", [
        _proxy_line("27/Aug/2026:23:30:00 +0200", path="/same-day-utc"),
        _proxy_line("28/Aug/2026:01:30:00 +0200", path="/previous-day-utc"),
    ])
    r = extract_partition(str(tmp_path), date(2026, 8, 27), "test", SALT, NO_GEO)
    assert {row["path"] for row in r.rows} == {"/same-day-utc", "/previous-day-utc"}
    assert all(row["timestamp"].tzinfo == timezone.utc for row in r.rows)


def test_extraction_is_idempotent(tmp_path):
    """PLAN.md's core property: running twice produces exactly the same rows."""
    _write(tmp_path, "proxy-host-1_access.log",
           [_proxy_line("27/Aug/2026:10:00:00 +0000", path=f"/p{i}") for i in range(20)])
    a = extract_partition(str(tmp_path), date(2026, 8, 27), "test", SALT, NO_GEO)
    b = extract_partition(str(tmp_path), date(2026, 8, 27), "test", SALT, NO_GEO)
    assert a.rows == b.rows


def test_no_row_contains_a_raw_client_address(tmp_path):
    """The guarantee that matters. Asserted over every value of every row."""
    address = "198.51.100.77"
    _write(tmp_path, "proxy-host-1_access.log",
           [_proxy_line("27/Aug/2026:10:00:00 +0000", client=address)])
    r = extract_partition(str(tmp_path), date(2026, 8, 27), "test", SALT, NO_GEO)
    assert r.rows, "expected a row to check"
    for row in r.rows:
        assert all(address not in str(v) for v in row.values())
        assert row["client_hash"] and row["client_network"] == "198.51.100.0/24"


def test_query_strings_are_stripped_at_extraction(tmp_path):
    _write(tmp_path, "proxy-host-1_access.log",
           [_proxy_line("27/Aug/2026:10:00:00 +0000", path="/reset?token=hunter2")])
    r = extract_partition(str(tmp_path), date(2026, 8, 27), "test", SALT, NO_GEO)
    assert r.rows[0]["path"] == "/reset"
    assert all("hunter2" not in str(v) for v in r.rows[0].values())


def test_unparseable_lines_are_recorded_as_rejects_not_dropped(tmp_path):
    _write(tmp_path, "proxy-host-1_access.log", [
        _proxy_line("27/Aug/2026:10:00:00 +0000"),
        "total garbage line\n",
    ])
    r = extract_partition(str(tmp_path), date(2026, 8, 27), "test", SALT, NO_GEO)
    assert len(r.rows) == 1
    assert len(r.rejects) == 1
    assert r.rejects[0]["raw_line"] == "total garbage line"
    assert r.stats.rejected == 1


def test_identical_rejects_in_live_and_rotated_files_count_once(tmp_path):
    _write(tmp_path, "proxy-host-1_access.log", ["garbage\n"])
    _write(tmp_path, "proxy-host-1_access.log.1.gz", ["garbage\n"], gzipped=True)
    r = extract_partition(str(tmp_path), date(2026, 8, 27), "test", SALT, NO_GEO)
    assert len(r.rejects) == 1


def test_files_older_than_the_partition_day_are_skipped(tmp_path):
    """The mtime prune. A file last written before the day cannot hold that day."""
    old = _write(tmp_path, "proxy-host-9_access.log.4.gz",
                 [_proxy_line("01/Aug/2026:10:00:00 +0000")], gzipped=True)
    os.utime(old, (1754000000, 1754000000))  # 1 Aug 2025-ish, well before the partition
    _write(tmp_path, "proxy-host-1_access.log", [_proxy_line("27/Aug/2026:10:00:00 +0000")])
    r = extract_partition(str(tmp_path), date(2026, 8, 27), "test", SALT, NO_GEO)
    assert r.files_read == 1 and r.files_skipped == 1


def test_geo_columns_are_null_when_no_database_is_configured(tmp_path):
    _write(tmp_path, "proxy-host-1_access.log", [_proxy_line("27/Aug/2026:10:00:00 +0000")])
    r = extract_partition(str(tmp_path), date(2026, 8, 27), "test", SALT, NO_GEO)
    assert r.geo_enabled is False
    assert r.rows[0]["geo_country_code"] is None and r.rows[0]["geo_asn"] is None


def test_cloudflare_client_is_labelled_as_a_cdn_edge(tmp_path):
    _write(tmp_path, "proxy-host-1_access.log", [
        _proxy_line("27/Aug/2026:10:00:00 +0000", client="172.71.130.17"),
        _proxy_line("27/Aug/2026:10:00:01 +0000", client="203.0.113.200"),
    ])
    r = extract_partition(str(tmp_path), date(2026, 8, 27), "test", SALT, NO_GEO)
    assert sorted(row["client_trust"] for row in r.rows) == ["cdn_edge", "resolved"]


def test_empty_directory_yields_nothing_without_failing(tmp_path):
    r = extract_partition(str(tmp_path), date(2026, 8, 27), "test", SALT, NO_GEO)
    assert r.rows == [] and r.rejects == [] and r.stats.total == 0
