"""Extraction tests: partition boundaries, rotation, and the privacy ordering.

These use synthetic logs written into a tmp directory, including a gzipped
"rotated" file, because rotation crossing a partition boundary is the case the
whole design exists to survive.
"""

import gzip
import json
import os
from datetime import UTC, date

from homelab_datalake.geo import GeoEnricher
from homelab_datalake.parsing.extract import (
    discover_logs,
    extract_mcp_api_partition,
    extract_partition,
    extract_partition_from_lake,
    reconciles,
)
from homelab_datalake.resources import LakeStorage

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
    assert all(row["timestamp"].tzinfo == UTC for row in r.rows)


def test_extraction_is_idempotent(tmp_path):
    """PLAN.md's core property: running twice produces exactly the same rows."""
    _write(tmp_path, "proxy-host-1_access.log",
           [_proxy_line("27/Aug/2026:10:00:00 +0000", path=f"/p{i}") for i in range(20)])
    a = extract_partition(str(tmp_path), date(2026, 8, 27), "test", SALT, NO_GEO)
    b = extract_partition(str(tmp_path), date(2026, 8, 27), "test", SALT, NO_GEO)
    assert a.rows == b.rows


def test_the_raw_address_lands_only_in_client_address(tmp_path):
    """2026-09-14: the raw address is kept, but only in the one column meant for it.

    Before that decision this test asserted the address appeared nowhere in the
    row at all. It still must not leak into anything else -- path, user agent,
    source_file -- and the derived columns must still be exactly as before; what
    changed is that `client_address` itself is now allowed, and expected, to hold
    it. See homelab_datalake.privacy for the decision.
    """
    address = "198.51.100.77"
    _write(tmp_path, "proxy-host-1_access.log",
           [_proxy_line("27/Aug/2026:10:00:00 +0000", client=address)])
    r = extract_partition(str(tmp_path), date(2026, 8, 27), "test", SALT, NO_GEO)
    assert r.rows, "expected a row to check"
    for row in r.rows:
        assert row["client_address"] == address
        other_values = {k: v for k, v in row.items() if k != "client_address"}
        assert all(address not in str(v) for v in other_values.values())
        assert row["client_hash"] and row["client_network"] == "198.51.100.0/24"


def test_query_strings_are_stripped_from_path_but_kept_raw(tmp_path):
    """path loses the query string as before; query_string now keeps it, raw.

    Before 2026-09-14 the value was dropped everywhere and this test asserted its
    absence from the whole row. Now it survives in exactly one column -- see
    homelab_datalake.privacy for the decision.
    """
    _write(tmp_path, "proxy-host-1_access.log",
           [_proxy_line("27/Aug/2026:10:00:00 +0000", path="/reset?token=hunter2")])
    r = extract_partition(str(tmp_path), date(2026, 8, 27), "test", SALT, NO_GEO)
    assert r.rows[0]["path"] == "/reset"
    assert r.rows[0]["query_string"] == "token=hunter2"
    other_values = {k: v for k, v in r.rows[0].items() if k != "query_string"}
    assert all("hunter2" not in str(v) for v in other_values.values())


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


def test_a_directory_with_no_access_logs_is_distinguishable_from_a_quiet_day(tmp_path):
    """An empty directory means a broken mount or a stalled sync, not zero traffic.

    The asset raises on this rather than writing an empty partition, because a
    partition that is green and empty looks exactly like a genuinely quiet day, and
    the pipeline would keep reporting success while receiving nothing at all.
    """
    (tmp_path / "proxy-host-1_error.log").write_text("not an access log\n")
    assert discover_logs(str(tmp_path)) == []

    # An access log with no lines for the day is the *other* case, and stays legal.
    _write(tmp_path, "proxy-host-1_access.log", [_proxy_line("01/Jan/2026:10:00:00 +0000")])
    assert discover_logs(str(tmp_path))
    r = extract_partition(str(tmp_path), date(2026, 8, 27), "test", SALT, NO_GEO)
    assert r.rows == [] and r.out_of_partition == 1


# --- the lake inbox (secondary, WS-41.E2 part B) --------------------------------------
#
# Same NPM line formats and the same redaction, only sourced through
# LakeStorage.list_keys/read_bytes instead of the filesystem -- so what is worth
# pinning here is specifically that indirection: plain and gzipped objects both
# read correctly, and a line repeated across what the hourly mirror leaves behind
# after a rotation still counts once.


def _lake(tmp_path) -> LakeStorage:
    return LakeStorage(local_root=str(tmp_path / "lake"))


def test_inbox_reads_a_plain_object(tmp_path):
    lake = _lake(tmp_path)
    lake.write_bytes(
        "inbox/npm/secondary/proxy-host-1_access.log",
        _proxy_line("27/Aug/2026:10:00:00 +0000", path="/plain").encode(),
    )
    r = extract_partition_from_lake(lake, "inbox/npm/secondary", date(2026, 8, 27), "secondary", SALT, NO_GEO)
    assert [row["path"] for row in r.rows] == ["/plain"]
    assert r.rows[0]["host_source"] == "secondary"


def test_inbox_reads_a_gzipped_object(tmp_path):
    lake = _lake(tmp_path)
    payload = gzip.compress(_proxy_line("27/Aug/2026:10:00:00 +0000", path="/rotated").encode())
    lake.write_bytes("inbox/npm/secondary/proxy-host-1_access.log.1.gz", payload)
    r = extract_partition_from_lake(lake, "inbox/npm/secondary", date(2026, 8, 27), "secondary", SALT, NO_GEO)
    assert [row["path"] for row in r.rows] == ["/rotated"]


def test_inbox_reads_plain_and_gzipped_objects_together(tmp_path):
    """The case a rotation landing between two hourly syncs produces."""
    lake = _lake(tmp_path)
    lake.write_bytes(
        "inbox/npm/secondary/proxy-host-1_access.log",
        _proxy_line("27/Aug/2026:20:00:00 +0000", path="/live").encode(),
    )
    lake.write_bytes(
        "inbox/npm/secondary/proxy-host-1_access.log.1.gz",
        gzip.compress(_proxy_line("27/Aug/2026:08:00:00 +0000", path="/rotated").encode()),
    )
    r = extract_partition_from_lake(lake, "inbox/npm/secondary", date(2026, 8, 27), "secondary", SALT, NO_GEO)
    assert {row["path"] for row in r.rows} == {"/live", "/rotated"}


def test_inbox_ignores_error_logs_and_objects_outside_the_prefix(tmp_path):
    lake = _lake(tmp_path)
    lake.write_bytes(
        "inbox/npm/secondary/proxy-host-1_access.log",
        _proxy_line("27/Aug/2026:10:00:00 +0000").encode(),
    )
    lake.write_bytes("inbox/npm/secondary/proxy-host-1_error.log", b"some nginx error\n")
    lake.write_bytes(
        "inbox/npm/primary/proxy-host-1_access.log",
        _proxy_line("27/Aug/2026:10:00:00 +0000", path="/wrong-host").encode(),
    )
    r = extract_partition_from_lake(lake, "inbox/npm/secondary", date(2026, 8, 27), "secondary", SALT, NO_GEO)
    assert len(r.rows) == 1
    assert r.rows[0]["path"] == "/"


def test_secondary_dedupe_across_repeated_mirrors(tmp_path):
    """The hourly mirror overwrites objects in place, but a rotation can leave the
    same broken line sitting under two keys at once -- the live file's last sync
    before it rotated, and the rotated copy that appeared under a new key. That is
    one problem, not two, exactly as it is for the mounted primary directory (see
    test_identical_rejects_in_live_and_rotated_files_count_once above)."""
    lake = _lake(tmp_path)
    lake.write_bytes("inbox/npm/secondary/proxy-host-1_access.log", b"garbage\n")
    lake.write_bytes(
        "inbox/npm/secondary/proxy-host-1_access.log.1.gz", gzip.compress(b"garbage\n")
    )
    r = extract_partition_from_lake(lake, "inbox/npm/secondary", date(2026, 8, 27), "secondary", SALT, NO_GEO)
    assert len(r.rejects) == 1
    assert r.duplicate_rejects == 1
    assert reconciles(r)


def test_an_inbox_object_that_vanishes_mid_read_is_skipped_not_fatal(tmp_path):
    """list_keys sees a key, then the object is gone by the time it is read --
    the mirror overwrote it between the two calls. Not an error: the caller loses
    that one object's lines for this run, and the next sync brings it back."""
    lake = _lake(tmp_path)
    lake.write_bytes(
        "inbox/npm/secondary/proxy-host-1_access.log",
        _proxy_line("27/Aug/2026:10:00:00 +0000").encode(),
    )
    assert lake.read_bytes("inbox/npm/secondary/missing_access.log") is None


# --- mcp.ai's own access log (WS-41.E2 part C) ----------------------------------


def _mcp_api_line(time="2026-09-14T10:00:00Z", client="203.0.113.9", uri="/mcp/tools/call", status=200):
    return json.dumps(
        {
            "time": time,
            "remote_addr": client,
            "method": "POST",
            "host": "mcp.ai.example.test",
            "request_uri": uri,
            "status": status,
            "body_bytes_sent": 128,
            "request_length": 400,
            "request_time": "0.02",
            "upstream_status": "200",
            "upstream_response_time": "0.018",
            "user_agent": "litellm/1.0",
            "referer": "-",
        }
    ) + "\n"


def test_mcp_api_partition_extracts_and_redacts_like_any_other_host(tmp_path):
    (tmp_path / "access-2026-09-14.log").write_text(
        _mcp_api_line(uri="/mcp/tools/call?tool=web_traffic")
    )
    r = extract_mcp_api_partition(str(tmp_path), date(2026, 9, 14), SALT, NO_GEO)
    assert len(r.rows) == 1
    row = r.rows[0]
    assert row["host_source"] == "mcp-api"
    assert row["log_format"] == "mcp_api_json"
    assert row["path"] == "/mcp/tools/call"
    assert row["query_string"] == "tool=web_traffic"
    assert row["client_address"] == "203.0.113.9"
    assert row["client_hash"] and row["client_network"] == "203.0.113.0/24"


def test_mcp_api_partition_filters_by_day_like_every_other_source(tmp_path):
    (tmp_path / "access-2026-09-14.log").write_text(
        _mcp_api_line(time="2026-09-13T23:59:59Z") + _mcp_api_line(time="2026-09-14T00:00:01Z")
    )
    r = extract_mcp_api_partition(str(tmp_path), date(2026, 9, 14), SALT, NO_GEO)
    assert len(r.rows) == 1
    assert r.out_of_partition == 1


def test_mcp_api_partition_with_no_file_for_the_day_is_a_quiet_day_not_an_error(tmp_path):
    (tmp_path / "access-2026-09-13.log").write_text(_mcp_api_line())
    r = extract_mcp_api_partition(str(tmp_path), date(2026, 9, 14), SALT, NO_GEO)
    assert r.rows == [] and r.rejects == [] and r.stats.total == 0
    assert r.files_read == 0


def test_mcp_api_partition_with_an_empty_directory_is_also_a_quiet_day(tmp_path):
    """The case MCP_API_LOG_START exists for: the first day this source has ever
    had, before the platform's nginx has written anything at all -- not one file
    for a different day, but none whatsoever. Still not an error: reconciliation
    must pass with zero lines and zero rows, the same as any other quiet day."""
    r = extract_mcp_api_partition(str(tmp_path), date(2026, 9, 15), SALT, NO_GEO)
    assert r.rows == [] and r.rejects == [] and r.stats.total == 0
    assert reconciles(r)


def test_mcp_api_partition_matches_the_platforms_own_sample_shape(tmp_path):
    """host_source, and the raw client_address/query_string, exactly as the
    platform's validated line would produce them -- with a documentation address
    substituted for the real one that line carried (see test_mcp_api_log.py's
    PLATFORM_SAMPLE for why), and every other field, including the
    comma-separated upstream_status and the empty-string referer, untouched."""
    line = json.dumps(
        {
            "time": "2026-09-14T22:08:27+00:00",
            "remote_addr": "203.0.113.117",
            "method": "POST",
            "host": "127.0.0.1",
            "request_uri": "/mcp?foo=bar",
            "status": 502,
            "body_bytes_sent": 157,
            "request_length": 212,
            "request_time": 0.002,
            "upstream_status": "502, 502",
            "upstream_response_time": "0.003, 0.000",
            "user_agent": "test-agent/1.0",
            "referer": "",
        }
    )
    (tmp_path / "access-2026-09-14.log").write_text(line + "\n")
    r = extract_mcp_api_partition(str(tmp_path), date(2026, 9, 14), SALT, NO_GEO)
    assert len(r.rows) == 1
    row = r.rows[0]
    assert row["host_source"] == "mcp-api"
    assert row["log_format"] == "mcp_api_json"
    assert row["client_address"] == "203.0.113.117"
    assert row["client_hash"]
    assert row["path"] == "/mcp"
    assert row["query_string"] == "foo=bar"
    assert row["upstream_status"] == "502, 502"
    assert row["status"] == 502
    assert row["referer"] is None
    assert reconciles(r)


def test_mcp_api_rejects_are_recorded_like_any_other_source(tmp_path):
    (tmp_path / "access-2026-09-14.log").write_text(_mcp_api_line() + "not json\n")
    r = extract_mcp_api_partition(str(tmp_path), date(2026, 9, 14), SALT, NO_GEO)
    assert len(r.rows) == 1
    assert len(r.rejects) == 1
    assert r.rejects[0]["host"] == "mcp-api"


# --- the reconciliation check ----------------------------------------------------


def test_a_real_extraction_always_reconciles(tmp_path):
    _write(tmp_path, "proxy-host-1_access.log", [
        _proxy_line("27/Aug/2026:10:00:00 +0000", path="/a"),
        _proxy_line("26/Aug/2026:10:00:00 +0000", path="/out-of-partition"),
        "garbage one\n",
    ])
    _write(tmp_path, "proxy-host-1_access.log.1.gz", ["garbage one\n"], gzipped=True)
    r = extract_partition(str(tmp_path), date(2026, 8, 27), "test", SALT, NO_GEO)
    assert len(r.rows) == 1 and r.out_of_partition == 1 and r.duplicate_rejects == 1
    assert reconciles(r)


def test_reconciliation_fails_when_a_line_goes_unaccounted_for():
    """The failure this check exists to catch: pinned with a result built by hand
    rather than through real extraction, since correct code never produces one."""
    from homelab_datalake.parsing.extract import ExtractResult
    from homelab_datalake.parsing.npm_log import ParseStats

    stats = ParseStats(total=10, parsed=10, rejected=0)
    result = ExtractResult(rows=[{}] * 5, rejects=[], stats=stats, out_of_partition=2)
    # [count withheld] rows + 0 rejects + 0 duplicates + 2 out_of_partition = 7, not the 10 read.
    assert not reconciles(result)


def test_reconciliation_accounts_for_duplicate_rejects():
    from homelab_datalake.parsing.extract import ExtractResult
    from homelab_datalake.parsing.npm_log import ParseStats

    stats = ParseStats(total=3, parsed=0, rejected=3)
    result = ExtractResult(rows=[], rejects=[{}], stats=stats, duplicate_rejects=2)
    assert reconciles(result)
