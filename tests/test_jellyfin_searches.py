"""The AI Search plugin's search log: `/AiSearch/SearchLog`. Payloads are injected,
so no server is needed -- the fixture below matches the shape documented for the
endpoint on plugin AiSearch/1.2.0.0.
"""

from datetime import UTC, datetime

import pytest
from dagster import AssetCheckResult, Output, build_asset_context

from homelab_datalake.assets import jellyfin as jellyfin_assets
from homelab_datalake.assets.jellyfin import (
    _SEARCH_SCHEMA,
    SEARCH_RESULTS_PREFIX,
    SEARCHES_PREFIX,
    _merge_keyed,
    _search_entry_key,
    _search_result_key,
    jellyfin_searches_raw,
)
from homelab_datalake.parsing.jellyfin import (
    SearchLogUnavailable,
    _search_entry_leftovers,
    _search_entry_row,
    _search_result_rows,
    fetch_search_log_page,
    search_log_window,
)
from homelab_datalake.resources import LakeStorage

# A test salt only -- never the real CLIENT_IP_HASH_SALT.
SALT = "ci-salt-not-a-secret"


def _entry(**overrides):
    """One /AiSearch/SearchLog entry, matching the endpoint's documented shape.
    192.0.2.0/24 is RFC 5737 documentation space, never a real address.
    """
    base = {
        "id": 639000000000000123,
        "timestamp": "2026-09-19T10:11:12.345Z",
        "kind": "search",
        "endpoint": "/Items",
        "method": "GET",
        "statusCode": 200,
        "durationMs": 184,
        "searchTerm": "heist movie with a twist",
        "userId": "4f1c2b3a4b3a4b3a4b3a4b3a4b3a4b3a",
        "userName": "francois",
        "isApiKey": False,
        "client": "Jellyfin Web",
        "clientVersion": "12.1.0",
        "device": "Firefox",
        "deviceId": "VGVzdERldmljZQ==",
        "remoteAddress": "192.0.2.10",
        "userAgent": "Mozilla/5.0 (Test)",
        "parameters": {
            "searchTerm": "heist movie with a twist",
            "includeItemTypes": "Movie,Series,Episode",
            "limit": "800",
            "recursive": "true",
        },
        "parentId": None,
        "parentName": None,
        "resultCount": 23,
        "totalRecordCount": 23,
        "resultsLogged": 1,
        "results": [
            {
                "position": 1, "itemId": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4",
                "name": "Inside Man", "type": "Movie", "year": 2006,
                "seriesName": None, "seasonNumber": None, "episodeNumber": None,
                "semantic": True, "semanticScore": 0.61, "semanticRank": 1,
                "reason": None,
            },
        ],
        "semantic": {
            "enabled": True, "ran": True, "skippedReason": None, "mode": "remote",
            "durationMs": 140, "rawHits": 80, "matches": 20, "error": None,
        },
        "mode": None,
        "answer": None,
    }
    base.update(overrides)
    return base


def _page(entries=None, **envelope):
    defaults = {
        "entries": entries if entries is not None else [_entry()],
        "hasMore": False,
        "nextAfterId": None,
        "loggingEnabled": True,
        "retentionDays": 7,
        "oldestTimestamp": "2026-09-19T08:00:01.123Z",
    }
    defaults.update(envelope)
    return defaults


# --------------------------------------------------------------------------------
# row builders: pure functions, no server, no wall clock
# --------------------------------------------------------------------------------


def test_the_entry_row_reads_every_documented_field():
    row = _search_entry_row(_entry(), SALT)
    assert row["id"] == 639000000000000123
    assert row["searched_at"] == datetime(2026, 9, 19, 10, 11, 12, 345000, tzinfo=UTC)
    assert row["kind"] == "search"
    assert row["endpoint"] == "/Items"
    assert row["status_code"] == 200
    assert row["duration_ms"] == 184
    assert row["search_term"] == "heist movie with a twist"
    assert row["user_id"] == "4f1c2b3a4b3a4b3a4b3a4b3a4b3a4b3a"
    assert row["user_name"] == "francois"
    assert row["is_api_key"] is False
    assert row["client"] == "Jellyfin Web"
    assert row["client_version"] == "12.1.0"
    assert row["device"] == "Firefox"
    assert row["device_id"] == "VGVzdERldmljZQ=="
    assert row["result_count"] == 23
    assert row["total_record_count"] == 23
    assert row["results_logged"] == 1
    assert row["semantic_enabled"] is True
    assert row["semantic_ran"] is True
    assert row["semantic_mode"] == "remote"
    assert row["semantic_duration_ms"] == 140
    assert row["semantic_raw_hits"] == 80
    assert row["semantic_matches"] == 20
    assert row["mode"] is None
    assert row["answer"] is None


def test_the_entry_row_serialises_parameters_as_json():
    row = _search_entry_row(_entry(), SALT)
    assert '"searchTerm"' in row["parameters"]
    assert '"heist movie with a twist"' in row["parameters"]


def test_empty_parameters_are_null_not_an_empty_object():
    row = _search_entry_row(_entry(parameters={}), SALT)
    assert row["parameters"] is None


def test_the_remote_address_is_kept_raw_alongside_its_redaction():
    row = _search_entry_row(_entry(), SALT)
    assert row["remote_address"] == "192.0.2.10"
    assert row["client_hash"] is not None
    assert row["client_network"] == "192.0.2.0/24"
    assert row["client_trust"] == "resolved"


def test_a_null_remote_address_needs_no_salt_at_all():
    """redact_client returns early for a falsy address, before it ever looks at the
    salt -- so a fixture with no remoteAddress must not need one."""
    row = _search_entry_row(_entry(remoteAddress=None), "")
    assert row["remote_address"] is None
    assert row["client_hash"] is None
    assert row["client_trust"] == "unknown"


def test_an_api_key_search_carries_no_user():
    row = _search_entry_row(_entry(userId=None, userName=None, isApiKey=True), SALT)
    assert row["user_id"] is None
    assert row["user_name"] is None
    assert row["is_api_key"] is True


def test_an_ai_search_carries_mode_and_answer():
    row = _search_entry_row(
        _entry(kind="ai", endpoint="/AiSearch/Ask", mode="surprise",
              answer="A few picks that match the mood."),
        SALT,
    )
    assert row["kind"] == "ai"
    assert row["mode"] == "surprise"
    assert row["answer"] == "A few picks that match the mood."


def test_semantic_block_is_flattened_with_defaults_when_absent():
    row = _search_entry_row(_entry(semantic=None), SALT)
    assert row["semantic_enabled"] is None
    assert row["semantic_ran"] is None
    assert row["semantic_error"] is None


def test_result_rows_carry_the_parents_id_and_timestamp():
    rows = _search_result_rows(_entry())
    assert len(rows) == 1
    assert rows[0]["search_id"] == 639000000000000123
    assert rows[0]["searched_at"] == datetime(2026, 9, 19, 10, 11, 12, 345000, tzinfo=UTC)
    assert rows[0]["position"] == 1
    assert rows[0]["item_id"] == "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4"
    assert rows[0]["name"] == "Inside Man"
    assert rows[0]["type"] == "Movie"
    assert rows[0]["year"] == 2006
    assert rows[0]["semantic"] is True
    assert rows[0]["semantic_score"] == 0.61
    assert rows[0]["semantic_rank"] == 1


def test_a_search_with_no_results_produces_no_result_rows():
    assert _search_result_rows(_entry(results=[], resultsLogged=0)) == []


def test_an_episode_result_carries_series_and_episode_fields():
    entry = _entry(results=[
        {"position": 1, "itemId": "i1", "name": "Winter Is Coming", "type": "Episode",
         "year": 2011, "seriesName": "Game of Thrones", "seasonNumber": 1,
         "episodeNumber": 1, "semantic": False, "semanticScore": None,
         "semanticRank": None, "reason": None},
    ])
    row = _search_result_rows(entry)[0]
    assert row["series_name"] == "Game of Thrones"
    assert row["season_number"] == 1
    assert row["episode_number"] == 1
    assert row["semantic"] is False


# --------------------------------------------------------------------------------
# unmapped fields: on the entry, and dotted into its nested semantic and results
# --------------------------------------------------------------------------------


def test_a_clean_entry_reports_nothing_unmapped():
    assert _search_entry_leftovers(_entry()) == {}


def test_a_new_top_level_field_is_reported():
    leftovers = _search_entry_leftovers(_entry(newField="x"))
    assert leftovers == {"newField": "x"}


def test_a_new_field_inside_semantic_is_reported_with_a_dotted_path():
    entry = _entry(semantic={
        "enabled": True, "ran": True, "skippedReason": None, "mode": "remote",
        "durationMs": 140, "rawHits": 80, "matches": 20, "error": None,
        "providerVersion": "2.0",
    })
    leftovers = _search_entry_leftovers(entry)
    assert leftovers == {"semantic.providerVersion": "2.0"}


def test_a_new_field_inside_a_result_is_reported_with_a_bracketed_dotted_path():
    entry = _entry(results=[
        {"position": 1, "itemId": "i1", "name": "X", "type": "Movie", "year": 2020,
         "seriesName": None, "seasonNumber": None, "episodeNumber": None,
         "semantic": False, "semanticScore": None, "semanticRank": None,
         "reason": None, "confidence": 0.9},
    ])
    leftovers = _search_entry_leftovers(entry)
    assert leftovers == {"results[].confidence": 0.9}


# --------------------------------------------------------------------------------
# the endpoint: paging on afterId, and the 404 -> missing-plugin translation
# --------------------------------------------------------------------------------


def test_a_404_raises_search_log_unavailable_not_a_generic_error():
    import urllib.error
    import urllib.request as _urllib_request

    def fake_urlopen(request, timeout=60):
        raise urllib.error.HTTPError(request.full_url, 404, "Not Found", {}, None)

    original = _urllib_request.urlopen
    _urllib_request.urlopen = fake_urlopen
    try:
        with pytest.raises(SearchLogUnavailable, match="AiSearch"):
            fetch_search_log_page(
                "https://jf.example", "key",
                datetime(2026, 9, 19, 10, tzinfo=UTC), datetime(2026, 9, 19, 11, tzinfo=UTC),
                0,
            )
    finally:
        _urllib_request.urlopen = original


def test_search_log_window_pages_while_hasmore_is_true():
    pages = [
        _page([_entry(id=1)], hasMore=True, nextAfterId=1),
        _page([_entry(id=2)], hasMore=False, nextAfterId=2),
    ]
    out = search_log_window(
        datetime(2026, 9, 19, 10, tzinfo=UTC), datetime(2026, 9, 19, 11, tzinfo=UTC),
        salt=SALT, pages=pages,
    )
    assert [row["id"] for row in out.entries] == [1, 2]
    assert out.pages == 2


def test_search_log_window_reads_the_envelope_from_the_first_page_only():
    pages = [
        _page([_entry(id=1)], loggingEnabled=True, retentionDays=7,
              oldestTimestamp="2026-09-19T08:00:01.123Z"),
    ]
    out = search_log_window(
        datetime(2026, 9, 19, 10, tzinfo=UTC), datetime(2026, 9, 19, 11, tzinfo=UTC),
        salt=SALT, pages=pages,
    )
    assert out.logging_enabled is True
    assert out.retention_days == 7
    assert out.oldest_timestamp == datetime(2026, 9, 19, 8, 0, 1, 123000, tzinfo=UTC)


def test_logging_disabled_on_the_server_is_reported_not_treated_as_empty():
    pages = [_page([], loggingEnabled=False)]
    out = search_log_window(
        datetime(2026, 9, 19, 10, tzinfo=UTC), datetime(2026, 9, 19, 11, tzinfo=UTC),
        salt=SALT, pages=pages,
    )
    assert out.logging_enabled is False
    assert out.entries == []


def test_no_pages_at_all_leaves_logging_enabled_unknown():
    """The injectable pages=[] case: nothing was ever read, so the check that only
    passes when logging_enabled is True correctly cannot confirm it either."""
    out = search_log_window(
        datetime(2026, 9, 19, 10, tzinfo=UTC), datetime(2026, 9, 19, 11, tzinfo=UTC),
        salt=SALT, pages=[],
    )
    assert out.logging_enabled is None


def test_search_log_window_collects_results_across_every_page():
    pages = [_page([_entry(id=1)]), _page([_entry(id=2)])]
    out = search_log_window(
        datetime(2026, 9, 19, 10, tzinfo=UTC), datetime(2026, 9, 19, 11, tzinfo=UTC),
        salt=SALT, pages=pages,
    )
    assert len(out.results) == 2
    assert {r["search_id"] for r in out.results} == {1, 2}


def test_search_log_window_with_no_pages_and_no_url_refuses_rather_than_hang():
    with pytest.raises(ValueError, match="JELLYFIN_URL"):
        search_log_window(datetime(2026, 9, 19, 10, tzinfo=UTC), datetime(2026, 9, 19, 11, tzinfo=UTC))


# --------------------------------------------------------------------------------
# the merge: entries keyed by id, results keyed by (search_id, position)
# --------------------------------------------------------------------------------


def _entry_row(**overrides):
    base = {
        "partition_hour": datetime(2026, 9, 19, 10, tzinfo=UTC),
        "dt": datetime(2026, 9, 19, 10, 0, 0, tzinfo=UTC).date(),
        "_extracted_at": datetime(2020, 1, 1, tzinfo=UTC),
        "id": 1, "searched_at": datetime(2026, 9, 19, 10, 0, 1, tzinfo=UTC),
        "kind": "search", "endpoint": "/Items", "method": "GET", "status_code": 200,
        "duration_ms": 100, "search_term": "x", "user_id": "u1", "user_name": "greg",
        "is_api_key": False, "client": "Jellyfin Web", "client_version": "12.1.0",
        "device": "Firefox", "device_id": "d1", "remote_address": None,
        "client_hash": None, "client_network": None, "client_trust": "unknown",
        "user_agent": "ua", "parameters": None, "parent_id": None, "parent_name": None,
        "result_count": 1, "total_record_count": 1, "results_logged": 1,
        "semantic_enabled": None, "semantic_ran": None, "semantic_skipped_reason": None,
        "semantic_mode": None, "semantic_duration_ms": None, "semantic_raw_hits": None,
        "semantic_matches": None, "semantic_error": None, "mode": None, "answer": None,
    }
    base.update(overrides)
    return base


def test_two_entries_with_different_ids_both_survive():
    merged, retained = _merge_keyed([], [_entry_row(id=1), _entry_row(id=2)],
                                    _SEARCH_SCHEMA, _search_entry_key)
    assert len(merged) == 2 and retained == 0


def test_a_later_read_of_the_same_entry_overwrites_the_earlier_one():
    early = [_entry_row(duration_ms=100, _extracted_at=datetime(2026, 9, 19, 10, 5, tzinfo=UTC))]
    late = [_entry_row(duration_ms=140, _extracted_at=datetime(2026, 9, 19, 10, 20, tzinfo=UTC))]
    merged, retained = _merge_keyed(early, late, _SEARCH_SCHEMA, _search_entry_key)
    assert len(merged) == 1 and retained == 0
    assert merged[0]["duration_ms"] == 140


def test_an_entry_the_plugin_no_longer_offers_is_retained_not_dropped():
    in_lake = [_entry_row(id=1), _entry_row(id=2)]
    still_offered = [_entry_row(id=1)]
    merged, retained = _merge_keyed(in_lake, still_offered, _SEARCH_SCHEMA, _search_entry_key)
    assert len(merged) == 2
    assert retained == 1


def test_result_key_is_search_id_and_position():
    row = {"search_id": 5, "position": 2}
    assert _search_result_key(row) == (5, 2)


def test_result_key_is_none_without_both_parts():
    assert _search_result_key({"search_id": 5, "position": None}) is None
    assert _search_result_key({"search_id": None, "position": 1}) is None


# --------------------------------------------------------------------------------
# the asset: driven end to end against a real (temp) lake
# --------------------------------------------------------------------------------


def _run(lake, pages, monkeypatch, partition_key="2026-09-19-10:00"):
    monkeypatch.setenv("JELLYFIN_URL", "https://jf.example")
    monkeypatch.setenv("JELLYFIN_API_KEY", "key")
    monkeypatch.setenv("CLIENT_IP_HASH_SALT", SALT)
    real_window = jellyfin_assets.jellyfin_source.search_log_window
    monkeypatch.setattr(
        jellyfin_assets.jellyfin_source, "search_log_window",
        lambda start, end, *a, **k: real_window(start, end, salt=SALT, pages=pages),
    )
    context = build_asset_context(partition_key=partition_key)
    results = list(jellyfin_searches_raw(context, lake))
    output = next(r for r in results if isinstance(r, Output))
    checks = {r.check_name: r for r in results if isinstance(r, AssetCheckResult)}
    return output, checks


def _meta(result, key):
    return result.metadata[key].value


def test_the_asset_requires_a_salt(tmp_path):
    lake = LakeStorage(local_root=str(tmp_path / "lake"))
    import os

    os.environ.pop("CLIENT_IP_HASH_SALT", None)
    os.environ["JELLYFIN_URL"] = "https://jf.example"
    os.environ["JELLYFIN_API_KEY"] = "key"
    try:
        with pytest.raises(ValueError, match="CLIENT_IP_HASH_SALT"):
            list(jellyfin_searches_raw(build_asset_context(partition_key="2026-09-19-10:00"), lake))
    finally:
        os.environ.pop("JELLYFIN_URL", None)
        os.environ.pop("JELLYFIN_API_KEY", None)


def test_a_clean_read_writes_entries_and_results_and_passes_every_check(tmp_path, monkeypatch):
    lake = LakeStorage(local_root=str(tmp_path / "lake"))
    output, checks = _run(lake, [_page([_entry()])], monkeypatch)

    assert _meta(output, "searches") == 1
    assert _meta(output, "results") == 1
    assert _meta(output, "logging_enabled") is True
    assert checks["searches_match_the_plugins_own_count"].passed is True
    assert checks["search_logging_enabled_on_the_server"].passed is True
    assert checks["no_unmapped_fields"].passed is True

    hour_prefix = f"{SEARCHES_PREFIX}/partition_hour=2026-09-19T10:00:00"
    written = lake.read_table(f"{hour_prefix}/data.parquet").to_pylist()
    assert written[0]["id"] == 639000000000000123
    assert written[0]["client_network"] == "192.0.2.0/24"

    results_prefix = f"{SEARCH_RESULTS_PREFIX}/partition_hour=2026-09-19T10:00:00"
    results = lake.read_table(f"{results_prefix}/data.parquet").to_pylist()
    assert results[0]["search_id"] == 639000000000000123
    assert results[0]["name"] == "Inside Man"


def test_a_field_the_schema_does_not_name_fails_only_the_unmapped_check(tmp_path, monkeypatch):
    lake = LakeStorage(local_root=str(tmp_path / "lake"))
    output, checks = _run(lake, [_page([_entry(newField="x")])], monkeypatch)

    assert checks["no_unmapped_fields"].passed is False
    assert checks["searches_match_the_plugins_own_count"].passed is True
    assert "newField" in _meta(output, "unmapped_fields")


def test_logging_disabled_fails_only_that_one_check(tmp_path, monkeypatch):
    lake = LakeStorage(local_root=str(tmp_path / "lake"))
    output, checks = _run(lake, [_page([_entry()], loggingEnabled=False)], monkeypatch)

    assert checks["search_logging_enabled_on_the_server"].passed is False
    assert checks["searches_match_the_plugins_own_count"].passed is True
    assert _meta(output, "logging_enabled") is False


def _seed_entries_hour(lake, hour, rows, extracted_at):
    """What a previous run of jellyfin_searches_raw left behind for one hour --
    written directly, with an explicit (deliberately old) _extracted_at, so a test
    exercising the merge does not depend on two real datetime.now(UTC) calls a few
    microseconds apart actually differing."""
    import pyarrow as pa

    from homelab_datalake.projection import project

    stamped = [{**row, "partition_hour": hour, "_extracted_at": extracted_at} for row in rows]
    projected, _ = project(stamped, _SEARCH_SCHEMA)
    prefix = f"{SEARCHES_PREFIX}/partition_hour={hour:%Y-%m-%dT%H:00:00}"
    lake.write_table(f"{prefix}/data.parquet", pa.Table.from_pylist(projected, schema=_SEARCH_SCHEMA))


def test_re_reading_the_same_hour_merges_rather_than_duplicates(tmp_path, monkeypatch):
    lake = LakeStorage(local_root=str(tmp_path / "lake"))
    hour = datetime(2026, 9, 19, 10, tzinfo=UTC)
    _seed_entries_hour(
        lake, hour, [_entry_row(id=639000000000000123, duration_ms=100)],
        datetime(2020, 1, 1, tzinfo=UTC),
    )

    _output, checks = _run(lake, [_page([_entry(durationMs=250)])], monkeypatch)

    hour_prefix = f"{SEARCHES_PREFIX}/partition_hour=2026-09-19T10:00:00"
    written = lake.read_table(f"{hour_prefix}/data.parquet").to_pylist()
    assert len(written) == 1
    assert written[0]["duration_ms"] == 250
    assert checks["searches_match_the_plugins_own_count"].passed is True


def test_an_entry_the_second_read_no_longer_offers_is_kept_and_the_check_still_passes(
    tmp_path, monkeypatch
):
    """The subset check, not equality: a merge can legitimately retain an entry this
    run's own query no longer offers, the same reasoning jellyfin_playback_raw
    documents for a session the plugin's table no longer returns."""
    lake = LakeStorage(local_root=str(tmp_path / "lake"))
    hour = datetime(2026, 9, 19, 10, tzinfo=UTC)
    _seed_entries_hour(
        lake, hour,
        [_entry_row(id=639000000000000123), _entry_row(id=639000000000000124)],
        datetime(2020, 1, 1, tzinfo=UTC),
    )

    output, checks = _run(lake, [_page([_entry(id=639000000000000123)])], monkeypatch)

    hour_prefix = f"{SEARCHES_PREFIX}/partition_hour=2026-09-19T10:00:00"
    written = lake.read_table(f"{hour_prefix}/data.parquet").to_pylist()
    assert sorted(r["id"] for r in written) == [639000000000000123, 639000000000000124]
    assert _meta(output, "searches_only_in_lake") == 1
    assert checks["searches_match_the_plugins_own_count"].passed is True


def test_the_reconciliation_check_fails_when_a_result_row_is_dropped(tmp_path, monkeypatch):
    """The check has to be more than decoration: with the write deliberately
    truncated to one row short, results_logged and the actual row count must
    disagree, and the check must catch it."""
    lake = LakeStorage(local_root=str(tmp_path / "lake"))
    entry = _entry(results=[
        {"position": 1, "itemId": "i1", "name": "A", "type": "Movie", "year": 2020,
         "seriesName": None, "seasonNumber": None, "episodeNumber": None,
         "semantic": False, "semanticScore": None, "semanticRank": None, "reason": None},
        {"position": 2, "itemId": "i2", "name": "B", "type": "Movie", "year": 2021,
         "seriesName": None, "seasonNumber": None, "episodeNumber": None,
         "semantic": False, "semanticScore": None, "semanticRank": None, "reason": None},
    ], resultsLogged=2)

    real_merge = jellyfin_assets._merge_keyed

    def drop_one_result_row(existing, incoming, schema, key_fn):
        if schema is jellyfin_assets._SEARCH_RESULT_SCHEMA and incoming:
            incoming = incoming[:-1]
        return real_merge(existing, incoming, schema, key_fn)

    monkeypatch.setattr(jellyfin_assets, "_merge_keyed", drop_one_result_row)

    _output, checks = _run(lake, [_page([entry])], monkeypatch)

    assert checks["searches_match_the_plugins_own_count"].passed is False
