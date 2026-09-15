"""Playback, activity log and account list extraction. Payloads are injected, so no
server is needed -- the two facts that matter (DateCreated is UTC, PlayDuration is
seconds) were verified against the real server on 2026-09-14, not assumed; see
`_parse_playback_timestamp` and `_parse_duration_seconds` in `parsing/jellyfin.py`
for the worked evidence these tests encode as fixtures.
"""

from datetime import UTC, date, datetime

import pyarrow as pa
import pytest
from dagster import AssetCheckResult, Output, build_asset_context

from homelab_datalake.assets import jellyfin as jellyfin_assets
from homelab_datalake.assets.jellyfin import (
    _PLAYBACK_SCHEMA,
    ACTIVITY_PREFIX,
    PLAYBACK_PREFIX,
    _merge_playback,
    _playback_key,
    _write_activity_history_day,
    jellyfin_playback_history_raw,
)
from homelab_datalake.parsing.jellyfin import (
    _PLAYBACK_COLUMNS,
    _parse_duration_seconds,
    _parse_playback_timestamp,
    _playback_history_query,
    _playback_query,
    _sqlite_literal,
    activity_history,
    activity_window,
    parse_playback_rows,
    parse_series_name,
    playback_history,
    playback_window,
    user_rows,
    users_by_id,
)
from homelab_datalake.projection import project
from homelab_datalake.resources import LakeStorage

# --------------------------------------------------------------------------------
# timestamp: DateCreated is UTC on this server, verified against the standard API's
# activity log, which does carry a `Z` -- not assumed, and not the local-server-time
# some Playback Reporting installs are documented to use.
# --------------------------------------------------------------------------------


def test_a_real_session_is_parsed_as_utc_not_local_time():
    """viewer's Example Movie session: activity log 18:00:01Z..19:52:43Z, PlaybackActivity
    DateCreated 2026-09-12 18:00:21.6180609. Twenty seconds after the activity log's
    own start, in the *same* hour -- not the two hours away a Europe/Paris reading
    would put it in September (CEST, UTC+2). If this function defaulted a naive
    string to local time it would return 16:00, not 18:00.
    """
    parsed = _parse_playback_timestamp("2026-09-12 18:00:21.6180609")
    assert parsed == datetime(2026, 9, 12, 18, 0, 21, 618060, tzinfo=UTC)


def test_seven_digit_fractional_seconds_are_truncated_not_rejected():
    parsed = _parse_playback_timestamp("2026-09-12 20:35:46.7224376")
    assert parsed is not None
    assert parsed.microsecond == 722437


def test_no_value_is_null_not_an_error():
    assert _parse_playback_timestamp(None) is None
    assert _parse_playback_timestamp("") is None


def test_the_paris_spring_forward_gap_is_unambiguous_as_utc():
    """2026-03-29 02:30 does not exist as a Europe/Paris wall-clock time -- clocks
    jump from 02:00 CET straight to 03:00 CEST that night. A fixture at exactly that
    value is only meaningful under the UTC reading this function actually uses; any
    code that tried to localise it to Europe/Paris first would have nothing valid to
    convert.
    """
    parsed = _parse_playback_timestamp("2026-03-29 02:30:00.0000000")
    assert parsed == datetime(2026, 3, 29, 2, 30, tzinfo=UTC)


def test_the_paris_autumn_ambiguous_hour_is_unambiguous_as_utc():
    """2026-10-25 02:30 occurs *twice* as a Europe/Paris wall-clock time that night --
    once as CEST (UTC+2, so 00:30 UTC) and once as CET (UTC+1, so 01:30 UTC). Reading
    it as UTC directly sidesteps the ambiguity entirely and lands on neither of those
    two candidate instants.
    """
    parsed = _parse_playback_timestamp("2026-10-25 02:30:00.0000000")
    assert parsed == datetime(2026, 10, 25, 2, 30, tzinfo=UTC)


# --------------------------------------------------------------------------------
# duration: PlayDuration is whole seconds, verified against the same Example Movie session
# -- an elapsed [duration withheld] by the activity log's own start and stop produced
# PlayDuration=6761.
# --------------------------------------------------------------------------------


def test_play_duration_is_seconds_not_an_hms_string():
    assert _parse_duration_seconds("6761") == 6761
    assert _parse_duration_seconds(53) == 53


def test_play_duration_rounds_a_fractional_value():
    assert _parse_duration_seconds("52.7") == 53


def test_play_duration_of_nothing_is_null_not_zero():
    assert _parse_duration_seconds(None) is None


def test_play_duration_garbage_is_null_not_a_crash():
    assert _parse_duration_seconds("not a number") is None


# --------------------------------------------------------------------------------
# series: parsed from item_name's own "<series> - sNNeNN - <title>" convention,
# checked against five real shows on this server before being written.
# --------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("item_name", "expected"),
    [
        ("A Knight of the Seven Kingdoms - s01e01 - Le chevalier errant",
         "A Knight of the Seven Kingdoms"),
        ("1883 - s01e01 - 1883", "1883"),
        ("1899 - s01e06 - La pyramide", "1899"),
        ("24 Heures chrono - s01e24 - 23h00 - Minuit", "24 Heures chrono"),
        ("American Horror Story - s02e02 - Exorcisme", "American Horror Story"),
    ],
)
def test_series_name_matches_real_episode_titles(item_name, expected):
    assert parse_series_name(item_name, "Episode") == expected


def test_series_name_is_null_for_a_movie():
    """Example Movie is a plain title with no season/episode marker at all."""
    assert parse_series_name("Example Movie", "Movie") is None


def test_series_name_is_null_when_an_episode_title_does_not_match_the_convention():
    assert parse_series_name("Some Odd Title With No Marker", "Episode") is None


# --------------------------------------------------------------------------------
# the plugin's response shape -- `colums`, not a typo here either
# --------------------------------------------------------------------------------


def _payload(rows, message="", columns=None):
    return {"colums": columns if columns is not None else list(_PLAYBACK_COLUMNS),
            "results": rows, "message": message}


def test_the_misspelled_colums_key_is_read():
    row = ["2026-09-12 18:00:21.6180609", "u1", "item1", "Movie", "Example Movie",
           "Transcode (v:hevc a:aac)", "Jellyfin Web", "Chrome Android", "6761"]
    rows = parse_playback_rows(_payload([row]), {"u1": "viewer"})
    assert len(rows) == 1
    assert rows[0]["user_name"] == "viewer"
    assert rows[0]["play_duration_seconds"] == 6761
    assert rows[0]["item_name"] == "Example Movie"


def test_a_legitimately_empty_window_is_not_an_error():
    payload = _payload([], message="Query executed, no data returned", columns=[])
    assert parse_playback_rows(payload, {}) == []


def test_a_real_query_failure_raises():
    payload = _payload([], message="near \"SELECT\": syntax error", columns=[])
    with pytest.raises(ValueError, match="query failed"):
        parse_playback_rows(payload, {})


def test_a_reordered_or_renamed_column_set_raises_rather_than_silently_misreading():
    """Reading by position out of an unchecked column list would swap fields
    instead of failing the day a plugin update reorders them."""
    payload = _payload([["x"]], columns=["ItemId", "DateCreated"])
    with pytest.raises(ValueError, match="expected"):
        parse_playback_rows(payload, {})


def test_an_unresolvable_user_id_leaves_the_name_null_rather_than_failing():
    row = ["2026-09-12 18:00:21.6180609", "deleted-account", "item1", "Movie",
           "Example Movie", "DirectPlay", "Jellyfin Web", "Chrome", "10"]
    rows = parse_playback_rows(_payload([row]), {})
    assert rows[0]["user_id"] == "deleted-account"
    assert rows[0]["user_name"] is None


# --------------------------------------------------------------------------------
# SQL safety: only a datetime this module computed itself can reach the query
# --------------------------------------------------------------------------------


def test_sqlite_literal_refuses_anything_but_a_datetime():
    with pytest.raises(TypeError):
        _sqlite_literal("2026-09-12 18:00:00")  # type: ignore[arg-type]


def test_sqlite_literal_formats_in_utc():
    assert _sqlite_literal(datetime(2026, 9, 12, 18, 0, tzinfo=UTC)) == "2026-09-12 18:00:00"


# --------------------------------------------------------------------------------
# the merge: PlaybackActivity has no id, so (started_at, user_id, item_id) is the
# natural key -- and a session's own row can change between two reads, because the
# plugin does not expose whether a session is still open.
# --------------------------------------------------------------------------------


def _row(**overrides):
    base = {
        "partition_hour": datetime(2026, 9, 12, 18, tzinfo=UTC),
        "dt": date(2026, 9, 12),
        "_extracted_at": datetime(2026, 9, 12, 18, 5, tzinfo=UTC),
        "started_at": datetime(2026, 9, 12, 18, 0, 1, tzinfo=UTC),
        "user_id": "u1", "user_name": "viewer", "item_id": "item1", "item_type": "Movie",
        "item_name": "Example Movie", "series_name": None, "playback_method": "DirectPlay",
        "client_name": "Jellyfin Web", "device_name": "Chrome", "play_duration_seconds": 53,
    }
    base.update(overrides)
    return base


def test_two_sessions_with_different_keys_both_survive():
    merged, retained = _merge_playback(
        [], [_row(item_id="item1"), _row(item_id="item2")], _PLAYBACK_SCHEMA
    )
    assert len(merged) == 2 and retained == 0


def test_a_later_read_of_the_same_session_overwrites_the_earlier_one():
    """The exact case the plugin creates: a film still playing shows a small
    PlayDuration on an early read and the final one once it ends."""
    early = [_row(play_duration_seconds=53,
                  _extracted_at=datetime(2026, 9, 12, 18, 5, tzinfo=UTC))]
    late = [_row(play_duration_seconds=6761,
                 _extracted_at=datetime(2026, 9, 12, 20, 0, tzinfo=UTC))]

    merged, retained = _merge_playback(early, late, _PLAYBACK_SCHEMA)

    assert len(merged) == 1 and retained == 0
    assert merged[0]["play_duration_seconds"] == 6761


def test_an_older_extraction_does_not_overwrite_a_newer_one():
    """Order of arguments must not matter -- only the timestamp does."""
    early = [_row(play_duration_seconds=53,
                  _extracted_at=datetime(2026, 9, 12, 18, 5, tzinfo=UTC))]
    late = [_row(play_duration_seconds=6761,
                 _extracted_at=datetime(2026, 9, 12, 20, 0, tzinfo=UTC))]

    merged, _ = _merge_playback(late, early, _PLAYBACK_SCHEMA)
    assert merged[0]["play_duration_seconds"] == 6761


def test_a_row_the_plugin_no_longer_offers_is_retained_not_dropped():
    """This is the case that would fail CHECK_PLAYBACK_RECONCILED: the merged count
    no longer equals what the plugin just offered, because the lake is carrying a row
    the source query did not return this time.
    """
    in_lake = [_row(item_id="item1"), _row(item_id="item2")]
    still_offered = [_row(item_id="item1")]

    merged, retained = _merge_playback(in_lake, still_offered, _PLAYBACK_SCHEMA)

    assert len(merged) == 2
    assert retained == 1
    # The reconciliation check's own arithmetic: passed = len(merged) == source_count.
    assert len(merged) != len(still_offered)


def test_the_reconciliation_check_passes_on_a_clean_first_read():
    offered = [_row(item_id="item1"), _row(item_id="item2")]
    merged, retained = _merge_playback([], offered, _PLAYBACK_SCHEMA)
    assert len(merged) == len(offered)
    assert retained == 0


def test_a_row_missing_started_at_is_skipped_by_the_merge_key():
    merged, _ = _merge_playback([], [_row(started_at=None)], _PLAYBACK_SCHEMA)
    assert merged == []


def test_playback_key_ignores_client_and_device():
    """The key is (started_at, user_id, item_id) only -- a device switch mid-session
    must not be read as two different sessions."""
    a = _playback_key(_row(client_name="Jellyfin Web"))
    b = _playback_key(_row(client_name="Android TV"))
    assert a == b


# --------------------------------------------------------------------------------
# the account list: name and two flags, nothing from configuration or policy
# --------------------------------------------------------------------------------


def _user(**overrides):
    base = {
        "Id": "u1", "Name": "guest",
        "Policy": {"IsAdministrator": False, "IsDisabled": False,
                   "InvalidLoginAttemptCount": 0},
        "LastLoginDate": "2026-07-30T19:20:48.3860424Z",
        "LastActivityDate": "2026-09-14T19:15:03.7115018Z",
        "HasPassword": True, "HasConfiguredPassword": True,
        "HasConfiguredEasyPassword": False, "EnableAutoLogin": False,
        "ServerId": "s1", "Configuration": {"SomePreference": True},
    }
    base.update(overrides)
    return base


def test_user_rows_keeps_only_name_and_two_flags():
    rows, _unmapped = user_rows([_user()], date(2026, 9, 14))
    assert rows[0] == {
        "dt": date(2026, 9, 14), "id": "u1", "name": "guest",
        "is_administrator": False, "is_disabled": False,
        "last_login": datetime(2026, 7, 30, 19, 20, 48, 386042, tzinfo=UTC),
        "last_activity": datetime(2026, 9, 14, 19, 15, 3, 711501, tzinfo=UTC),
    }


def test_password_and_configuration_fields_are_not_reported_as_unmapped():
    """These are dropped on purpose, per the owner's own instruction -- the warning
    must stay quiet about them or it stops being read."""
    _, unmapped = user_rows([_user()], date(2026, 9, 14))
    assert unmapped == set()


def test_a_genuinely_new_top_level_field_is_reported():
    _, unmapped = user_rows([_user(SomethingNew=True)], date(2026, 9, 14))
    assert "SomethingNew" in unmapped


def test_an_administrator_flag_survives():
    rows, _ = user_rows(
        [_user(Policy={"IsAdministrator": True, "IsDisabled": False})], date(2026, 9, 14)
    )
    assert rows[0]["is_administrator"] is True


def test_users_by_id_maps_id_to_name():
    assert users_by_id([_user(Id="u1", Name="guest"), _user(Id="u2", Name="viewer")]) == {
        "u1": "guest", "u2": "viewer",
    }


def test_users_by_id_skips_an_entry_with_no_id():
    assert users_by_id([_user(Id=None)]) == {}


# --------------------------------------------------------------------------------
# the activity log: a floor-only, newest-first, paginated API
# --------------------------------------------------------------------------------


def _entry(**overrides):
    base = {
        "Id": 1, "Date": "2026-09-12T12:00:00.0000000Z", "Type": "VideoPlayback",
        "Name": "n", "UserId": None, "ItemId": None, "Severity": "Information",
    }
    base.update(overrides)
    return base


def test_activity_window_keeps_only_entries_inside_the_day():
    day = date(2026, 9, 12)
    pages = [[
        _entry(Id=3, Date="2026-09-12T23:59:59.0000000Z"),  # inside
        _entry(Id=2, Date="2026-09-13T00:30:00.0000000Z"),  # too new -- next day
    ], [
        _entry(Id=1, Date="2026-09-11T23:00:00.0000000Z"),  # too old -- previous day
    ]]

    rows, _ = activity_window(day, pages=pages)

    assert [r["id"] for r in rows] == [3]


def test_activity_window_reports_a_field_the_schema_does_not_name():
    pages = [[_entry(Id=1, SomethingNew="x")]]
    _, unmapped = activity_window(date(2026, 9, 12), pages=pages)
    assert "SomethingNew" in unmapped


def test_activity_window_with_no_pages_and_no_url_refuses_rather_than_hang():
    with pytest.raises(ValueError, match="JELLYFIN_URL"):
        activity_window(date(2026, 9, 12))


def test_activity_window_reads_multiple_pages():
    """Pagination has to actually walk more than one page: a bug that only reads the
    first would look identical to a correct implementation on a one-page day."""
    day = date(2026, 9, 12)
    pages = [
        [_entry(Id=n, Date="2026-09-12T20:00:00.0000000Z") for n in range(200, 190, -1)],
        [_entry(Id=n, Date="2026-09-12T08:00:00.0000000Z") for n in range(10, 0, -1)],
    ]
    rows, _ = activity_window(day, pages=pages)
    assert len(rows) == 20


# --------------------------------------------------------------------------------
# the one-pass history load: every entry, bucketed by day, no minDate floor
# --------------------------------------------------------------------------------


def test_activity_history_pages_all_the_way_to_the_end():
    """A bug that stops after the first page would look identical to a correct
    implementation on a log short enough to fit in one -- the same shape of test as
    activity_window's own multi-page check, but with no day filter to stop it early."""
    pages = [
        [_entry(Id=n, Date="2026-09-12T20:00:00.0000000Z") for n in range(200, 190, -1)],
        [_entry(Id=n, Date="2023-11-21T16:15:05.0000000Z") for n in range(10, 0, -1)],
    ]
    by_day, _ = activity_history(pages=pages)
    assert sum(len(rows) for rows in by_day.values()) == 20


def test_activity_history_buckets_across_a_utc_midnight():
    """The whole point of bucketing by the entry's own date rather than a requested
    window: two entries twenty seconds apart either side of midnight must land in
    two different day partitions."""
    pages = [[
        _entry(Id=1, Date="2026-09-12T23:59:59.0000000Z"),
        _entry(Id=2, Date="2026-09-13T00:00:19.0000000Z"),
    ]]
    by_day, _ = activity_history(pages=pages)
    assert set(by_day) == {date(2026, 9, 12), date(2026, 9, 13)}
    assert [r["id"] for r in by_day[date(2026, 9, 12)]] == [1]
    assert [r["id"] for r in by_day[date(2026, 9, 13)]] == [2]


def test_activity_history_reaches_the_logs_first_day():
    """Verified against the real server on 2026-09-15: the earliest entry is
    2023-11-21T16:15:05Z. A fixture at that instant must bucket to that day, not be
    dropped as somehow out of range -- there is no floor here to drop it against."""
    by_day, _ = activity_history(pages=[[_entry(Id=1, Date="2023-11-21T16:15:05.0000000Z")]])
    assert date(2023, 11, 21) in by_day


def test_activity_history_skips_an_entry_with_no_parseable_date():
    by_day, _ = activity_history(pages=[[_entry(Id=1, Date=None)]])
    assert by_day == {}


def test_activity_history_reports_a_field_the_schema_does_not_name():
    _, unmapped = activity_history(pages=[[_entry(Id=1, SomethingNew="x")]])
    assert "SomethingNew" in unmapped


def test_activity_history_with_no_pages_and_no_url_refuses_rather_than_hang():
    with pytest.raises(ValueError, match="JELLYFIN_URL"):
        activity_history()


def test_activity_history_omits_minDate_entirely():
    """The whole reason this reaches three years back in one pass rather than
    paying activity_window's escalating per-day cost: no floor is ever sent."""
    from homelab_datalake.parsing.jellyfin import fetch_activity_page

    captured = {}
    import urllib.request as _urllib_request

    class _FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self):
            return b'{"Items": []}'

    def fake_urlopen(request, timeout=60):
        captured["url"] = request.full_url
        return _FakeResponse()

    original = _urllib_request.urlopen
    _urllib_request.urlopen = fake_urlopen
    try:
        fetch_activity_page("https://jf.example", "key", 0, 500)
    finally:
        _urllib_request.urlopen = original
    assert "minDate" not in captured["url"]


def test_writing_a_history_day_twice_replaces_rather_than_accumulates(tmp_path):
    """The property that makes the whole pass safe to re-run: the second write for a
    day must leave the lake with exactly the second call's rows, not both calls'."""
    lake = LakeStorage(local_root=str(tmp_path / "lake"))
    day = date(2023, 11, 21)

    first, _ = activity_history(
        pages=[[_entry(Id=n, Date="2023-11-21T16:15:05.0000000Z") for n in (1, 2, 3)]]
    )
    _, count, _ = _write_activity_history_day(lake, day, first[day])
    assert count == 3

    second, _ = activity_history(
        pages=[[_entry(Id=9, Date="2023-11-21T16:15:05.0000000Z")]]
    )
    _, count, _ = _write_activity_history_day(lake, day, second[day])
    assert count == 1

    table = lake.read_table(f"{ACTIVITY_PREFIX}/dt={day.isoformat()}/data.parquet")
    assert table.num_rows == 1
    assert table.to_pylist()[0]["id"] == 9


def test_a_repeated_write_of_the_same_day_is_byte_for_byte_the_same_shape(tmp_path):
    """Re-running the whole day with identical input must be indistinguishable from
    having run it once -- same row count, same content, not doubled."""
    lake = LakeStorage(local_root=str(tmp_path / "lake"))
    day = date(2023, 11, 21)
    by_day, _ = activity_history(
        pages=[[_entry(Id=n, Date="2023-11-21T16:15:05.0000000Z") for n in (1, 2)]]
    )

    _write_activity_history_day(lake, day, by_day[day])
    _write_activity_history_day(lake, day, by_day[day])

    table = lake.read_table(f"{ACTIVITY_PREFIX}/dt={day.isoformat()}/data.parquet")
    assert table.num_rows == 2


# --------------------------------------------------------------------------------
# the one-pass playback history: the whole PlaybackActivity table in one request,
# bucketed by each row's own hour and *merged* into the hours jellyfin_playback_raw
# owns -- never replacing them, unlike the activity history's per-day writes.
# --------------------------------------------------------------------------------


def _session(date_created, user_id="u1", item_id="item1", duration="53"):
    """One PlaybackActivity row, positional, in _PLAYBACK_COLUMNS order."""
    return [date_created, user_id, item_id, "Movie", "Example Movie",
            "DirectPlay", "Jellyfin Web", "Chrome", duration]


def _rows(*sessions):
    """Parsed rows, through the real parser and the real injectable payload path."""
    return playback_history(payload=_payload(list(sessions)))


def _hour_key(hour: datetime) -> str:
    return f"{PLAYBACK_PREFIX}/partition_hour={hour:%Y-%m-%dT%H:00:00}/data.parquet"


def _seed_hour(lake, hour, rows, extracted_at):
    """What jellyfin_playback_raw leaves behind for one hour, written here directly
    rather than through the helper under test: schema-projected rows at the hour's own
    prefix. The merge cases below have to start from an hour the *hourly* asset wrote,
    not one this pass wrote, or they would only be testing the helper against itself.
    """
    stamped = [{**row, "partition_hour": hour, "dt": row["started_at"].date(),
                "_extracted_at": extracted_at} for row in rows]
    projected, _ = project(stamped, _PLAYBACK_SCHEMA)
    lake.write_table(_hour_key(hour), pa.Table.from_pylist(projected, schema=_PLAYBACK_SCHEMA))


def _run_history(lake, rows, monkeypatch):
    """Drive the asset itself. The bucketing, the refusals and the check all live in
    the asset body, so a test of the helper alone would not reach any of them."""
    monkeypatch.setenv("JELLYFIN_URL", "https://jf.example")
    monkeypatch.setenv("JELLYFIN_API_KEY", "key")
    monkeypatch.setattr(jellyfin_assets.jellyfin_source, "playback_history",
                        lambda *a, **k: rows)
    results = list(jellyfin_playback_history_raw(build_asset_context(), lake))
    output = next(r for r in results if isinstance(r, Output))
    check = next(r for r in results if isinstance(r, AssetCheckResult))
    return output, check


def _meta(result, key):
    """Dagster wraps every metadata value, even on a direct invocation like this one."""
    return result.metadata[key].value


# -- the query: one statement, whole table, and a column list that cannot drift ----


def test_the_history_query_asks_for_the_whole_table_with_no_window():
    """No WHERE at all is the point: the plugin runs one SQL statement and answers
    with the entire result set, so the whole table costs a single request."""
    assert "WHERE" not in _playback_history_query().upper()


def test_both_queries_name_exactly_the_same_columns_in_the_same_order():
    """parse_playback_rows reads results by position and rejects a `colums` list that
    is not _PLAYBACK_COLUMNS, so a column list that drifted between the two queries
    would fail on the real server rather than here."""
    head = f"SELECT {', '.join(_PLAYBACK_COLUMNS)} FROM PlaybackActivity"  # noqa: S608
    window = _playback_query(datetime(2026, 9, 12, 18, tzinfo=UTC),
                             datetime(2026, 9, 12, 19, tzinfo=UTC))
    assert _playback_history_query().startswith(head)
    assert window.startswith(head)


def test_the_history_path_produces_the_same_row_shape_as_a_window():
    """The two paths write into one prefix, so their rows have to be the same rows."""
    payload = _payload([_session("2026-09-12 18:00:21.6180609")])
    assert playback_history(payload=payload) == playback_window(
        datetime(2026, 9, 12, 18, tzinfo=UTC), datetime(2026, 9, 12, 19, tzinfo=UTC),
        payload=payload,
    )


def test_playback_history_with_no_payload_and_no_url_refuses_rather_than_hang():
    with pytest.raises(ValueError, match="JELLYFIN_URL"):
        playback_history()


# -- bucketing: by the row's own hour, never by a requested window -----------------


def test_the_pass_buckets_two_sessions_either_side_of_an_hour_boundary(tmp_path, monkeypatch):
    """The whole reason this asset buckets per row rather than writing one object:
    two sessions twenty seconds apart across the top of an hour belong to two
    different hourly partitions, and a range run of jellyfin_playback_raw would have
    put both under the range's first hour instead."""
    lake = LakeStorage(local_root=str(tmp_path / "lake"))
    rows = _rows(
        _session("2026-09-12 18:59:59.0000000", item_id="item1"),
        _session("2026-09-12 19:00:19.0000000", item_id="item2"),
    )

    output, check = _run_history(lake, rows, monkeypatch)

    assert _meta(output, "hours") == 2
    assert check.passed is True
    first = lake.read_table(_hour_key(datetime(2026, 9, 12, 18, tzinfo=UTC)))
    second = lake.read_table(_hour_key(datetime(2026, 9, 12, 19, tzinfo=UTC)))
    assert [r["item_id"] for r in first.to_pylist()] == ["item1"]
    assert [r["item_id"] for r in second.to_pylist()] == ["item2"]


def test_each_hours_rows_carry_that_hour_and_that_rows_date(tmp_path, monkeypatch):
    """partition_hour is the row's own floor and dt is the row's own date -- exactly
    what jellyfin_playback_raw writes, so a reader globbing the prefix cannot tell
    which asset produced a row."""
    lake = LakeStorage(local_root=str(tmp_path / "lake"))
    rows = _rows(_session("2025-02-14 22:03:38.7652082"))

    _run_history(lake, rows, monkeypatch)

    written = lake.read_table(_hour_key(datetime(2025, 2, 14, 22, tzinfo=UTC))).to_pylist()
    assert written[0]["partition_hour"] == datetime(2025, 2, 14, 22, tzinfo=UTC)
    assert written[0]["dt"] == date(2025, 2, 14)


def test_the_earliest_session_the_plugin_holds_is_loaded_not_dropped(tmp_path, monkeypatch):
    """Measured on the real server on 2026-09-15: the oldest row is
    2025-02-14 22:03:38. There is no floor in this query to drop it against."""
    lake = LakeStorage(local_root=str(tmp_path / "lake"))
    output, _ = _run_history(lake, _rows(_session("2025-02-14 22:03:38.7652082")), monkeypatch)
    assert _meta(output, "first_hour") == "2025-02-14T22:00:00"


# -- merge, not replace: an hour the hourly asset already wrote keeps its rows -----


def test_an_hour_already_written_keeps_rows_the_plugin_no_longer_offers(tmp_path, monkeypatch):
    """The case that makes replacing wrong here. The lake holds a session the plugin's
    table no longer returns; replacing the hour would delete it, which would mean this
    pass destroyed history while loading it."""
    lake = LakeStorage(local_root=str(tmp_path / "lake"))
    hour = datetime(2026, 9, 12, 18, tzinfo=UTC)
    already_in_the_lake = _rows(
        _session("2026-09-12 18:00:21.6180609", item_id="kept"),
        _session("2026-09-12 18:30:00.0000000", item_id="still-offered"),
    )
    _seed_hour(lake, hour, already_in_the_lake, datetime(2026, 9, 12, 18, 5, tzinfo=UTC))

    still_offered = _rows(_session("2026-09-12 18:30:00.0000000", item_id="still-offered"))
    output, check = _run_history(lake, still_offered, monkeypatch)

    written = lake.read_table(_hour_key(hour)).to_pylist()
    assert sorted(r["item_id"] for r in written) == ["kept", "still-offered"]
    assert _meta(output, "sessions_only_in_lake") == 1
    # And the check still passes: more rows than this pass offered is legitimate here,
    # which is exactly why the invariant is not equality with the written count.
    assert check.passed is True


def test_a_longer_duration_from_this_pass_wins_over_the_hourly_assets_partial_read(
    tmp_path, monkeypatch
):
    """Same session, seen mid-flight by an hourly run and complete by this pass: the
    merge keys on (started_at, user_id, item_id), so it is one row, the newer one."""
    lake = LakeStorage(local_root=str(tmp_path / "lake"))
    hour = datetime(2026, 9, 12, 18, tzinfo=UTC)
    _seed_hour(lake, hour, _rows(_session("2026-09-12 18:00:21.6180609", duration="53")),
               datetime(2026, 9, 12, 18, 5, tzinfo=UTC))

    _run_history(lake, _rows(_session("2026-09-12 18:00:21.6180609", duration="6761")),
                 monkeypatch)

    written = lake.read_table(_hour_key(hour)).to_pylist()
    assert len(written) == 1
    assert written[0]["play_duration_seconds"] == 6761


def test_re_running_the_whole_pass_leaves_one_copy_of_each_session(tmp_path, monkeypatch):
    """Idempotence, the property that makes a one-off load safe to repeat after a
    partial failure: the second pass's rows carry the keys the first pass's rows have
    and win on recency rather than landing beside them."""
    lake = LakeStorage(local_root=str(tmp_path / "lake"))
    hour = datetime(2026, 9, 12, 18, tzinfo=UTC)
    rows = _rows(
        _session("2026-09-12 18:00:21.6180609", item_id="item1"),
        _session("2026-09-12 18:30:00.0000000", item_id="item2"),
    )

    _run_history(lake, rows, monkeypatch)
    output, check = _run_history(lake, rows, monkeypatch)

    assert lake.read_table(_hour_key(hour)).num_rows == 2
    assert _meta(output, "sessions_only_in_lake") == 0
    assert check.passed is True


# -- a session with no timestamp, and the refusal to write nothing -----------------


def test_a_session_with_no_timestamp_is_counted_rather_than_silently_dropped(
    tmp_path, monkeypatch
):
    """There is no hour to write it to and no merge key to write it under, so it
    cannot land -- but a non-zero count here means DateCreated stopped parsing, which
    has to be visible rather than inferred from a short row count."""
    lake = LakeStorage(local_root=str(tmp_path / "lake"))
    rows = _rows(
        _session(None, item_id="undateable"),
        _session("2026-09-12 18:00:21.6180609", item_id="item1"),
    )

    output, check = _run_history(lake, rows, monkeypatch)

    assert _meta(output, "sessions_without_a_timestamp") == 1
    # Everything the plugin returned is accounted for: 2 back, 1 written, 1 named.
    assert _meta(output, "sessions_returned_by_plugin") == 2
    assert _meta(output, "sessions_bucketed") == 1
    written = lake.read_table(_hour_key(datetime(2026, 9, 12, 18, tzinfo=UTC)))
    assert [r["item_id"] for r in written.to_pylist()] == ["item1"]
    assert check.passed is True


def test_the_pass_refuses_to_write_nothing_when_the_plugin_returns_no_rows(
    tmp_path, monkeypatch
):
    """An empty answer from a table with [count withheld] rows in it means an unreachable server
    or a disabled plugin, not an empty history."""
    lake = LakeStorage(local_root=str(tmp_path / "lake"))
    with pytest.raises(ValueError, match="refusing to write nothing"):
        _run_history(lake, [], monkeypatch)


def test_the_pass_refuses_when_no_session_at_all_can_be_bucketed(tmp_path, monkeypatch):
    """Rows came back but not one of them has a usable DateCreated: nothing can be
    written, so this refuses rather than reporting a successful load of zero hours."""
    lake = LakeStorage(local_root=str(tmp_path / "lake"))
    with pytest.raises(ValueError, match="unparseable DateCreated"):
        _run_history(lake, _rows(_session(None), _session("")), monkeypatch)


# -- the reconciliation check: what it is, and that it would catch a lost row ------


def test_the_check_fails_when_a_session_never_reaches_the_object(tmp_path, monkeypatch):
    """The check has to be more than decoration. With a deliberately lossy write --
    one row short of what the pass collected for the hour -- the invariant
    `written - retained == distinct collected` must break."""
    lake = LakeStorage(local_root=str(tmp_path / "lake"))
    real_write = jellyfin_assets._write_playback_history_hour

    def drops_one(lake_, hour, rows, extracted_at):
        return real_write(lake_, hour, rows[:-1], extracted_at)

    monkeypatch.setattr(jellyfin_assets, "_write_playback_history_hour", drops_one)
    rows = _rows(
        _session("2026-09-12 18:00:21.6180609", item_id="item1"),
        _session("2026-09-12 18:30:00.0000000", item_id="item2"),
    )

    _output, check = _run_history(lake, rows, monkeypatch)

    assert check.passed is False
    assert "2026-09-12T18:00:00" in _meta(check, "mismatched_hours")


def test_the_check_counts_distinct_sessions_not_raw_rows(tmp_path, monkeypatch):
    """Two identical rows are one session to the merge, which keys on
    (started_at, user_id, item_id). Counting raw rows instead would report a dropped
    row every time the plugin's table held an exact duplicate."""
    lake = LakeStorage(local_root=str(tmp_path / "lake"))
    duplicated = _rows(
        _session("2026-09-12 18:00:21.6180609"),
        _session("2026-09-12 18:00:21.6180609"),
    )

    _output, check = _run_history(lake, duplicated, monkeypatch)

    assert check.passed is True
    assert lake.read_table(_hour_key(datetime(2026, 9, 12, 18, tzinfo=UTC))).num_rows == 1
