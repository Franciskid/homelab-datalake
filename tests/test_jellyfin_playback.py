"""Playback, activity log and account list extraction. Payloads are injected, so no
server is needed -- the two facts that matter (DateCreated is UTC, PlayDuration is
seconds) were verified against the real server on 2026-09-14, not assumed; see
`_parse_playback_timestamp` and `_parse_duration_seconds` in `parsing/jellyfin.py`
for the worked evidence these tests encode as fixtures.
"""

from datetime import UTC, date, datetime

import pytest

from homelab_datalake.assets.jellyfin import (
    _PLAYBACK_SCHEMA,
    _merge_playback,
    _playback_key,
)
from homelab_datalake.parsing.jellyfin import (
    _PLAYBACK_COLUMNS,
    _parse_duration_seconds,
    _parse_playback_timestamp,
    _sqlite_literal,
    activity_window,
    parse_playback_rows,
    parse_series_name,
    user_rows,
    users_by_id,
)

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
    rows, unmapped = user_rows([_user()], date(2026, 9, 14))
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
