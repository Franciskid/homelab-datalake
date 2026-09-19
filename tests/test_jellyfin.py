"""Jellyfin extraction tests. Items are injected, so no server is needed."""

import json
from datetime import UTC, date, datetime, timedelta, timezone

import pytest

from homelab_datalake.assets.jellyfin import (
    SHRINK_OVERRIDE_VAR,
    _shrink_permitted,
    library_shrank,
)
from homelab_datalake.parsing.jellyfin import (
    FIELDS,
    _parse_timestamp,
    fetch_page,
    resolution_class,
    snapshot,
)

DAY = date(2026, 8, 28)


def item(**overrides):
    base = {
        "Id": "abc123",
        "Name": "300",
        "ProductionYear": 2007,
        "PremiereDate": "2007-03-07T00:00:00.0000000Z",
        "DateCreated": "2023-12-01T20:14:25.0000000Z",
        "CommunityRating": 7.177,
        "CriticRating": 61,
        "OfficialRating": "FR-12",
        "RunTimeTicks": 69988620000,
        "Container": "mkv",
        "Genres": ["Action", "Aventure", "Guerre"],
        "ProviderIds": {"Imdb": "tt0416449", "Tmdb": "1271"},
        "Studios": [{"Name": "Atmosphere Entertainment MM"}],
        "MediaStreams": [
            {"Type": "Video", "Codec": "h264", "Height": 1040, "Width": 1920, "BitDepth": 8},
            {"Type": "Audio", "Codec": "ac3", "Channels": 6},
        ],
        # Deliberately present in the fixture: these must not reach the output.
        "Path": "/Media/Movies/[ Torrent911.cc ] 300 [1080p] MULTI 2006.mkv",
        "Overview": "Adapte du roman graphique de Frank Miller...",
    }
    base.update(overrides)
    return base


def test_the_file_path_never_reaches_the_row():
    """Path carries filesystem topology and, in this library, the source site name.

    Overview used to be held back beside it, on the grounds that no question asked
    about it. That was only true until somebody asked one, and prose about a film is
    not the same kind of secret as the name of the site the file came from -- so the
    synopsis now travels and the path still does not.
    """
    out = snapshot(DAY, items=[item()])
    row = out.titles[0]
    assert "path" not in row
    blob = " ".join(str(v) for v in row.values())
    assert "Torrent911" not in blob
    assert "/Media/Movies" not in blob
    assert row["overview"].startswith("Adapte du roman graphique")


def test_a_field_jellyfin_adds_is_reported_rather_than_vanishing():
    """The signal whose absence lost the camera's `counts` for a month."""
    out = snapshot(DAY, items=[item(SomethingNew={"a": 1})])
    assert "SomethingNew" in out.unmapped
    assert '"SomethingNew"' in out.titles[0]["_unmapped"]


def test_a_field_deliberately_not_stored_is_not_reported_as_unmapped():
    """`_unmapped` is a prompt to add a column, so it must stay quiet about Path."""
    out = snapshot(DAY, items=[item()])
    assert "Path" not in out.unmapped
    assert out.titles[0]["_unmapped"] is None


@pytest.mark.parametrize(
    ("height", "expected"),
    [
        (2160, "4K"), (1080, "1080p"),
        # The interesting cases: real cropped and letterboxed encodes from this
        # library. Equality bucketing would scatter these across a dozen classes.
        (1040, "1080p"), (1036, "1080p"),
        (816, "720p"), (808, "720p"), (804, "720p"), (800, "720p"), (720, "720p"),
        (576, "SD"), (480, "low"),
        (None, None), (0, None),
    ],
)
def test_resolution_class_buckets_real_heights(height, expected):
    assert resolution_class(height) == expected


def test_seven_digit_fractional_seconds_are_parsed():
    """Jellyfin sends seven digits; Python's fromisoformat accepts at most six."""
    parsed = _parse_timestamp("2023-12-01T20:14:25.0000000Z")
    assert parsed is not None
    assert (parsed.year, parsed.month, parsed.day, parsed.hour) == (2023, 12, 1, 20)
    assert parsed.tzinfo is not None


# .NET writes seven fractional digits but trims trailing zeros, so the activity log
# sends fractions of every length from one to seven. These three are verbatim from the
# 291 real entries the old parser rejected, measured on 2026-09-15.
@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("2023-11-22T15:22:08.16003Z", datetime(2023, 11, 22, 15, 22, 8, 160030, tzinfo=UTC)),
        ("2023-11-23T09:35:10.01098Z", datetime(2023, 11, 23, 9, 35, 10, 10980, tzinfo=UTC)),
        ("2023-11-23T19:35:53.47435Z", datetime(2023, 11, 23, 19, 35, 53, 474350, tzinfo=UTC)),
    ],
)
def test_real_five_digit_fractions_from_the_activity_log_parse(value, expected):
    assert _parse_timestamp(value) == expected


@pytest.mark.parametrize(
    ("fraction", "microsecond"),
    [
        ("1", 100000),
        ("12", 120000),
        ("123", 123000),
        ("1234", 123400),
        ("12345", 123450),
        ("123456", 123456),
        ("1234567", 123456),  # seven digits: truncated, not rounded
    ],
)
def test_every_fraction_length_from_one_to_seven_digits_parses(fraction, microsecond):
    parsed = _parse_timestamp(f"2023-12-01T20:14:25.{fraction}Z")
    assert parsed == datetime(2023, 12, 1, 20, 14, 25, microsecond, tzinfo=UTC)


def test_no_fraction_at_all_parses():
    assert _parse_timestamp("2023-12-01T20:14:25Z") == datetime(2023, 12, 1, 20, 14, 25, tzinfo=UTC)


@pytest.mark.parametrize("fraction", ["1", "16003", "160031", "1600312"])
def test_an_explicit_positive_offset_is_kept_whatever_the_fraction_length(fraction):
    """The old parser ate an offset's digits into a short fraction: `.1+02:00` came
    back as a valid instant two hours off, rather than failing."""
    parsed = _parse_timestamp(f"2023-11-22T17:22:08.{fraction}+02:00")
    assert parsed.utcoffset() == timedelta(hours=2)
    assert parsed.astimezone(UTC).replace(microsecond=0) == datetime(
        2023, 11, 22, 15, 22, 8, tzinfo=UTC
    )


@pytest.mark.parametrize("fraction", ["1", "16003", "1600312"])
def test_an_explicit_negative_offset_is_kept_whatever_the_fraction_length(fraction):
    parsed = _parse_timestamp(f"2023-11-22T10:22:08.{fraction}-05:00")
    assert parsed.tzinfo == timezone(timedelta(hours=-5))
    assert parsed.astimezone(UTC).replace(microsecond=0) == datetime(
        2023, 11, 22, 15, 22, 8, tzinfo=UTC
    )


@pytest.mark.parametrize("value", ["2023-11-22T15:22:08.16003", "2023-11-22T15:22:08"])
def test_no_offset_is_read_as_utc(value):
    parsed = _parse_timestamp(value)
    assert parsed.tzinfo == UTC
    assert parsed.replace(microsecond=0) == datetime(2023, 11, 22, 15, 22, 8, tzinfo=UTC)


@pytest.mark.parametrize("value", [None, "", "not a date", "2023-11-22T15:22:08.Z",
                                   "2023-11-22T15:22:08.+00:00"])
def test_a_timestamp_that_cannot_be_read_is_none(value):
    assert _parse_timestamp(value) is None


def test_runtime_ticks_become_minutes():
    out = snapshot(DAY, items=[item()])
    assert out.titles[0]["runtime_minutes"] == 117  # 69988620000 ticks


def test_genres_become_pairs_not_columns():
    out = snapshot(DAY, items=[item()])
    assert len(out.titles) == 1
    assert {g["genre"] for g in out.genres} == {"Action", "Aventure", "Guerre"}
    assert all(g["item_id"] == "abc123" for g in out.genres)


def test_a_title_with_no_genres_still_produces_a_title_row():
    out = snapshot(DAY, items=[item(Genres=[])])
    assert len(out.titles) == 1 and out.genres == []


def test_missing_streams_and_ids_do_not_break_extraction():
    out = snapshot(DAY, items=[item(MediaStreams=[], ProviderIds={}, RunTimeTicks=None,
                                    ProductionYear=None, PremiereDate=None)])
    row = out.titles[0]
    assert row["video_codec"] is None and row["resolution_class"] is None
    assert row["tmdb_id"] is None and row["runtime_minutes"] is None
    assert row["decade"] is None


def test_decade_is_derived_from_production_year():
    assert snapshot(DAY, items=[item(ProductionYear=1994)]).titles[0]["decade"] == 1990
    assert snapshot(DAY, items=[item(ProductionYear=2007)]).titles[0]["decade"] == 2000


def test_playback_fields_are_never_requested_on_the_library_snapshot():
    """This is about the movie snapshot specifically, not a claim that the warehouse
    holds no playback data anywhere -- since 2026-09-14 it does, read from the
    Playback Reporting plugin by the functions tested in test_jellyfin_playback.py.
    What stays true here is that FIELDS, which asks the standard `/Items` endpoint
    for one snapshot of the library, never asks it for a user's own history inline.
    """
    for forbidden in ("UserData", "PlayCount", "Sessions"):
        assert forbidden not in FIELDS


def test_snapshot_is_idempotent():
    items = [item(Id=f"id{i}") for i in range(20)]
    assert snapshot(DAY, items=items).titles == snapshot(DAY, items=items).titles


# --------------------------------------------------------------------------------
# the 19 Sep 2026 collapse: a query that stopped meaning what it says
# --------------------------------------------------------------------------------
#
# Jellyfin 12.1.0 changed the default of `CollapseBoxSetItems`: a movie that belongs
# to a collection is folded into the BoxSet holding it, and the BoxSet is returned in
# its place. `IncludeItemTypes=Movie` answered TotalRecordCount=1068 -- 882 standalone
# films plus 186 collections -- for a library /Items/Counts still reported as 3,135
# films. Nothing failed: the snapshot wrote 1,068 rows, 184 of them collections
# described as films, and SCD2 recorded 2,260 titles as having left the library.


def test_the_query_says_it_wants_collections_left_alone():
    """The parameter is the fix, so it is the thing worth pinning.

    Measured against the real server on 2026-09-19: without it 1,068 items come back,
    with it 3,135. `ExcludeItemTypes=BoxSet` does not substitute -- the collapsing
    happens before the type filter, so that query still answered 1,068.
    """
    captured = {}

    class FakeResponse:
        def read(self):
            return b'{"Items": [], "TotalRecordCount": 0}'

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    def fake_urlopen(request, timeout=None):
        captured["url"] = request.full_url
        return FakeResponse()

    import urllib.request

    original = urllib.request.urlopen
    urllib.request.urlopen = fake_urlopen
    try:
        fetch_page("https://jellyfin.example", "key", 0, 500)
    finally:
        urllib.request.urlopen = original

    assert "CollapseBoxSetItems=false" in captured["url"]


def test_the_types_that_came_back_are_counted():
    out = snapshot(DAY, items=[item(Id="a", Type="Movie"), item(Id="b", Type="Movie")])
    assert out.types == {"Movie": 2}
    assert out.foreign_types == {}


def test_a_collection_among_the_films_is_reported_as_foreign():
    """The shape of the 19 Sep snapshot, in miniature.

    A BoxSet has no runtime, no codec and no TMDB id, so it lands as a film with
    almost every column null -- which is why this cannot be left to the shrink guard
    alone: a collapse that swapped rows one-for-one would not shrink anything.
    """
    out = snapshot(
        DAY, items=[item(Id="a", Type="Movie"), item(Id="set", Type="BoxSet", Name="#Pixar")]
    )
    assert out.foreign_types == {"BoxSet": 1}
    assert out.types["Movie"] == 1


def test_an_item_with_no_type_is_still_counted_rather_than_ignored():
    out = snapshot(DAY, items=[item(Id="a", Type=None)])
    assert out.foreign_types == {"unknown": 1}


# --------------------------------------------------------------------------------
# the shrink guard, as the pure predicate the asset composes
# --------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "count, previous, shrank",
    [
        (3143, 3143, False),
        (3135, 3143, False),        # a handful of deletions is an ordinary day
        (2515, 3143, False),        # just inside a fifth
        (2514, 3143, True),         # just outside it
        (1068, 3143, True),         # 19 Sep 2026
        (3143, None, False),        # nothing to compare against
        (0, 3143, True),
    ],
)
def test_the_shrink_guard_draws_the_line_at_a_fifth(count, previous, shrank):
    assert library_shrank(count, previous) is shrank


def test_a_shrink_is_refused_unless_the_day_is_named(monkeypatch):
    """The override is per day, so it cannot be set once and left on.

    A guard that can be disabled permanently by a flag somebody set during one
    incident is a comment, not a guard.
    """
    monkeypatch.delenv(SHRINK_OVERRIDE_VAR, raising=False)
    assert _shrink_permitted(date(2026, 9, 19)) is False

    monkeypatch.setenv(SHRINK_OVERRIDE_VAR, "2026-09-19")
    assert _shrink_permitted(date(2026, 9, 19)) is True
    assert _shrink_permitted(date(2026, 9, 20)) is False

    monkeypatch.setenv(SHRINK_OVERRIDE_VAR, "2026-09-19, 2026-09-20")
    assert _shrink_permitted(date(2026, 9, 20)) is True

    monkeypatch.setenv(SHRINK_OVERRIDE_VAR, "all")
    assert _shrink_permitted(date(2027, 1, 1)) is True


# --------------------------------------------------------------------------------
# `_unmapped`: kept, as the check says, rather than overwritten on the way to disk
# --------------------------------------------------------------------------------
#
# The snapshot computes each item's leftovers and puts them on the row as `_unmapped`,
# because the payload is gone by the time the asset projects the row to its schema.
# `project()` skipped that value and then wrote its own leftovers over it -- none, for
# a row built to the schema -- so on 2026-09-19 `_unmapped` was null on every one of
# 3,135 films and 16,378 library items while `no_unmapped_fields` named the fields
# every day as kept. These run the real path: snapshot, then project.


def test_a_field_nobody_reads_survives_projection_into_the_row_written():
    from homelab_datalake.assets.jellyfin import _TITLE_SCHEMA
    from homelab_datalake.projection import project

    out = snapshot(DAY, items=[item(Type="Movie", BrandNewField="kept")])
    projected, seen = project(out.titles, _TITLE_SCHEMA)

    assert json.loads(projected[0]["_unmapped"]) == {"BrandNewField": "kept"}
    assert "BrandNewField" in seen


def test_the_same_holds_for_the_wider_library():
    from homelab_datalake.assets.jellyfin import _LIBRARY_ITEMS_SCHEMA
    from homelab_datalake.parsing.jellyfin import library_items_snapshot
    from homelab_datalake.projection import project

    series = {"Id": "s1", "Name": "Show", "Type": "Series", "BrandNewField": 3}
    out = library_items_snapshot(DAY, items=[series])
    projected, _ = project(out.items, _LIBRARY_ITEMS_SCHEMA)

    assert json.loads(projected[0]["_unmapped"]) == {"BrandNewField": 3}


def test_an_empty_value_is_not_reported_as_unmapped():
    """Nothing to lose, and reporting it made the warning permanent.

    Every item carried `ChannelId` and 907 films carried `AspectRatio`, all empty.
    """
    out = snapshot(DAY, items=[item(ChannelId=None, AspectRatio="", Extra=[], More={})])
    assert out.unmapped == set()
    assert out.titles[0]["_unmapped"] is None


def test_a_zero_or_false_is_an_answer_and_is_still_reported():
    out = snapshot(DAY, items=[item(NewCount=0, NewFlag=False)])
    assert {"NewCount", "NewFlag"} <= out.unmapped


def test_the_fields_twelve_one_added_have_columns():
    out = snapshot(DAY, items=[item(
        OriginalLanguage="ja", PartCount=2, IsoType="Dvd", Video3DFormat="HalfSideBySide",
    )])
    row = out.titles[0]
    assert (row["original_language"], row["part_count"], row["iso_type"],
            row["video_3d_format"]) == ("ja", 2, "Dvd", "HalfSideBySide")
    assert out.unmapped == set()
