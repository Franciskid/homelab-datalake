"""Jellyfin extraction tests. Items are injected, so no server is needed."""

from datetime import UTC, date, datetime, timedelta, timezone

import pytest

from homelab_datalake.parsing.jellyfin import (
    FIELDS,
    _parse_timestamp,
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
