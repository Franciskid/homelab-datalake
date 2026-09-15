"""Jellyfin extraction tests. Items are injected, so no server is needed."""

from datetime import date

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
        "Path": "/Media/Movies/ExampleMovie.mkv",
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
    assert "ExampleMovie" not in blob
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
