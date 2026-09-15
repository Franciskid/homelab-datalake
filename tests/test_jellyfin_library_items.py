"""Series, seasons, episodes, box sets and other videos. Items are injected, so no
server is needed -- the field shapes below are the real ones read from the server on
2026-09-15, not invented, and the `People`-exclusion tests are a regression guard on
a measured 30x performance difference (see LIBRARY_ITEM_FIELDS in
parsing/jellyfin.py).
"""

from datetime import date

from homelab_datalake.parsing.jellyfin import (
    LIBRARY_ITEM_FIELDS,
    _library_item_row,
    library_items_snapshot,
)

DAY = date(2026, 9, 15)


def test_people_is_never_requested_for_library_items():
    """Measured, not assumed: [duration withheld] of a [duration withheld] page of [count withheld] episodes was People
    alone. Cast and crew per episode was never asked for."""
    assert "People" not in LIBRARY_ITEM_FIELDS.split(",")


def test_child_count_fields_are_not_requested_either():
    """Tried against a real BoxSet and a real Season on 2026-09-15; both came back
    null, so asking for them would add request weight for an always-null column."""
    for forbidden in ("ChildCount", "RecursiveItemCount"):
        assert forbidden not in LIBRARY_ITEM_FIELDS.split(",")


# Real shapes, read from the server on 2026-09-15 -- see the WS-41.B follow-up report
# for the live queries these were taken from.


def test_a_series_carries_status_and_end_date_not_a_season_or_episode():
    series = {
        "Id": "s1", "Name": "Apocalypse", "Type": "Series", "ProductionYear": 2023,
        "PremiereDate": "2023-02-21T00:00:00.0000000Z",
        "EndDate": "2023-02-21T00:00:00.0000000Z", "Status": "Ended",
        "DateCreated": "2024-01-03T00:46:04.077508Z", "CommunityRating": 7.8,
        "ProviderIds": {"Imdb": "tt27126625", "Tmdb": "214849"},
        "ParentId": "root-folder", "Genres": ["Documentaire"],
        # Present on a real Series response; must not reach the row or be unmapped.
        "People": [{"Name": "x"}], "ChannelId": None, "AirDays": [],
    }
    out = library_items_snapshot(DAY, items=[series])
    row = out.items[0]
    assert row["item_type"] == "Series"
    assert row["status"] == "Ended"
    assert row["end_date"] == date(2023, 2, 21)
    assert row["series_id"] is None and row["season_id"] is None
    assert row["tmdb_id"] == "214849" and row["imdb_id"] == "tt27126625"
    assert out.unmapped == set()


def test_a_season_carries_its_series_id_as_both_parent_and_series():
    season = {
        "Id": "se1", "Name": "Bonus", "Type": "Season", "IndexNumber": 0,
        "SeriesId": "s2", "SeriesName": "Kaamelott", "ParentId": "s2",
        "DateCreated": "2026-09-15T00:56:58.1406775Z",
        "PremiereDate": "2005-09-28T00:00:00.0000000Z", "ProductionYear": 2005,
        "ProviderIds": {},
    }
    row = library_items_snapshot(DAY, items=[season]).items[0]
    assert row["item_type"] == "Season"
    assert row["series_id"] == "s2" == row["parent_id"]
    assert row["series_name"] == "Kaamelott"
    assert row["index_number"] == 0
    assert row["season_id"] is None, "a Season's own id is its item_id, not season_id"


def test_an_episode_carries_both_series_and_season():
    episode = {
        "Id": "ep1", "Name": "Example Series - 00 - Extra", "Type": "Episode",
        "IndexNumber": 0, "ParentIndexNumber": 0, "SeriesId": "sr1",
        "SeriesName": "Example Series", "SeasonId": "se2", "SeasonName": "Bonus",
        "ParentId": "se2", "RunTimeTicks": 782129984, "ProviderIds": {},
        "Container": "mkv,webm", "DateCreated": "2023-12-20T11:12:51.1368064Z",
        "MediaStreams": [{"Type": "Video", "Codec": "h264", "Height": 480, "Width": 720}],
    }
    row = library_items_snapshot(DAY, items=[episode]).items[0]
    assert row["item_type"] == "Episode"
    assert row["series_id"] == "sr1" and row["series_name"] == "Example Series"
    assert row["season_id"] == "se2" == row["parent_id"]
    assert row["index_number"] == 0 and row["parent_index_number"] == 0
    assert row["video_codec"] == "h264"
    assert row["resolution_class"] == "low"  # 480 is below the SD floor of 500


def test_a_boxset_has_no_series_or_season_concept():
    boxset = {
        "Id": "b1", "Name": "#A24", "Type": "BoxSet", "ProductionYear": 2013,
        "ParentId": "root-folder", "ProviderIds": {}, "Genres": ["Drame"],
        "Studios": [{"Name": "A24"}], "People": [], "DisplayOrder": "SortName",
    }
    row = library_items_snapshot(DAY, items=[boxset]).items[0]
    assert row["item_type"] == "BoxSet"
    assert row["series_id"] is None and row["season_id"] is None
    assert row["studio"] == "A24"
    assert row["genres"] == ["Drame"]


def test_a_standalone_video_has_no_parent_at_all():
    video = {
        "Id": "v1", "Name": "Home movie", "Type": "Video", "RunTimeTicks": 39590080512,
        "Container": "mkv,webm", "ParentId": None, "ProviderIds": {}, "People": [],
    }
    row = library_items_snapshot(DAY, items=[video]).items[0]
    assert row["item_type"] == "Video"
    assert row["parent_id"] is None
    assert row["series_id"] is None


def test_a_field_the_schema_does_not_name_is_reported():
    out = library_items_snapshot(DAY, items=[{"Id": "x1", "Type": "Video",
                                              "SomethingNew": {"a": 1}}])
    assert "SomethingNew" in out.unmapped
    assert '"SomethingNew"' in out.items[0]["_unmapped"]


def test_movies_are_never_produced_by_this_function():
    """This is an addition beside snapshot(), not a replacement -- a Movie item
    accidentally passed in should still be shaped, but nothing here ever asks the
    API for one; LIBRARY_ITEM_TYPES names the five other types explicitly."""
    from homelab_datalake.parsing.jellyfin import LIBRARY_ITEM_TYPES
    assert "Movie" not in LIBRARY_ITEM_TYPES.split(",")


def test_a_missing_end_date_is_null_not_a_sentinel():
    """An ongoing show has no end date yet -- a real state, not a missing one."""
    row = _library_item_row({"Id": "s1", "Type": "Series", "Status": "Continuing"}, DAY)
    assert row["end_date"] is None
    assert row["status"] == "Continuing"
