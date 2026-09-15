"""Extracting the media library from Jellyfin's API.

A full snapshot per day. At ~[count withheld] titles that is one paginated read taking under a
second, and snapshots are what SCD2 wants anyway -- an incremental sync would be more
code, more state, and no faster.

Reading the API rather than the CSV export matters for one concrete reason:
`ProviderIds.Tmdb` comes back as a real identifier on **[rate withheld]** of titles. Any later
enrichment is then a key lookup instead of fuzzy title matching, which is what gets
remakes and translated titles wrong.

Most of what people reach for TMDB to get is already here, because Jellyfin fetched it
from TMDB during the scan: the community rating *is* TMDB's vote average, and the
original title and production country come back at [rate withheld] and [rate withheld]. Only figures
Jellyfin has no field for -- budget, revenue, popularity, vote count -- need the API.

Playback history was deliberately not requested here until 2026-09-14, on the grounds
that it is personal and adds nothing the metadata does not. The owner reversed that
decision that day: asked for "this weekend's Jellyfin usage" with a graph of watch time
per user, the platform's own Datalake Analyst correctly refused, because the warehouse
held the library catalogue and nothing else -- while the household's daily recap email
was, at that moment, already reading the same watch time live from the Playback
Reporting plugin. A warehouse that cannot answer a question its own household already
gets answered elsewhere is a gap, not a boundary, so the rule became: the warehouse
holds everything the Jellyfin API and its plugins offer. `Path` is still requested and
dropped -- see `NOT_STORED` -- because filesystem topology was never part of this
decision and still is not.

Playback, the server activity log and the account list are read by the functions at the
bottom of this file, and are not folded into `snapshot()` above because they are a
different shape of problem: three different APIs (the Playback Reporting plugin's own
SQL endpoint, the standard activity log, and the standard user list), landing through
their own assets in `assets.jellyfin` on their own partitioning -- hourly for playback,
so a question asked on Sunday evening about Sunday gets an answer, daily for the rest.

Three objects per snapshot became five. A title has many genres, many media streams and
many people, and the honest shape for each is a row rather than a column: keeping only
the first audio stream described a bilingual film as monolingual, and keeping only the
first studio described a twenty-five-studio co-production as the work of one.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from datetime import time as dtime

from homelab_datalake.useragent import USER_AGENT

# Requested from the API.
#
# `OriginalTitle` and `ProductionLocations` are here because Jellyfin already holds
# them -- it fetched them from TMDB itself when it matched the file. Measured across
# the library: [rate withheld] and [rate withheld] coverage. The same goes for `CommunityRating`, which
# *is* TMDB's vote average. Calling TMDB again for any of these would mean a network
# round trip, an API key and a rate limit, to re-derive what is already on disk.
FIELDS = (
    "Genres,ProviderIds,ProductionYear,PremiereDate,CommunityRating,CriticRating,"
    "OfficialRating,RunTimeTicks,DateCreated,MediaStreams,Studios,Container,"
    "OriginalTitle,ProductionLocations,People,Tags,Taglines,Overview,SortName,"
    "ExternalUrls,RemoteTrailers,Chapters,MediaSources,ParentId"
)

# Fields Jellyfin returns that are deliberately not stored, and are therefore not
# reported as unmapped either. `_unmapped` is a prompt to give a field a column, so a
# field nobody will ever store belongs here rather than in a warning that would then
# fire forever and stop being read.
#
# `Path` is the important one. It is filesystem topology, and in this library it also
# carries the name of the site each file came from -- not something to publish or even
# to keep. The image tags and blur hashes are cache keys for artwork; they identify
# nothing and answer nothing. The rest of MediaSources is transcoding capability, not
# analytics: what is worth keeping from it is pulled out explicitly below.
NOT_STORED = frozenset(
    {
        "Path",
        "MediaSources",
        "ImageTags",
        "ImageBlurHashes",
        "BackdropImageTags",
        "ServerId",
        "LocationType",
        "PlayAccess",
        "ParentBackdropImageTags",
        "ParentBackdropItemId",
        "ParentThumbImageTag",
        "ParentThumbItemId",
        "ScreenshotImageTags",
    }
)

PAGE_SIZE = 500

# A tick is 100 nanoseconds. [count withheld] of them make a second.
TICKS_PER_SECOND = 10_000_000

# Real heights in this library run 1080, 1040, 1036, 816, 808, 804, 800, 720 -- letterboxed
# and cropped encodes, not neat standards. Bucketing on equality would scatter one
# logical resolution across a dozen values and make every "how much of the library is
# HD" answer wrong. Ranges, with the boundaries set below the nominal height so a
# cropped 1080p encode still counts as one.
_RESOLUTION_BANDS = (
    (2000, "4K"),
    (1000, "1080p"),
    (700, "720p"),
    (500, "SD"),
    (0, "low"),
)


# Every key `_title_row`, `_stream_rows` and `_people_rows` actually read. Anything an
# item carries that is neither here nor in NOT_STORED is something Jellyfin has started
# sending that this module does not look at -- which is the signal that went missing
# for the camera's `counts`. Unlike the camera, these rows are built key by key rather
# than projected from the payload, so the comparison has to be made explicitly.
CONSUMED = frozenset(
    {
        "Id", "Name", "OriginalTitle", "SortName", "Overview", "Taglines", "Tags",
        "ProductionYear", "PremiereDate", "DateCreated", "CommunityRating",
        "CriticRating", "OfficialRating", "RunTimeTicks", "Container", "MediaStreams",
        "ProviderIds", "Studios", "ProductionLocations", "Genres", "GenreItems",
        "People", "Chapters", "RemoteTrailers", "ExternalUrls", "ParentId", "Type",
        "MediaType", "VideoType", "HasSubtitles", "IsFolder", "Width", "Height",
        "IndexNumber", "ParentIndexNumber", "CustomRating", "DateLastMediaAdded",
    }
)


@dataclass
class LibrarySnapshot:
    titles: list[dict] = field(default_factory=list)
    genres: list[dict] = field(default_factory=list)
    streams: list[dict] = field(default_factory=list)
    people: list[dict] = field(default_factory=list)
    #: Field names Jellyfin sent that nothing here reads. Reported, never silent.
    unmapped: set[str] = field(default_factory=set)
    total_reported: int = 0
    pages: int = 0

    @property
    def count(self) -> int:
        return len(self.titles)


def resolution_class(height: int | None) -> str | None:
    if not height or height <= 0:
        return None
    for floor, label in _RESOLUTION_BANDS:
        if height >= floor:
            return label
    return None


def _parse_timestamp(value: str | None) -> datetime | None:
    """Jellyfin sends seven-digit fractional seconds; Python accepts at most six."""
    if not value:
        return None
    text = value.replace("Z", "+00:00")
    if "." in text:
        head, _, tail = text.partition(".")
        digits = "".join(c for c in tail if c.isdigit())[:6]
        offset = tail[len(digits):] if len(tail) > len(digits) else ""
        offset = offset.lstrip("0123456789")
        text = f"{head}.{digits:<06s}{offset}"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _stream(streams, kind: str) -> dict:
    """The first stream of a kind. Jellyfin orders them with the default first."""
    for stream in streams or []:
        if stream.get("Type") == kind:
            return stream
    return {}


def _names(entries) -> list[str]:
    """The `Name` of each entry, in the order the API gave them.

    Order is meaningful and is kept: TMDB lists co-production countries and studios
    with the primary one first, which is the only reason taking `[0]` was ever
    defensible. Now the whole list travels and `[0]` is still available to anyone who
    only wants the primary.
    """
    return [n for n in ((e or {}).get("Name") for e in entries or []) if n]


def _stream_rows(item: dict, snapshot_day: date) -> list[dict]:
    """One row per media stream. Subtitle and audio tracks are the point.

    Keeping the first video and the first audio stream and discarding the rest lost
    every subtitle track in the library and every audio track after the first -- so a
    film with French and English audio and thirty-four subtitle tracks was recorded as
    having one of each.
    """
    rows = []
    for stream in item.get("MediaStreams") or []:
        # `Path` on a stream is the external subtitle file on disk, and is dropped for
        # the same reason the item's own Path is.
        rows.append(
            {
                "dt": snapshot_day,
                "item_id": item.get("Id"),
                "stream_index": stream.get("Index"),
                "stream_type": stream.get("Type"),
                "codec": stream.get("Codec"),
                "language": stream.get("Language"),
                "title": stream.get("Title"),
                # Jellyfin's own verdict on the stream -- "1080p H264 SDR". Worth
                # keeping beside the derived resolution_class, which disagrees with it
                # on cropped encodes: a 1080p film letterboxed to [count withheld] rows is called
                # 720p by the band table and 1080p here.
                "display_title": stream.get("DisplayTitle"),
                "is_default": stream.get("IsDefault"),
                "is_forced": stream.get("IsForced"),
                "is_external": stream.get("IsExternal"),
                "is_hearing_impaired": stream.get("IsHearingImpaired"),
                "is_interlaced": stream.get("IsInterlaced"),
                "height": stream.get("Height"),
                "width": stream.get("Width"),
                "channels": stream.get("Channels"),
                "channel_layout": stream.get("ChannelLayout"),
                "bit_rate": stream.get("BitRate"),
                "bit_depth": stream.get("BitDepth"),
                "sample_rate": stream.get("SampleRate"),
                "video_range": stream.get("VideoRange"),
                "video_range_type": stream.get("VideoRangeType"),
                "profile": stream.get("Profile"),
                "level": stream.get("Level"),
            }
        )
    return rows


def _people_rows(item: dict, snapshot_day: date) -> list[dict]:
    """One row per cast or crew credit. Public film metadata, not personal data."""
    rows = []
    for order, person in enumerate(item.get("People") or []):
        rows.append(
            {
                "dt": snapshot_day,
                "item_id": item.get("Id"),
                "person_id": person.get("Id"),
                "name": person.get("Name"),
                "person_type": person.get("Type"),
                "role": person.get("Role"),
                "sort_order": order,
            }
        )
    return rows


def _title_row(item: dict, snapshot_day: date) -> dict:
    video = _stream(item.get("MediaStreams"), "Video")
    audio = _stream(item.get("MediaStreams"), "Audio")
    ticks = item.get("RunTimeTicks") or 0
    height = video.get("Height")
    year = item.get("ProductionYear")

    providers = item.get("ProviderIds") or {}
    sources = item.get("MediaSources") or [{}]
    primary_source = sources[0] if sources else {}
    streams = item.get("MediaStreams") or []
    subtitle_languages = sorted(
        {s.get("Language") for s in streams if s.get("Type") == "Subtitle" and s.get("Language")}
    )
    audio_languages = sorted(
        {s.get("Language") for s in streams if s.get("Type") == "Audio" and s.get("Language")}
    )
    taglines = [t for t in (item.get("Taglines") or []) if t]

    # Path is never stored: filesystem topology, and in this library it also carries
    # the name of the site each file came from. Overview *is* stored now -- it was
    # excluded as "prose no question asks about", which is only true until somebody
    # asks one.
    return {
        "dt": snapshot_day,
        "item_id": item.get("Id"),
        "title": item.get("Name"),
        # The title as released, which differs from `title` whenever Jellyfin is
        # showing a localised name -- this library is largely French-labelled.
        "original_title": item.get("OriginalTitle"),
        "production_year": year,
        "premiere_date": (_parse_timestamp(item.get("PremiereDate")) or datetime.min.replace(tzinfo=UTC)).date()
        if item.get("PremiereDate") else None,
        "added_at": _parse_timestamp(item.get("DateCreated")),
        "community_rating": item.get("CommunityRating"),
        "critic_rating": item.get("CriticRating"),
        "official_rating": item.get("OfficialRating"),
        "runtime_minutes": round(ticks / TICKS_PER_SECOND / 60) if ticks else None,
        "container": item.get("Container"),
        "video_codec": video.get("Codec"),
        "video_height": height,
        "video_width": video.get("Width"),
        "video_bit_depth": video.get("BitDepth"),
        "resolution_class": resolution_class(height),
        "audio_codec": audio.get("Codec"),
        "audio_channels": audio.get("Channels"),
        "tmdb_id": providers.get("Tmdb"),
        "imdb_id": providers.get("Imdb"),
        # Franchise membership. Two films sharing this belong to the same series,
        # which no other field here says.
        "tmdb_collection_id": providers.get("TmdbCollection"),
        "tvdb_id": providers.get("Tvdb"),
        # The primary one stays, for every caller that only wants one, and the whole
        # list travels beside it. A twenty-five-studio co-production was being
        # recorded as the work of one studio.
        "studio": (item.get("Studios") or [{}])[0].get("Name"),
        "studios": _names(item.get("Studios")),
        "production_country": (item.get("ProductionLocations") or [None])[0],
        "production_countries": [c for c in (item.get("ProductionLocations") or []) if c],
        "decade": (year // 10) * 10 if year else None,
        "sort_name": item.get("SortName"),
        "overview": item.get("Overview"),
        "tagline": taglines[0] if taglines else None,
        "taglines": taglines,
        "tags": [t for t in (item.get("Tags") or []) if t],
        "parent_id": item.get("ParentId"),
        "item_type": item.get("Type"),
        "media_type": item.get("MediaType"),
        "video_type": item.get("VideoType"),
        "has_subtitles": item.get("HasSubtitles"),
        "subtitle_languages": subtitle_languages,
        "audio_languages": audio_languages,
        "stream_count": len(streams),
        "chapter_count": len(item.get("Chapters") or []),
        "remote_trailer_count": len(item.get("RemoteTrailers") or []),
        "external_urls": [
            {"name": u.get("Name"), "url": u.get("Url")}
            for u in (item.get("ExternalUrls") or [])
            if u.get("Url")
        ],
        # From MediaSources, which is otherwise transcoding capability rather than
        # analytics. The file size is the one figure a library-growth question needs.
        "file_size_bytes": primary_source.get("Size"),
        "file_bitrate": primary_source.get("Bitrate"),
        "media_source_count": len(sources) if item.get("MediaSources") else 0,
    }


def fetch_page(base_url: str, api_key: str, start_index: int, limit: int,
               include_types: str = "Movie", fields: str = FIELDS) -> dict:
    import urllib.parse
    import urllib.request

    query = urllib.parse.urlencode(
        {
            "Recursive": "true",
            "IncludeItemTypes": include_types,
            "Fields": fields,
            "StartIndex": start_index,
            "Limit": limit,
            "EnableTotalRecordCount": "true",
            "SortBy": "Id",  # stable order, so a re-run reads the same rows
            "SortOrder": "Ascending",
        }
    )
    url = f"{base_url.rstrip('/')}/Items?{query}"
    if not url.startswith(("http://", "https://")):
        raise ValueError(f"JELLYFIN_URL must be http(s), got {base_url!r}")

    import json

    request = urllib.request.Request(  # noqa: S310 - scheme checked immediately above
        url,
        headers={
            "Authorization": f'MediaBrowser Token="{api_key}"',
            # Named explicitly for the same reason as the camera reader: Cloudflare
            # rejects urllib's default agent, and this URL could move behind the proxy.
            "User-Agent": USER_AGENT,
        },
    )
    with urllib.request.urlopen(request, timeout=60) as response:  # noqa: S310
        return json.loads(response.read().decode("utf-8"))


def snapshot(snapshot_day: date, base_url: str | None = None, api_key: str | None = None,
             items: list[dict] | None = None) -> LibrarySnapshot:
    """The whole movie library as of today. `items` is injectable so tests need no server."""
    out = LibrarySnapshot()

    if items is None:
        base_url = base_url or os.environ.get("JELLYFIN_URL", "")
        api_key = api_key or os.environ.get("JELLYFIN_API_KEY", "")
        if not base_url or not api_key:
            raise ValueError("JELLYFIN_URL and JELLYFIN_API_KEY are required")
        items = []
        start = 0
        while True:
            page = fetch_page(base_url, api_key, start, PAGE_SIZE)
            batch = page.get("Items") or []
            out.total_reported = page.get("TotalRecordCount", 0) or out.total_reported
            out.pages += 1
            items.extend(batch)
            start += len(batch)
            if not batch or start >= out.total_reported:
                break
    else:
        out.total_reported = len(items)

    seen_genres: dict[str, None] = {}
    for item in items:
        leftovers = {
            key: value for key, value in item.items()
            if key not in CONSUMED and key not in NOT_STORED
        }
        out.unmapped.update(leftovers)
        row = _title_row(item, snapshot_day)
        row["_unmapped"] = (
            json.dumps(leftovers, default=str, sort_keys=True) if leftovers else None
        )
        out.titles.append(row)
        out.streams.extend(_stream_rows(item, snapshot_day))
        out.people.extend(_people_rows(item, snapshot_day))
        for genre in item.get("Genres") or []:
            name = (genre or "").strip()
            if not name:
                continue
            seen_genres[name] = None
            out.genres.append({"dt": snapshot_day, "item_id": item.get("Id"), "genre": name})

    return out


# --------------------------------------------------------------------------------
# the wider library: series, seasons, episodes, box sets, other videos
# --------------------------------------------------------------------------------
#
# Added 2026-09-14, in the same reversal that added playback: the owner's decision was
# that the warehouse holds everything available, and `snapshot()` above only ever asked
# for Movie. The server was measured on 2026-09-15 to hold Movie [count withheld], Series 325,
# Season [count withheld], Episode [count withheld], BoxSet 186, Video 417 -- roughly [count withheld] items the
# warehouse had never seen.
#
# **`People` is not requested here, and that is a measured decision, not an oversight.**
# A page of 500 Episodes with the same field list `snapshot()` uses for movies took
# 13.29s; the same page with every field held constant except `People` removed took
# [duration withheld] -- thirty times faster. Isolating each field individually confirmed `People`
# alone: [duration withheld] for [count withheld] items, against [duration withheld] each for Studios, ExternalUrls and
# RemoteTrailers. A full combined read of all five non-movie types with `People`
# included measured at [duration withheld] ([count withheld] items, [volume withheld] of JSON); the same read
# with only `People` dropped measured at [duration withheld] for [count withheld] items, [volume withheld]. Cast and crew
# per episode was never asked for -- the brief wants series/season/episode relationships
# and dates, not a credits list per episode -- so the field that costs thirty times the
# rest of the response combined is simply not requested.
LIBRARY_ITEM_TYPES = "Series,Season,Episode,BoxSet,Video"

LIBRARY_ITEM_FIELDS = (
    "Genres,ProviderIds,ProductionYear,PremiereDate,EndDate,CommunityRating,"
    "CriticRating,OfficialRating,RunTimeTicks,DateCreated,MediaStreams,MediaSources,"
    "Studios,Container,OriginalTitle,ProductionLocations,Tags,Taglines,Overview,"
    "SortName,ExternalUrls,RemoteTrailers,ParentId,SeriesId,SeriesName,SeasonId,"
    "SeasonName,IndexNumber,ParentIndexNumber,Status"
)

# `ChildCount` and `RecursiveItemCount` are deliberately not requested: both were tried
# against a real BoxSet and a real Season and both came back null on this server, so
# asking for them would add request weight for a column that would be null throughout.
#
# Image and UI-only fields dropped for the same reason as the movie NOT_STORED list.
# `AirDays` is a series' broadcast schedule -- never asked for, and not requested.
_LIBRARY_ITEM_NOT_STORED = frozenset(
    {
        "Path", "MediaSources", "ImageTags", "ImageBlurHashes", "BackdropImageTags",
        "ServerId", "LocationType", "PlayAccess", "ParentBackdropImageTags",
        "ParentBackdropItemId", "ParentThumbImageTag", "ParentThumbItemId",
        "ScreenshotImageTags", "ChannelId", "AirDays", "SeriesPrimaryImageTag",
        "ParentLogoItemId", "ParentLogoImageTag", "DisplayOrder", "People",
    }
)

_LIBRARY_ITEM_CONSUMED = frozenset(
    {
        "Id", "Name", "OriginalTitle", "SortName", "Overview", "Taglines", "Tags",
        "ProductionYear", "PremiereDate", "EndDate", "Status", "DateCreated",
        "CommunityRating", "CriticRating", "OfficialRating", "RunTimeTicks",
        "Container", "MediaStreams", "ProviderIds", "Studios", "ProductionLocations",
        "Genres", "GenreItems", "RemoteTrailers", "ExternalUrls", "ParentId", "Type",
        "MediaType", "VideoType", "HasSubtitles", "IsFolder", "SeriesId", "SeriesName",
        "SeasonId", "SeasonName", "IndexNumber", "ParentIndexNumber",
    }
)


@dataclass
class LibraryItemsSnapshot:
    items: list[dict] = field(default_factory=list)
    #: Field names Jellyfin sent that nothing here reads. Reported, never silent.
    unmapped: set[str] = field(default_factory=set)
    total_reported: int = 0
    pages: int = 0

    @property
    def count(self) -> int:
        return len(self.items)


def _library_item_row(item: dict, snapshot_day: date) -> dict:
    """One row for a Series, Season, Episode, BoxSet or Video item.

    Deliberately not a second copy of `_title_row`: a Season and a BoxSet are
    containers with no video file of their own, and forcing them through a function
    that assumes one would mean every video/audio column silently reading null for
    reasons that have nothing to do with the field being absent from Jellyfin. What
    genuinely applies to every type -- name, dates, ratings, provider ids, genres,
    the primary studio and country -- is read the same way `_title_row` reads it;
    what only applies to a playable file -- codec, resolution, file size -- comes
    from the same MediaStreams/MediaSources shape when it is there and is null
    otherwise; what only applies to the series hierarchy -- SeriesId, SeasonId, the
    season and episode numbers, a series' own status and end date -- is new here.
    """
    video = _stream(item.get("MediaStreams"), "Video")
    audio = _stream(item.get("MediaStreams"), "Audio")
    ticks = item.get("RunTimeTicks") or 0
    height = video.get("Height")
    year = item.get("ProductionYear")

    providers = item.get("ProviderIds") or {}
    sources = item.get("MediaSources") or [{}]
    primary_source = sources[0] if sources else {}
    streams = item.get("MediaStreams") or []
    taglines = [t for t in (item.get("Taglines") or []) if t]

    return {
        "dt": snapshot_day,
        "item_id": item.get("Id"),
        "item_type": item.get("Type"),
        "title": item.get("Name"),
        "original_title": item.get("OriginalTitle"),
        "production_year": year,
        "premiere_date": (_parse_timestamp(item.get("PremiereDate")) or datetime.min.replace(tzinfo=UTC)).date()
        if item.get("PremiereDate") else None,
        # Only a Series carries this, and only sometimes -- an ongoing show has no
        # end date yet, which is a real state and not a missing one.
        "end_date": (_parse_timestamp(item.get("EndDate")) or datetime.min.replace(tzinfo=UTC)).date()
        if item.get("EndDate") else None,
        # "Continuing" or "Ended", on a Series. Null on everything else.
        "status": item.get("Status"),
        "added_at": _parse_timestamp(item.get("DateCreated")),
        "community_rating": item.get("CommunityRating"),
        "critic_rating": item.get("CriticRating"),
        "official_rating": item.get("OfficialRating"),
        "runtime_minutes": round(ticks / TICKS_PER_SECOND / 60) if ticks else None,
        "container": item.get("Container"),
        "video_codec": video.get("Codec"),
        "video_height": height,
        "video_width": video.get("Width"),
        "resolution_class": resolution_class(height),
        "audio_codec": audio.get("Codec"),
        "audio_channels": audio.get("Channels"),
        "tmdb_id": providers.get("Tmdb"),
        "imdb_id": providers.get("Imdb"),
        "tvdb_id": providers.get("Tvdb"),
        "studio": (item.get("Studios") or [{}])[0].get("Name"),
        "studios": _names(item.get("Studios")),
        "production_country": (item.get("ProductionLocations") or [None])[0],
        "production_countries": [c for c in (item.get("ProductionLocations") or []) if c],
        "genres": [g.strip() for g in (item.get("Genres") or []) if (g or "").strip()],
        "sort_name": item.get("SortName"),
        "overview": item.get("Overview"),
        "tagline": taglines[0] if taglines else None,
        "taglines": taglines,
        "tags": [t for t in (item.get("Tags") or []) if t],
        # The library-folder id on a Series, Movie, BoxSet or standalone Video; the
        # series id on a Season; the season id on an Episode. What it means depends
        # on item_type, which is why series_id/season_id below exist as their own
        # columns rather than asking a reader to know this.
        "parent_id": item.get("ParentId"),
        # Populated on Season and Episode. A Season's own id is its series id here;
        # an Episode carries both its series and its season.
        "series_id": item.get("SeriesId"),
        "series_name": item.get("SeriesName"),
        # Populated on Episode only.
        "season_id": item.get("SeasonId"),
        "season_name": item.get("SeasonName"),
        # Raw as Jellyfin sends them: a season's own number on a Season row, an
        # episode's own number on an Episode row. stg_jellyfin_library_items derives
        # the unambiguous season_number/episode_number columns from these plus
        # item_type -- kept raw here for the same reason every other raw layer in
        # this repo keeps the source's own fields rather than a derived reading.
        "index_number": item.get("IndexNumber"),
        "parent_index_number": item.get("ParentIndexNumber"),
        "stream_count": len(streams),
        "remote_trailer_count": len(item.get("RemoteTrailers") or []),
        "external_urls": [
            {"name": u.get("Name"), "url": u.get("Url")}
            for u in (item.get("ExternalUrls") or [])
            if u.get("Url")
        ],
        "file_size_bytes": primary_source.get("Size"),
        "file_bitrate": primary_source.get("Bitrate"),
        "media_source_count": len(sources) if item.get("MediaSources") else 0,
    }


def library_items_snapshot(snapshot_day: date, base_url: str | None = None,
                           api_key: str | None = None,
                           items: list[dict] | None = None) -> LibraryItemsSnapshot:
    """Series, seasons, episodes, box sets and other videos, as of today.

    `items` is injectable so tests need no server. Movies are not included -- they
    stay on `snapshot()`, unchanged, which is the point: this is an addition, not a
    replacement for the pipeline dim_title_scd2 already depends on.
    """
    out = LibraryItemsSnapshot()

    if items is None:
        base_url = base_url or os.environ.get("JELLYFIN_URL", "")
        api_key = api_key or os.environ.get("JELLYFIN_API_KEY", "")
        if not base_url or not api_key:
            raise ValueError("JELLYFIN_URL and JELLYFIN_API_KEY are required")
        items = []
        start = 0
        while True:
            page = fetch_page(base_url, api_key, start, PAGE_SIZE,
                              include_types=LIBRARY_ITEM_TYPES, fields=LIBRARY_ITEM_FIELDS)
            batch = page.get("Items") or []
            out.total_reported = page.get("TotalRecordCount", 0) or out.total_reported
            out.pages += 1
            items.extend(batch)
            start += len(batch)
            if not batch or start >= out.total_reported:
                break
    else:
        out.total_reported = len(items)

    for item in items:
        leftovers = {
            key: value for key, value in item.items()
            if key not in _LIBRARY_ITEM_CONSUMED and key not in _LIBRARY_ITEM_NOT_STORED
        }
        out.unmapped.update(leftovers)
        row = _library_item_row(item, snapshot_day)
        row["_unmapped"] = (
            json.dumps(leftovers, default=str, sort_keys=True) if leftovers else None
        )
        out.items.append(row)

    return out


# --------------------------------------------------------------------------------
# playback: the Playback Reporting plugin's `submit_custom_query`
# --------------------------------------------------------------------------------


def _sqlite_literal(value: datetime) -> str:
    """A `datetime` formatted the way the plugin's SQLite comparisons expect it.

    `submit_custom_query` takes one raw SQL string with nowhere to bind a parameter --
    the request body *is* the query -- so the safety here has to live in what is
    allowed to reach this function rather than in how the result is escaped. It
    accepts a `datetime` and nothing else: there is no string path in, which is what
    makes it true that the only two values that ever reach this are the partition's
    own hour bounds, computed from the partition key, never from a caller.
    """
    if not isinstance(value, datetime):
        raise TypeError(f"expected a datetime, got {type(value).__name__}")
    return value.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")


# The exact column list asked for, in this order, and checked against what comes
# back. `submit_custom_query` runs the SQL text as written and the plugin answers
# with whatever `res.get("colums")` says -- misspelled in the API itself, not here --
# so reading fields out by position without checking that list first would silently
# swap two columns the day a plugin update reorders them, rather than fail loudly.
_PLAYBACK_COLUMNS = (
    "DateCreated", "UserId", "ItemId", "ItemType", "ItemName",
    "PlaybackMethod", "ClientName", "DeviceName", "PlayDuration",
)

_RESULT_COLUMNS_KEY = "colums"  # the plugin's own spelling, not a typo here


def _playback_query(start: datetime, end: datetime) -> str:
    """SQL text for one half-open `[start, end)` window over `PlaybackActivity`."""
    columns = ", ".join(_PLAYBACK_COLUMNS)
    return (
        f"SELECT {columns} FROM PlaybackActivity "  # noqa: S608 - see _sqlite_literal
        f"WHERE datetime(DateCreated) >= datetime('{_sqlite_literal(start)}') "
        f"AND datetime(DateCreated) < datetime('{_sqlite_literal(end)}') "
        "ORDER BY DateCreated"
    )


def fetch_playback_query(base_url: str, api_key: str, query: str) -> dict:
    import urllib.request

    url = f"{base_url.rstrip('/')}/user_usage_stats/submit_custom_query"
    if not url.startswith(("http://", "https://")):
        raise ValueError(f"JELLYFIN_URL must be http(s), got {base_url!r}")
    body = json.dumps({"CustomQueryString": query, "ReplaceUserId": False}).encode()
    request = urllib.request.Request(  # noqa: S310 - scheme checked immediately above
        url,
        data=body,
        headers={
            "Authorization": f'MediaBrowser Token="{api_key}"',
            "Content-Type": "application/json",
            "User-Agent": USER_AGENT,
        },
    )
    with urllib.request.urlopen(request, timeout=60) as response:  # noqa: S310
        return json.loads(response.read().decode("utf-8"))


def _parse_playback_timestamp(value) -> datetime | None:
    """`DateCreated` from the plugin's SQLite table. Always UTC on this server.

    Verified on 2026-09-14 by lining a PlaybackActivity row up against the standard
    API's activity log, which *is* UTC and says so with a `Z`. One example, of several
    checked: the activity log timed a session `viewer` played "Example Movie" from
    `2026-09-12T18:00:01.6169626Z` to `2026-09-12T19:52:43.1572661Z` -- [duration withheld]
    by the wall clock. The matching PlaybackActivity row has
    `DateCreated = '2026-09-12 18:00:21.6180609'`, twenty seconds after the activity
    log's own start and in the *same* hour, not the two hours away a Europe/Paris
    local-time reading would put it in September (UTC+2). The plugin is documented to
    sometimes store server-local time; on this installation, checked rather than
    assumed, it does not.

    No zone suffix ever arrives on this value, which is why it gets its own function
    instead of reusing `_parse_timestamp` above: that one's other input, Jellyfin's
    own API, really is UTC but marks it with `Z`, and defaulting a naive read to
    system-local time would be the wrong default specifically for this source.
    """
    if not value:
        return None
    text = str(value).replace(" ", "T", 1)
    if "." in text:
        head, _, tail = text.partition(".")
        digits = "".join(c for c in tail if c.isdigit())[:6]
        text = f"{head}.{digits:<06s}"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed.replace(tzinfo=UTC)


def _parse_duration_seconds(value) -> int | None:
    """`PlayDuration` as whole seconds.

    Verified alongside the timestamp above, on the same Example Movie session: an elapsed
    [duration withheld] by the activity log's own start and stop produced
    `PlayDuration = 6761`. Seconds, not the `H:MM:SS` string some Playback Reporting
    installs are known to emit for this column -- checked, not assumed, here too.
    """
    if value is None:
        return None
    try:
        return round(float(value))
    except (TypeError, ValueError):
        return None


# Verified against this installation on 2026-09-14: every `Episode` row's `ItemName`
# follows Jellyfin's own display convention, "<series> - sNNeNN - <episode title>" --
# checked across "1883", "1899", "24 Heures chrono", "A Knight of the Seven Kingdoms"
# and "American Horror Story", all matching exactly. This is the join the mart uses
# for an episode's series, and it needs no extra API call: the library snapshot never
# requests series or episode items (`IncludeItemTypes=Movie` in FIELDS above), so
# there is no series-grain row anywhere in the warehouse to look a real `SeriesId` up
# against even if the plugin offered one on this query, which it does not -- the
# query above only carries the episode's own `ItemId`.
_SERIES_PATTERN = re.compile(r"^(?P<series>.+?) - s\d{2}e\d{2} - .+$")


def parse_series_name(item_name: str | None, item_type: str | None) -> str | None:
    if item_type != "Episode" or not item_name:
        return None
    match = _SERIES_PATTERN.match(item_name)
    return match.group("series") if match else None


def parse_playback_rows(payload: dict, users_by_id_map: dict[str, str]) -> list[dict]:
    """The plugin's `{colums, results, message}` shape, turned into named rows.

    `message` is non-empty on a genuine query failure, and also on the legitimate
    "nothing in this window" case -- the plugin's own wording for that is "query
    executed, no data returned", which must not be treated as an error.
    """
    columns = [str(c) for c in (payload.get(_RESULT_COLUMNS_KEY) or [])]
    results = payload.get("results") or []
    message = str(payload.get("message") or "")

    if not results and message.lower().startswith("query executed, no data returned"):
        return []
    if message:
        raise ValueError(f"Playback Reporting query failed: {message}")
    if columns and columns != list(_PLAYBACK_COLUMNS):
        raise ValueError(
            f"Playback Reporting returned columns {columns}, expected "
            f"{list(_PLAYBACK_COLUMNS)} -- the query or the plugin's schema changed"
        )

    index = {name: i for i, name in enumerate(_PLAYBACK_COLUMNS)}
    rows = []
    for result in results:
        user_id = result[index["UserId"]]
        item_name = result[index["ItemName"]]
        item_type = result[index["ItemType"]]
        rows.append(
            {
                "started_at": _parse_playback_timestamp(result[index["DateCreated"]]),
                "user_id": user_id,
                "user_name": users_by_id_map.get(user_id),
                "item_id": result[index["ItemId"]],
                "item_type": item_type,
                "item_name": item_name,
                "series_name": parse_series_name(item_name, item_type),
                "playback_method": result[index["PlaybackMethod"]],
                "client_name": result[index["ClientName"]],
                "device_name": result[index["DeviceName"]],
                "play_duration_seconds": _parse_duration_seconds(result[index["PlayDuration"]]),
            }
        )
    return rows


def playback_window(start: datetime, end: datetime, base_url: str | None = None,
                    api_key: str | None = None, users_by_id_map: dict[str, str] | None = None,
                    payload: dict | None = None) -> list[dict]:
    """Every PlaybackActivity row in `[start, end)`. `payload` is injectable for tests."""
    if payload is None:
        base_url = base_url or os.environ.get("JELLYFIN_URL", "")
        api_key = api_key or os.environ.get("JELLYFIN_API_KEY", "")
        if not base_url or not api_key:
            raise ValueError("JELLYFIN_URL and JELLYFIN_API_KEY are required")
        if users_by_id_map is None:
            users_by_id_map = users_by_id(fetch_users(base_url, api_key))
        payload = fetch_playback_query(base_url, api_key, _playback_query(start, end))
    return parse_playback_rows(payload, users_by_id_map or {})


# --------------------------------------------------------------------------------
# the account list: `/Users`
# --------------------------------------------------------------------------------

# Everything this reads from a `/Users` object.
_USER_CONSUMED = frozenset({"Id", "Name", "Policy", "LastLoginDate", "LastActivityDate"})

# Deliberately dropped, and therefore never reported as unmapped: authentication
# state (`HasPassword`, `HasConfiguredPassword`, `HasConfiguredEasyPassword`,
# `EnableAutoLogin`), the id of the *server* rather than the account, and the two
# large blocks -- `Configuration` (playback and UI preferences) and the rest of
# `Policy` beyond the two flags actually asked for. The instruction behind this list
# was explicit: nothing from configuration or password policy beyond admin and
# disabled.
_USER_NOT_STORED = frozenset(
    {"HasPassword", "HasConfiguredPassword", "HasConfiguredEasyPassword",
     "EnableAutoLogin", "ServerId", "Configuration"}
)


def user_rows(raw_users: list[dict], snapshot_day: date) -> tuple[list[dict], set[str]]:
    """`/Users` turned into the account snapshot. `Policy` is read from, not kept."""
    rows = []
    unmapped: set[str] = set()
    for user in raw_users:
        leftovers = {
            key: value for key, value in user.items()
            if key not in _USER_CONSUMED and key not in _USER_NOT_STORED
        }
        unmapped.update(leftovers)
        policy = user.get("Policy") or {}
        rows.append(
            {
                "dt": snapshot_day,
                "id": user.get("Id"),
                "name": user.get("Name"),
                "is_administrator": policy.get("IsAdministrator"),
                "is_disabled": policy.get("IsDisabled"),
                "last_login": _parse_timestamp(user.get("LastLoginDate")),
                "last_activity": _parse_timestamp(user.get("LastActivityDate")),
            }
        )
    return rows, unmapped


def users_by_id(raw_users: list[dict]) -> dict[str, str]:
    """id -> name, for resolving playback's raw `UserId` without a second endpoint.

    `/user_usage_stats/user_list` offers the same mapping -- measured to agree with
    `/Users` exactly on this installation -- but `/Users` is used because
    `jellyfin_users_raw` already has to fetch it for the account snapshot, and a
    second endpoint for the same fourteen names would be a second thing to keep
    working.
    """
    return {u.get("Id"): u.get("Name") for u in raw_users if u.get("Id")}


def fetch_users(base_url: str, api_key: str) -> list[dict]:
    import urllib.request

    url = f"{base_url.rstrip('/')}/Users"
    if not url.startswith(("http://", "https://")):
        raise ValueError(f"JELLYFIN_URL must be http(s), got {base_url!r}")
    request = urllib.request.Request(  # noqa: S310 - scheme checked immediately above
        url,
        headers={
            "Authorization": f'MediaBrowser Token="{api_key}"',
            "User-Agent": USER_AGENT,
        },
    )
    with urllib.request.urlopen(request, timeout=60) as response:  # noqa: S310
        return json.loads(response.read().decode("utf-8"))


# --------------------------------------------------------------------------------
# the server activity log: `/System/ActivityLog/Entries`
# --------------------------------------------------------------------------------

ACTIVITY_PAGE_SIZE = 200

_ACTIVITY_FIELDS = frozenset(
    {"Id", "Date", "Type", "Name", "Overview", "ShortOverview", "UserId", "ItemId", "Severity"}
)


def fetch_activity_page(base_url: str, api_key: str, start_index: int, limit: int,
                        min_date: datetime) -> dict:
    import urllib.parse
    import urllib.request

    query = urllib.parse.urlencode(
        {
            "startIndex": start_index,
            "limit": limit,
            "minDate": min_date.astimezone(UTC).isoformat().replace("+00:00", "Z"),
        }
    )
    url = f"{base_url.rstrip('/')}/System/ActivityLog/Entries?{query}"
    if not url.startswith(("http://", "https://")):
        raise ValueError(f"JELLYFIN_URL must be http(s), got {base_url!r}")
    request = urllib.request.Request(  # noqa: S310 - scheme checked immediately above
        url,
        headers={
            "Authorization": f'MediaBrowser Token="{api_key}"',
            "User-Agent": USER_AGENT,
        },
    )
    with urllib.request.urlopen(request, timeout=60) as response:  # noqa: S310
        return json.loads(response.read().decode("utf-8"))


def _activity_row(entry: dict, snapshot_day: date) -> dict:
    return {
        "dt": snapshot_day,
        "id": entry.get("Id"),
        "date": _parse_timestamp(entry.get("Date")),
        "type": entry.get("Type"),
        "name": entry.get("Name"),
        "overview": entry.get("Overview"),
        "short_overview": entry.get("ShortOverview"),
        "user_id": entry.get("UserId"),
        "item_id": entry.get("ItemId"),
        "severity": entry.get("Severity"),
    }


def activity_window(day: date, base_url: str | None = None, api_key: str | None = None,
                    pages: list[list[dict]] | None = None) -> tuple[list[dict], set[str]]:
    """One day of the activity log, read newest-first and stopped at the day's start.

    The endpoint takes `minDate` and a `startIndex`/`limit` page, and returns entries
    newest first with no way to ask for an upper bound. So landing day *D* means
    paging from "now" down through every entry newer than *D* before reaching it --
    cheap for the daily schedule, which only ever asks for yesterday, and a one-off
    cost for a backfill of an old day. There is no cheaper path through an API that
    offers a floor and no ceiling.

    `pages` is injectable -- already-paginated `Items` batches, oldest page last -- so
    tests exercise the stopping condition directly, with no server.
    """
    start = datetime.combine(day, dtime.min, tzinfo=UTC)
    end = start + timedelta(days=1)

    rows: list[dict] = []
    unmapped: set[str] = set()

    def consume(items: list[dict]) -> None:
        for entry in items:
            leftovers = {key: value for key, value in entry.items() if key not in _ACTIVITY_FIELDS}
            unmapped.update(leftovers)
            parsed_date = _parse_timestamp(entry.get("Date"))
            if parsed_date is not None and start <= parsed_date < end:
                rows.append(_activity_row(entry, day))

    if pages is not None:
        for page in pages:
            consume(page)
        return rows, unmapped

    if not base_url:
        raise ValueError("JELLYFIN_URL is required")
    start_index = 0
    while True:
        payload = fetch_activity_page(base_url, api_key, start_index, ACTIVITY_PAGE_SIZE, start)
        items = payload.get("Items") or []
        if not items:
            break
        consume(items)
        start_index += len(items)
        if len(items) < ACTIVITY_PAGE_SIZE:
            break
    return rows, unmapped
