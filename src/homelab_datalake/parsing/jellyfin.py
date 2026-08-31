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

Deliberately not requested: `/Sessions` and `/Users`. Playback history is personal and
adds nothing the metadata does not. `Path` is requested and dropped -- see `NOT_STORED`.

Three objects per snapshot became five. A title has many genres, many media streams and
many people, and the honest shape for each is a row rather than a column: keeping only
the first audio stream described a bilingual film as monolingual, and keeping only the
first studio described a twenty-five-studio co-production as the work of one.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import UTC, date, datetime

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


def fetch_page(base_url: str, api_key: str, start_index: int, limit: int) -> dict:
    import urllib.parse
    import urllib.request

    query = urllib.parse.urlencode(
        {
            "Recursive": "true",
            "IncludeItemTypes": "Movie",
            "Fields": FIELDS,
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
