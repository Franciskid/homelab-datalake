"""Extracting the media library from Jellyfin's API.

A full snapshot per day. At ~[count withheld] titles that is one paginated read taking under a
second, and snapshots are what SCD2 wants anyway -- an incremental sync would be more
code, more state, and no faster.

Reading the API rather than the CSV export matters for one concrete reason:
`ProviderIds.Tmdb` comes back as a real identifier on **[rate withheld]** of titles. TMDB
enrichment is then a key lookup instead of fuzzy title matching, which is what gets
remakes and translated titles wrong.

Deliberately not requested: `/Sessions` and `/Users`. Playback history is personal and
adds nothing the metadata does not. `Path` and `Overview` are requested by the field
list but dropped here -- see `_title_row`.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import UTC, date, datetime

# Requested from the API. `Path` is included because Jellyfin will not return
# MediaSources without it on some versions, and it is discarded below rather than
# stored -- see the note in `_title_row`.
FIELDS = (
    "Genres,ProviderIds,ProductionYear,PremiereDate,CommunityRating,CriticRating,"
    "OfficialRating,RunTimeTicks,DateCreated,MediaStreams,Studios,Container"
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


@dataclass
class LibrarySnapshot:
    titles: list[dict] = field(default_factory=list)
    genres: list[dict] = field(default_factory=list)
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


def _title_row(item: dict, snapshot_day: date) -> dict:
    video = _stream(item.get("MediaStreams"), "Video")
    audio = _stream(item.get("MediaStreams"), "Audio")
    providers = item.get("ProviderIds") or {}
    ticks = item.get("RunTimeTicks") or 0
    height = video.get("Height")
    year = item.get("ProductionYear")

    # Path and Overview are never stored. Path is filesystem topology -- and in this
    # library it also carries the name of the site each file came from, which is not
    # something to publish or even to keep. Overview is a paragraph of prose that no
    # question here asks about.
    return {
        "dt": snapshot_day,
        "item_id": item.get("Id"),
        "title": item.get("Name"),
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
        "studio": (item.get("Studios") or [{}])[0].get("Name"),
        "decade": (year // 10) * 10 if year else None,
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
            "User-Agent": "homelab-datalake/1.0 (+https://data.example.test)",
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
        out.titles.append(_title_row(item, snapshot_day))
        for genre in item.get("Genres") or []:
            name = (genre or "").strip()
            if not name:
                continue
            seen_genres[name] = None
            out.genres.append({"dt": snapshot_day, "item_id": item.get("Id"), "genre": name})

    return out
