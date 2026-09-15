"""Which tables of Radarr, Sonarr and Jellyseerr the lake keeps, and what it leaves out.

The request-to-download path of every title: a Jellyseerr request, the grab Radarr or
Sonarr made for it, the import, and the files that ended up on disk -- with the
lookups that give those ids names. One ledger per table; see parsing/ledger.py.

**Left out, by table and by column, because they authenticate:** indexer and download
client `Settings` (API keys, passwords), Jellyseerr's `password`, `plexToken`,
`jellyfinAuthToken`, `resetPasswordGuid`, its `user_settings` notification tokens, and
`user_push_subscription` and `session` entirely. Credential query parameters inside
history URLs are redacted by `ledger.redact_secrets`. Personal data is otherwise kept as
the source holds it: the owner decided on 2026-09-15 that privacy handling comes later.

**Volatile columns** change on every metadata refresh or search without the title
changing, and would otherwise mint a new version of every row every day.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class TableSpec:
    app: str
    source_table: str
    name: str
    description: str
    exclude: frozenset[str] = field(default_factory=frozenset)
    volatile: frozenset[str] = field(default_factory=frozenset)
    redact: bool = False

    @property
    def table_name(self) -> str:
        return f"{self.app}_{self.name}"


def _spec(app, source_table, name, description, exclude=(), volatile=(), redact=False):
    return TableSpec(app, source_table, name, description, frozenset(exclude), frozenset(volatile), redact)


_ARR_SECRETS = ("Settings",)

RADARR: tuple[TableSpec, ...] = (
    _spec("radarr", "History", "history",
          "Every grab, import, deletion and failure Radarr recorded, with release and quality.",
          redact=True),
    _spec("radarr", "DownloadHistory", "download_history",
          "Each download's lifecycle in the download client: grabbed, imported, failed, ignored.",
          redact=True),
    _spec("radarr", "Blocklist", "blocklist", "Releases Radarr refused to grab again, and why.",
          redact=True),
    _spec("radarr", "Movies", "movies", "The films Radarr manages: monitoring, quality profile, when added.",
          volatile=("LastSearchTime",)),
    _spec("radarr", "MovieMetadata", "movie_metadata",
          "TMDB/IMDb ids, titles, release dates, runtime, studio and genres of each film.",
          volatile=("LastInfoSync", "Popularity", "Ratings", "Recommendations", "Images")),
    _spec("radarr", "MovieFiles", "movie_files",
          "The file on disk for each film: size, quality, languages, release group, media info."),
    _spec("radarr", "QualityProfiles", "quality_profiles", "The quality profiles history rows point at."),
    _spec("radarr", "Indexers", "indexers", "Configured indexers. Settings (API keys) are never read.",
          exclude=_ARR_SECRETS),
    _spec("radarr", "DownloadClients", "download_clients",
          "Configured download clients. Settings (passwords) are never read.", exclude=_ARR_SECRETS),
    _spec("radarr", "Tags", "tags", "Tag labels by id."),
    _spec("radarr", "Collections", "collections", "Film collections Radarr tracks."),
)

SONARR: tuple[TableSpec, ...] = (
    _spec("sonarr", "History", "history",
          "Every grab, import, deletion and failure Sonarr recorded, per episode.", redact=True),
    _spec("sonarr", "DownloadHistory", "download_history",
          "Each download's lifecycle in the download client.", redact=True),
    _spec("sonarr", "Blocklist", "blocklist", "Releases Sonarr refused to grab again, and why.",
          redact=True),
    _spec("sonarr", "Series", "series", "The shows Sonarr manages: ids, network, status, seasons.",
          volatile=("LastInfoSync", "LastDiskSync", "Ratings", "Images", "NextAiring")),
    _spec("sonarr", "Episodes", "episodes", "Every episode: numbering, air date, whether a file exists.",
          volatile=("LastSearchTime", "Ratings", "Images")),
    _spec("sonarr", "EpisodeFiles", "episode_files",
          "The file on disk for each episode: size, quality, languages, release group, media info."),
    _spec("sonarr", "QualityProfiles", "quality_profiles", "The quality profiles history rows point at."),
    _spec("sonarr", "Indexers", "indexers", "Configured indexers. Settings (API keys) are never read.",
          exclude=_ARR_SECRETS),
    _spec("sonarr", "DownloadClients", "download_clients",
          "Configured download clients. Settings (passwords) are never read.", exclude=_ARR_SECRETS),
    _spec("sonarr", "Tags", "tags", "Tag labels by id."),
)

JELLYSEERR: tuple[TableSpec, ...] = (
    _spec("jellyseerr", "media_request", "requests",
          "Every request: who asked, for what, 4K or not, and each status it passed through."),
    _spec("jellyseerr", "season_request", "season_requests", "The seasons a TV request asked for."),
    _spec("jellyseerr", "media", "media",
          "Each title Jellyseerr knows: TMDB/TVDB ids, availability status, when it became available.",
          volatile=("updatedAt", "lastSeasonChange")),
    _spec("jellyseerr", "season", "seasons", "Per-season availability of TV titles.",
          volatile=("updatedAt",)),
    _spec("jellyseerr", "user", "users",
          "Jellyseerr accounts and quotas. Password, reset token and Plex/Jellyfin tokens are never read.",
          exclude=("password", "plexToken", "jellyfinAuthToken", "resetPasswordGuid")),
    _spec("jellyseerr", "issue", "issues", "Problems reported against a title."),
    _spec("jellyseerr", "issue_comment", "issue_comments", "Comments on reported problems."),
    _spec("jellyseerr", "blocklist", "blocklist", "Titles hidden from discovery."),
    _spec("jellyseerr", "watchlist", "watchlist", "Titles users put on their watchlist."),
)

APPS: dict[str, tuple[TableSpec, ...]] = {
    "radarr": RADARR,
    "sonarr": SONARR,
    "jellyseerr": JELLYSEERR,
}

DB_PATH_ENV = {
    "radarr": ("RADARR_DB_PATH", "/sources/radarr/radarr.db"),
    "sonarr": ("SONARR_DB_PATH", "/sources/sonarr/sonarr.db"),
    "jellyseerr": ("JELLYSEERR_DB_PATH", "/sources/jellyseerr/db.sqlite3"),
}

ALL_TABLES: tuple[TableSpec, ...] = RADARR + SONARR + JELLYSEERR
