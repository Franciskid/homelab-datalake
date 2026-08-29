"""TMDB enrichment: only the four facts Jellyfin genuinely does not have.

This module started much larger and was cut down, because most of it was re-fetching
data the library already held. Jellyfin queries TMDB itself when it matches a file, so
`CommunityRating` **is** TMDB's vote average, and the original title, production
country and release date all come back from Jellyfin at 99.8-[rate withheld] coverage. Asking
TMDB for those again would mean an API key, a rate limit and a network round trip to
re-derive what is already on disk -- and every one of those columns would sit null
until somebody created an account, for no reason at all.

What Jellyfin has no field for, and only these:

    budget · revenue · popularity · vote_count · original_language

Those are what turn a catalogue into something answerable -- does the 4K shelf skew to
expensive films, is the collection mostly anglophone -- and they are the only reason
this module still exists.

**Optional, exactly like geo enrichment**, and for the same reason. TMDB requires a
free account, so without a key every column comes back null and the pipeline runs
unchanged. CI never needs a credential, which is what lets this repo be cloned and
built by someone who is not the author.

The lookup is a key lookup, not a search. [rate withheld] of titles carry a TMDB id already --
Jellyfin stored it when it matched the file -- so there is no fuzzy title matching
here and no chance of silently enriching a film with another film's box office. A
title with no id is left null rather than guessed at.
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass

from homelab_datalake.useragent import USER_AGENT

logger = logging.getLogger(__name__)

API_ROOT = "https://api.themoviedb.org/3"

# TMDB's documented limit is around [count withheld] requests/second. This is well under it: the
# whole library is ~[count withheld] titles and runs once, then only for titles not yet enriched,
# so there is nothing to gain from going faster and a rate-limit ban to lose.
REQUESTS_PER_SECOND = 20
_MIN_INTERVAL = 1.0 / REQUESTS_PER_SECOND

# Retried rather than failed: 429 is a rate limit and 5xx is TMDB having a moment.
# Neither says anything about the title being asked for.
_RETRYABLE = frozenset({429, 500, 502, 503, 504})
MAX_ATTEMPTS = 4


@dataclass(frozen=True)
class TmdbFacts:
    """What is kept. Deliberately a short list, and shorter than it first was.

    No overview, no tagline, no poster path, no cast: prose and images are not what a
    warehouse is for, and they are the bulk of the payload. No vote average, original
    title, release date or production country either -- Jellyfin already has all four,
    so keeping them here would mean two columns for one fact and a needless API key.
    """

    tmdb_id: int
    original_language: str | None
    budget_usd: int | None
    revenue_usd: int | None
    popularity: float | None
    vote_count: int | None


def _positive_or_none(value) -> int | None:
    """TMDB reports an unknown budget or revenue as 0, not as null.

    Kept apart because the difference matters: a film that genuinely earned nothing
    and a film nobody recorded earnings for are not the same row, and averaging them
    together understates every box-office question asked of this table.
    """
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def parse_movie(payload: dict) -> TmdbFacts:
    """Project one TMDB movie response onto the columns kept."""
    return TmdbFacts(
        tmdb_id=int(payload["id"]),
        original_language=payload.get("original_language") or None,
        budget_usd=_positive_or_none(payload.get("budget")),
        revenue_usd=_positive_or_none(payload.get("revenue")),
        popularity=payload.get("popularity"),
        # The count, not the average: the average is already in the library as
        # `community_rating`, and the count is what says whether to trust it.
        vote_count=payload.get("vote_count"),
    )


class TmdbClient:
    """A polite, rate-limited client for one endpoint.

    `fetch` is injectable so the tests exercise the parsing, the pacing and the
    retry logic without a key and without the network.
    """

    def __init__(self, api_key: str, fetch=None) -> None:
        self.api_key = api_key
        self._fetch = fetch or self._http_get
        self._last_request = 0.0

    def _pace(self) -> None:
        elapsed = time.monotonic() - self._last_request
        if elapsed < _MIN_INTERVAL:
            time.sleep(_MIN_INTERVAL - elapsed)
        self._last_request = time.monotonic()

    def _http_get(self, url: str) -> dict | None:
        request = urllib.request.Request(  # noqa: S310 - url is built from API_ROOT
            url, headers={"Accept": "application/json", "User-Agent": USER_AGENT}
        )
        with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310
            return json.loads(response.read().decode("utf-8"))

    def movie(self, tmdb_id: int) -> TmdbFacts | None:
        """One film, or None if TMDB does not have it.

        A 404 is a real answer -- an id Jellyfin holds that TMDB has since removed or
        merged -- so it returns None rather than raising. Anything else is retried,
        and then raised, because a whole run failing is better than a run that
        silently enriched two thirds of the library.
        """
        query = urllib.parse.urlencode({"api_key": self.api_key})
        url = f"{API_ROOT}/movie/{int(tmdb_id)}?{query}"

        for attempt in range(1, MAX_ATTEMPTS + 1):
            self._pace()
            try:
                payload = self._fetch(url)
            except urllib.error.HTTPError as error:
                if error.code == 404:
                    logger.debug("tmdb has no movie %s", tmdb_id)
                    return None
                if error.code in _RETRYABLE and attempt < MAX_ATTEMPTS:
                    # Exponential, because a rate limit that is being hit is made
                    # worse by retrying at the same rate.
                    time.sleep(2 ** attempt * _MIN_INTERVAL)
                    continue
                raise
            if payload is None:
                return None
            return parse_movie(payload)
        return None


def client_from_env() -> TmdbClient | None:
    """A client, or None when no key is configured. None is a supported state."""
    key = os.environ.get("TMDB_API_KEY", "").strip()
    if not key:
        logger.info("TMDB_API_KEY is not set; library enrichment columns will be null")
        return None
    return TmdbClient(api_key=key)
