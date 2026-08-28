"""TMDB enrichment: the facts Jellyfin does not have.

Jellyfin knows what is on the disk -- codec, resolution, runtime, the rating the
community gave it. It does not know what the film cost, what it earned, what language
it was shot in, or how many people rated it. Those are the columns that turn a
catalogue into something you can ask questions of: does the 4K shelf skew towards
recent blockbusters, is the collection mostly anglophone, does budget predict anything
about what actually gets watched.

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

logger = logging.getLogger(__name__)

API_ROOT = "https://api.themoviedb.org/3"
USER_AGENT = "homelab-datalake/1.0 (+https://data.example.test)"

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
    """What is kept. Deliberately a short list.

    No overview, no tagline, no poster path, no cast. Prose and images are not what a
    warehouse is for, they are the bulk of the payload, and an overview column would
    be a free-text field flowing into a system whose whole design is that free text
    does not.
    """

    tmdb_id: int
    original_language: str | None
    original_title: str | None
    release_date: str | None
    budget_usd: int | None
    revenue_usd: int | None
    popularity: float | None
    vote_average: float | None
    vote_count: int | None
    is_adult: bool | None
    status: str | None
    production_country: str | None


def _first_country(payload: dict) -> str | None:
    countries = payload.get("production_countries") or []
    if not countries:
        return None
    return countries[0].get("iso_3166_1")


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
        original_title=payload.get("original_title") or None,
        release_date=payload.get("release_date") or None,
        budget_usd=_positive_or_none(payload.get("budget")),
        revenue_usd=_positive_or_none(payload.get("revenue")),
        popularity=payload.get("popularity"),
        vote_average=payload.get("vote_average"),
        # A vote average over zero votes is 0.0, which is not a rating of zero.
        vote_count=payload.get("vote_count"),
        is_adult=payload.get("adult"),
        status=payload.get("status") or None,
        production_country=_first_country(payload),
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
