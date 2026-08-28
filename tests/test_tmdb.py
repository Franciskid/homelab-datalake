"""TMDB enrichment: the parsing, the zero-versus-null distinction, and the retries.

No key and no network needed. The client takes an injectable fetch, which is the
whole reason these paths are testable at all -- including the ones that only happen
when TMDB is having a bad day.
"""

import urllib.error

import pytest

from homelab_datalake.parsing import tmdb

# A response shaped like TMDB's, with the fields kept and several that are not.
MOVIE = {
    "id": 27205,
    "adult": False,
    "original_language": "en",
    "original_title": "Inception",
    "release_date": "2010-07-15",
    "budget": 160000000,
    "revenue": 825532764,
    "popularity": 42.5,
    "vote_average": 8.4,
    "vote_count": 34000,
    "status": "Released",
    "production_countries": [
        {"iso_3166_1": "GB", "name": "United Kingdom"},
        {"iso_3166_1": "US", "name": "United States of America"},
    ],
    # Present in the payload, deliberately not kept.
    "overview": "A thief who steals corporate secrets...",
    "poster_path": "/edv5CZvWj09upOsy2Y6IwDhK8bt.jpg",
    "tagline": "Your mind is the scene of the crime.",
}


def test_the_kept_fields_are_parsed():
    facts = tmdb.parse_movie(MOVIE)
    assert facts.tmdb_id == 27205
    assert facts.original_language == "en"
    assert facts.budget_usd == 160000000
    assert facts.revenue_usd == 825532764
    assert facts.vote_count == 34000
    assert facts.production_country == "GB"  # the first, deterministically


def test_prose_and_images_are_not_kept():
    """A warehouse is not the place for free text, and it is most of the payload."""
    facts = tmdb.parse_movie(MOVIE)
    stored = vars(facts).values()
    assert not any(
        isinstance(v, str) and ("thief" in v or v.endswith(".jpg")) for v in stored
    )
    assert not hasattr(facts, "overview")


@pytest.mark.parametrize("value", [0, "0", None, "", "not a number"])
def test_an_unknown_budget_is_null_not_zero(value):
    """TMDB reports an unknown budget as 0, and averaging that in understates everything."""
    facts = tmdb.parse_movie({**MOVIE, "budget": value})
    assert facts.budget_usd is None


def test_a_real_zero_stays_distinguishable():
    """Nothing legitimately has a budget of exactly zero, so 0 always means unknown."""
    assert tmdb._positive_or_none(0) is None
    assert tmdb._positive_or_none(1) == 1


def test_a_film_with_no_country_does_not_raise():
    assert tmdb.parse_movie({**MOVIE, "production_countries": []}).production_country is None
    assert tmdb.parse_movie({k: v for k, v in MOVIE.items()
                             if k != "production_countries"}).production_country is None


# --- the client -----------------------------------------------------------------


def _error(code):
    return urllib.error.HTTPError("u", code, "err", {}, None)


def test_a_missing_film_is_an_answer_not_a_failure():
    """An id Jellyfin holds that TMDB has since removed or merged."""
    client = tmdb.TmdbClient("k", fetch=lambda _url: (_ for _ in ()).throw(_error(404)))
    assert client.movie(27205) is None


def test_a_rate_limit_is_retried():
    calls = []

    def flaky(url):
        calls.append(url)
        if len(calls) < 3:
            raise _error(429)
        return MOVIE

    client = tmdb.TmdbClient("k", fetch=flaky)
    assert client.movie(27205).tmdb_id == 27205
    assert len(calls) == 3


def test_a_permanent_error_is_raised_rather_than_swallowed():
    """A run that half-enriched the library silently is worse than one that failed."""
    client = tmdb.TmdbClient("k", fetch=lambda _url: (_ for _ in ()).throw(_error(401)))
    with pytest.raises(urllib.error.HTTPError):
        client.movie(27205)


def test_the_key_is_never_part_of_the_path():
    """A key in the path would end up in somebody's access log; a query string is scrubbed."""
    seen = []
    client = tmdb.TmdbClient("secret-key", fetch=lambda url: seen.append(url) or MOVIE)
    client.movie(27205)
    path = seen[0].split("?")[0]
    assert "secret-key" not in path
    assert path.endswith("/movie/27205")


def test_an_id_is_coerced_to_an_integer_before_it_reaches_a_url():
    seen = []
    client = tmdb.TmdbClient("k", fetch=lambda url: seen.append(url) or MOVIE)
    with pytest.raises(ValueError):
        client.movie("27205/../../delete")
    assert not seen


def test_no_key_means_no_client_rather_than_a_crash(monkeypatch):
    """The supported state: enrichment is optional, exactly like geo."""
    monkeypatch.delenv("TMDB_API_KEY", raising=False)
    assert tmdb.client_from_env() is None
    monkeypatch.setenv("TMDB_API_KEY", "   ")
    assert tmdb.client_from_env() is None
    monkeypatch.setenv("TMDB_API_KEY", "abc")
    assert tmdb.client_from_env() is not None
