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
    "runtime": 148,
    "homepage": "https://www.warnerbros.com/movies/inception",
    "imdb_id": "tt1375666",
    "title": "Inception",
    "origin_country": ["GB", "US"],
    "genres": [{"id": 28, "name": "Action"}, {"id": 878, "name": "Science Fiction"}],
    "production_companies": [
        {"id": 923, "name": "Legendary Pictures", "logo_path": "/8M99Dkt23MjQMTTWukq4m5XsEuo.png"},
        {"id": 9996, "name": "Syncopy"},
    ],
    "spoken_languages": [
        {"english_name": "English", "iso_639_1": "en", "name": "English"},
        {"english_name": "Japanese", "iso_639_1": "ja", "name": "\u65e5\u672c\u8a9e"},
    ],
    "belongs_to_collection": None,
    # Prose travels now -- a synopsis is a fact about the film. The artwork paths
    # beside it still do not: they are CDN cache keys and answer nothing.
    "overview": "A thief who steals corporate secrets...",
    "tagline": "Your mind is the scene of the crime.",
    "poster_path": "/edv5CZvWj09upOsy2Y6IwDhK8bt.jpg",
    "backdrop_path": "/s3TBrRGB1iav7gFOCNx3H31MoES.jpg",
}


def test_the_kept_fields_are_parsed():
    facts = tmdb.parse_movie(MOVIE)
    assert facts.tmdb_id == 27205
    assert facts.original_language == "en"
    assert facts.budget_usd == 160000000
    assert facts.revenue_usd == 825532764
    assert facts.vote_count == 34000


def test_what_jellyfin_also_has_is_kept_under_its_own_name():
    """Two columns for one fact, deliberately, because they disagree.

    This module used to hold five columns on the argument that Jellyfin already had
    the rest. The duplication that argument avoided is real -- but so is the drift it
    hid. `community_rating` in the library snapshot is TMDB's vote average *as of the
    scan*, which can be years stale, and with nothing to compare it to the lake could
    not tell. Both are kept, each named for where it came from.
    """
    facts = tmdb.parse_movie(MOVIE)
    assert facts.vote_average == MOVIE["vote_average"]
    assert facts.vote_count == MOVIE["vote_count"]
    assert facts.original_title == MOVIE["original_title"]
    assert facts.release_date == MOVIE["release_date"]


def test_the_columns_jellyfin_has_no_field_for_at_all():
    """The genuinely new half: franchise, spoken languages, release status."""
    facts = tmdb.parse_movie(MOVIE)
    assert facts.status == MOVIE["status"]
    assert facts.genres == [g["name"] for g in MOVIE["genres"]]
    assert facts.spoken_languages == [
        lang["english_name"] for lang in MOVIE["spoken_languages"]
    ]
    assert facts.production_companies == [
        c["name"] for c in MOVIE["production_companies"]
    ]


def test_a_film_in_a_franchise_says_which_one():
    """Nothing else in this warehouse says two films are episodes of one series."""
    facts = tmdb.parse_movie({**MOVIE, "belongs_to_collection": {"id": 1565, "name": "28 Days Later"}})
    assert facts.collection_id == 1565
    assert facts.collection_name == "28 Days Later"


def test_a_standalone_film_has_no_collection_rather_than_a_zero():
    facts = tmdb.parse_movie({**MOVIE, "belongs_to_collection": None})
    assert facts.collection_id is None
    assert facts.collection_name is None


def test_image_paths_are_still_not_kept():
    """Poster and backdrop paths are CDN cache keys; they answer nothing.

    The prose beside them does now travel -- a synopsis is a fact about the film --
    but an artwork path is not a fact about anything.
    """
    facts = tmdb.parse_movie(MOVIE)
    stored = [v for v in vars(facts).values() if isinstance(v, str)]
    assert not any(v.endswith((".jpg", ".png")) for v in stored)
    assert not hasattr(facts, "poster_path")
    assert not hasattr(facts, "backdrop_path")


@pytest.mark.parametrize("value", [0, "0", None, "", "not a number"])
def test_an_unknown_budget_is_null_not_zero(value):
    """TMDB reports an unknown budget as 0, and averaging that in understates everything."""
    facts = tmdb.parse_movie({**MOVIE, "budget": value})
    assert facts.budget_usd is None


def test_a_real_zero_stays_distinguishable():
    """Nothing legitimately has a budget of exactly zero, so 0 always means unknown."""
    assert tmdb._positive_or_none(0) is None
    assert tmdb._positive_or_none(1) == 1


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
