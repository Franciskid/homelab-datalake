"""What `assert_no_raw_ip_addresses.sql` must and must not fire on.

That guard has been rewritten four times, and every rewrite followed the same
morning: a build broke, the user agent in front of us turned out to hold a version
string rather than an address, and the rule learned that one shape. A product
version outside an allow-list, then a Gecko `rv:` version, then an Android firmware
revision introduced by a space -- the last of which failed the 04:15 build on
1 Sep 2026 and is the reason this file exists.

The rule was rewritten a fourth time to stop guessing at vocabulary, and that
rewrite shipped with nothing pinning it. So the samples live here instead of in a
commit message: the three that must stay quiet, and the shapes that must still
fire, checked against the real SQL rather than a copy of it.

The SQL is rendered from `dbt/tests/assert_no_raw_ip_addresses.sql` on purpose. A
copy pasted into this file would pass forever while the guard drifted underneath
it, which is the failure mode this test is supposed to prevent.

Octets are RFC 5737 documentation addresses, including where they stand in for a
version string -- `tests/test_no_real_addresses_in_fixtures.py` forbids a real one
in a tracked file, and the shapes under test are structural, so nothing is lost by
substituting. The firmware revision that actually broke the build was 1.19.161.x.
"""

import warnings
from pathlib import Path

import duckdb
import jinja2
import pytest

REPO = Path(__file__).resolve().parent.parent
GUARD_SQL = REPO / "dbt" / "tests" / "assert_no_raw_ip_addresses.sql"

#: 128 bits of lowercase hex, which is what the privacy boundary emits.
GOOD_HASH = "a1b2c3d4e5f60718293a4b5c6d7e8f90"

#: A network that is not the one any user agent below mentions, so the
#: "caller's own network" branch stays out of the way unless a case wants it.
OTHER_NET = "192.0.2.0/24"


def render_guard(table: str) -> str:
    r"""The guard's own SQL, with `ref()` pointed at a fixture table.

    Jinja decodes string literals with `unicode-escape`, so the guard's `\d`
    raises a DeprecationWarning as the template is parsed. dbt renders it through
    the same Jinja and gets the same `\d` out the other side; the warning is a
    property of writing a regex in a Jinja literal, not a defect this test should
    report, and the address cases below are what prove the pattern survived.
    """
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=DeprecationWarning, module="jinja2.*")
        template = jinja2.Template(GUARD_SQL.read_text())
    return template.render(ref=lambda _name: table)


@pytest.fixture
def fires():
    """Run the guard over one set of rows; return the column names that fired.

    Returns the guard's own labels rather than row counts, so a case that fires
    for the wrong reason fails instead of quietly passing.
    """
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE fct_http_request "
        "(user_agent VARCHAR, client_network VARCHAR, client_hash VARCHAR)"
    )
    sql = render_guard("fct_http_request")

    def run(user_agent, client_network=OTHER_NET, client_hash=GOOD_HASH):
        con.execute("DELETE FROM fct_http_request")
        con.execute(
            "INSERT INTO fct_http_request VALUES (?, ?, ?)",
            [user_agent, client_network, client_hash],
        )
        return {row[0] for row in con.execute(sql).fetchall()}

    yield run
    con.close()


# --- the three that broke a build, and must never break another ----------------

QUIET = [
    pytest.param(
        "WhatsApp/10.0.2.1 A",
        id="product version outside an allow-list",
    ),
    pytest.param(
        "Mozilla/5.0 (X11; Linux x86_64; rv:1.9.5.20) Gecko/20100101 Firefox/9.0",
        id="version introduced by a colon",
    ),
    pytest.param(
        "Mozilla/5.0 (Linux; U; Android 2.1-update1; de-de; HTC Desire 198.51.100.5 "
        "Build/ERE27) AppleWebKit/530.17 (KHTML, like Gecko) Version/4.0 "
        "Mobile Safari/530.17",
        id="firmware version introduced by a space",
    ),
]


@pytest.mark.parametrize("user_agent", QUIET)
def test_version_strings_do_not_fire(user_agent, fires):
    """A four-part number a client wrote is not evidence of a privacy fault.

    Each of these failed the build once. The third one is why the rule stopped
    trying to tell a version from an address by its digits.
    """
    assert fires(user_agent) == set()


def test_a_version_matching_another_network_stays_quiet(fires):
    """The own-network branch keys on the caller's /24, not on any /24.

    A firmware revision is not suspicious merely because some other visitor came
    from a network shaped like it.
    """
    assert fires("HTC Desire 198.51.100.5 Build/ERE27", client_network=OTHER_NET) == set()


# --- and the shapes the guard exists to catch ----------------------------------


def test_a_bare_address_fires(fires):
    """Nothing sends an address as its user agent; a column misalignment does."""
    assert fires("203.0.113.4") == {"user_agent is an address"}


def test_bare_address_tolerates_surrounding_space(fires):
    assert fires("  203.0.113.4  ") == {"user_agent is an address"}


@pytest.mark.parametrize(
    "user_agent",
    [
        pytest.param("X-Real-IP: 203.0.113.4", id="hyphenated name, space after colon"),
        pytest.param("X-Forwarded-For:203.0.113.4", id="hyphenated name, no space"),
        pytest.param("CF-Connecting-IP=203.0.113.4", id="introduced by equals"),
    ],
)
def test_a_header_value_in_the_user_agent_fires(user_agent, fires):
    """A header that reached the wrong column arrives looking like a header."""
    assert fires(user_agent) == {"header value in user_agent"}


def test_the_callers_own_network_fires(fires):
    """The fault the rule exists for: the visitor just hashed, written out in text.

    Deliberately shaped like a version -- space-introduced, no header syntax -- so
    it is caught by the own-network branch alone. That is the branch's whole claim:
    a version string matching the caller's own /24 does not happen by accident.
    """
    assert fires(
        "Mozilla/5.0 (Linux; Android 10; Build 203.0.113.77)",
        client_network="203.0.113.0/24",
    ) == {"client network leaked into user_agent"}


@pytest.mark.parametrize(
    "client_hash",
    [
        pytest.param("203.0.113.4", id="an address"),
        pytest.param(GOOD_HASH.upper(), id="uppercase hex"),
        pytest.param(GOOD_HASH[:31], id="too short"),
        pytest.param(GOOD_HASH + "ab", id="too long"),
    ],
)
def test_a_hash_that_did_not_come_from_the_boundary_fires(client_hash, fires):
    """`client_hash` is ours, not the client's, so it is checked strictly."""
    assert fires("Mozilla/5.0", client_hash=client_hash) == {"client_hash"}


def test_a_clean_row_fires_nothing(fires):
    assert fires("Mozilla/5.0 (Macintosh) Safari/605.1.15") == set()


def test_nulls_fire_nothing(fires):
    """An absent user agent and an absent hash are ordinary, not suspicious."""
    assert fires(None, client_network=None, client_hash=None) == set()
