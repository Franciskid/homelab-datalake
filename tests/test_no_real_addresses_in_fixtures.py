"""Guard: every address literal in this repository must be a documentation address.

This test exists because the rule was broken. Two fixtures were lifted straight
from the production logs -- a French ISP address and an IPv6 subscriber prefix --
while writing the suite that asserts addresses never leak. Convenient, and exactly
the thing the whole privacy boundary exists to prevent.

Reviewing by eye does not scale and did not work. So the rule is a test:
documentation ranges (RFC 5737, RFC 3849), private and reserved space, and
Cloudflare's published prefixes are allowed, because those name infrastructure
rather than a person. Anything else fails.
"""

import ipaddress
import re
import subprocess
from pathlib import Path

import pytest

from homelab_datalake.privacy import CLOUDFLARE_RANGES

REPO = Path(__file__).resolve().parent.parent

# RFC 5737 (IPv4) and RFC 3849 (IPv6) exist precisely so examples need not borrow
# someone's real address.
DOCUMENTATION = [
    ipaddress.ip_network(c)
    for c in ("192.0.2.0/24", "198.51.100.0/24", "203.0.113.0/24", "2001:db8::/32")
]
CLOUDFLARE = [ipaddress.ip_network(c) for c in CLOUDFLARE_RANGES]

# 8.8.8.8 names a public resolver, not a subscriber, and is used to test the
# is_private split -- documentation ranges all report as private, so the "public"
# case needs an address that is genuinely globally routable.
ALLOWED_LITERALS = {"8.8.8.8", "0.0.0.0", "255.255.255.255"}  # noqa: S104 - a literal in an allow-list, not a bind address

ADDRESS = re.compile(
    r"\b(?:\d{1,3}\.){3}\d{1,3}\b"          # dotted quad
    r"|\b(?:[0-9a-fA-F]{1,4}:){2,7}[0-9a-fA-F]{1,4}\b"  # bare IPv6
)


def tracked_files() -> list[Path]:
    out = subprocess.run(
        ["git", "ls-files"],  # noqa: S607 - git from PATH is the intent; this is a test helper
        cwd=REPO,
        capture_output=True,
        text=True,
        check=True,
    )
    keep = {".py", ".sql", ".yml", ".yaml", ".md", ".conf", ".sh", ".toml"}
    return [REPO / line for line in out.stdout.split() if Path(line).suffix in keep]


def is_allowed(address: str) -> bool:
    if address in ALLOWED_LITERALS:
        return True
    try:
        ip = ipaddress.ip_address(address)
    except ValueError:
        return True  # a version string like "Chrome/151.0.0.0" is not an address
    if ip.is_private or ip.is_loopback or ip.is_reserved or ip.is_unspecified:
        return True
    if any(ip in net for net in DOCUMENTATION):
        return True
    return any(ip in net for net in CLOUDFLARE)


def _literals(text: str) -> set[str]:
    """Address-shaped tokens, minus the ones that are version strings.

    A match preceded by "/" is a product version -- "Chrome/151.0.0.0" -- not an
    address. The dbt test that re-checks the warehouse uses the same heuristic, for
    the same reason.
    """
    found = set()
    for match in ADDRESS.finditer(text):
        if match.start() and text[match.start() - 1] == "/":
            continue
        found.add(match.group())
    return found


@pytest.mark.parametrize("path", tracked_files(), ids=lambda p: str(p.relative_to(REPO)))
def test_no_real_addresses_in_tracked_files(path: Path):
    text = path.read_text(errors="replace")
    offenders = sorted({m for m in _literals(text) if not is_allowed(m)})
    assert not offenders, (
        f"{path.relative_to(REPO)} contains addresses that are not documentation, "
        f"private, or published infrastructure: {offenders}. "
        "Use RFC 5737 (192.0.2.0/24, 198.51.100.0/24, 203.0.113.0/24) or "
        "RFC 3849 (2001:db8::/32) instead."
    )


# --- and the same rule, applied to every commit --------------------------------
#
# Checking the working tree is not enough, and this is not hypothetical: the two
# fixtures above were removed in a later commit and stayed in history, where the
# check above could not see them. A third one -- a scanner's address pasted into
# the implementation plan as an example log line -- had been sitting there the
# whole time and was only found by scanning history.
#
# History was rewritten to remove all three. This test is what stops that from
# being a one-off cleanup: a leak that lives for a single commit is still a leak,
# and "we deleted it in the next commit" is not a fix for anything that has been
# pushed.


def _history_blobs() -> dict[str, set[str]]:
    """Every distinct version of every text file the repo has ever held.

    Keyed by blob hash so a file that never changed is read once, not once per
    commit -- the whole scan is a couple of seconds on this repo.
    """
    listing = subprocess.run(
        ["git", "rev-list", "--all"],  # noqa: S607
        cwd=REPO, capture_output=True, text=True, check=True,
    ).stdout.split()

    keep = {".py", ".sql", ".yml", ".yaml", ".md", ".conf", ".sh", ".toml"}
    blobs: dict[str, set[str]] = {}
    for commit in listing:
        # S603: the argument is a commit hash git itself just printed, not input.
        tree = subprocess.run(  # noqa: S603
            ["git", "ls-tree", "-r", commit],  # noqa: S607
            cwd=REPO, capture_output=True, text=True, check=True,
        ).stdout
        for line in tree.splitlines():
            meta, _, path = line.partition("\t")
            if Path(path).suffix not in keep:
                continue
            blobs.setdefault(meta.split()[2], set()).add(path)
    return blobs


def test_no_real_addresses_anywhere_in_history():
    if (REPO / ".git" / "shallow").exists():
        pytest.skip("shallow clone: history is not available to scan")

    offenders: dict[str, set[str]] = {}
    for blob, paths in _history_blobs().items():
        # S603: blob is a hash from ls-tree above, not input.
        text = subprocess.run(  # noqa: S603
            ["git", "cat-file", "-p", blob],  # noqa: S607
            cwd=REPO, capture_output=True, text=True, check=True,
        ).stdout
        found = {m for m in _literals(text) if not is_allowed(m)}
        if found:
            offenders.setdefault(sorted(paths)[0], set()).update(found)

    assert not offenders, (
        "these addresses appear in git history even though they are not in the "
        f"working tree: { {k: sorted(v) for k, v in offenders.items()} }. "
        "Removing them in a later commit does not remove them from the repository."
    )
