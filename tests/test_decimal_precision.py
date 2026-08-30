"""Guard: a cast that feeds round() must state the precision it keeps.

DuckDB's bare NUMERIC is DECIMAL(18,3). So

    round(sum(spend_usd)::numeric, 6)

rounds to *three* decimals, and the 6 is decoration -- the cast threw the digits away
before round() ever saw them. Nothing raises. The column simply carries less precision
than it claims, which is the failure mode this repository exists to argue against: a
wrong number that looks like a right one.

It was live for the whole history of agg_llm_spend_daily. The daily total came out
[spend withheld] low against [spend withheld], and 26 of [count withheld] rows reported a usd_per_call of [spend withheld] for a
model whose calls genuinely cost about [spend withheld] each. The MCP layer read the same
expression, so a model asking "what does a call cost" was told zero.

The rule is structural rather than a list of known-bad lines: if the round() asks for
more decimals than the cast can hold, fail. A cast that names its own precision --
decimal(18, 9) -- is always allowed, because then the two numbers can be compared.
"""

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent

# What DuckDB gives you for an unparameterised NUMERIC / DECIMAL.
BARE_NUMERIC_SCALE = 3

SEARCH_ROOTS = (REPO / "src", REPO / "dbt" / "models", REPO / "dbt" / "tests")
SUFFIXES = {".py", ".sql"}

# round( <anything> ::<type> , <digits> )
#
# The cast type is captured so a parameterised decimal can be read for its real scale.
# Non-greedy between the cast and the comma, and no nested round() in this codebase, so
# a simple pattern is enough and a SQL parser is not.
ROUND_CAST = re.compile(
    r"round\((?P<body>[^()]*(?:\([^()]*\)[^()]*)*?)"
    r"::(?P<type>numeric|decimal)\s*(?:\(\s*\d+\s*,\s*(?P<scale>\d+)\s*\))?"
    r"(?P<tail>[^()]*(?:\([^()]*\)[^()]*)*?),\s*(?P<digits>\d+)\s*\)",
    re.IGNORECASE,
)


def _files() -> list[Path]:
    found: list[Path] = []
    for root in SEARCH_ROOTS:
        if not root.exists():
            continue
        found.extend(p for p in root.rglob("*") if p.suffix in SUFFIXES and p.is_file())
    return sorted(found)


def _strip_comments(path: Path, text: str) -> str:
    """Remove comments, keep code.

    The documentation in this repository quotes the broken expression on purpose --
    both aggregates and the dbt test carry the exact line that shipped, because
    naming the fault is how it stays fixed. Scanning raw text would fail on the
    explanation of the very rule it enforces, which is how the address guard was
    first got wrong too.

    Python is tokenised rather than regexed: `#` is a comment, but SQL held in a
    string is *code* here -- the MCP semantic layer builds its measures that way, and
    that is exactly where the fault was live.
    """
    if path.suffix == ".py":
        import io
        import tokenize

        kept = []
        try:
            for token in tokenize.generate_tokens(io.StringIO(text).readline):
                if token.type != tokenize.COMMENT:
                    kept.append(token.string)
        except tokenize.TokenError:  # pragma: no cover - unparseable file
            return text
        return "\n".join(kept)
    # SQL: everything after `--` to end of line. No `--` appears inside a string
    # literal in this project, and a SQL parser to prove it would cost more than it
    # buys.
    return "\n".join(line.split("--", 1)[0] for line in text.splitlines())


def _offenders(text: str) -> list[tuple[str, int, int]]:
    """(matched text, digits asked for, digits the cast can hold)."""
    out = []
    for match in ROUND_CAST.finditer(text):
        scale = int(match.group("scale")) if match.group("scale") else BARE_NUMERIC_SCALE
        digits = int(match.group("digits"))
        if digits > scale:
            out.append((match.group(0), digits, scale))
    return out


@pytest.mark.parametrize("path", _files(), ids=lambda p: str(p.relative_to(REPO)))
def test_round_never_asks_for_more_decimals_than_the_cast_keeps(path: Path) -> None:
    offenders = _offenders(_strip_comments(path, path.read_text(encoding="utf-8")))
    assert not offenders, "\n".join(
        f"{path.relative_to(REPO)}: round() asks for {digits} decimals but the cast "
        f"keeps {scale}. Use ::decimal(18, {digits}) or more.\n    {text.strip()}"
        for text, digits, scale in offenders
    )


def test_the_guard_catches_the_expression_that_was_actually_wrong() -> None:
    """The exact line that shipped, so the pattern is not merely plausible."""
    shipped = "round(sum(spend_usd)::numeric, 6)                       as spend_usd,"
    assert _offenders(shipped) == [("round(sum(spend_usd)::numeric, 6)", 6, 3)]


def test_the_guard_accepts_a_cast_that_states_its_precision() -> None:
    assert _offenders("round(sum(spend_usd)::decimal(18, 9), 6)") == []


def test_the_guard_accepts_rounding_within_a_bare_numeric() -> None:
    """round(x::numeric, 1) is correct -- one decimal fits in three."""
    assert _offenders("round(median(duration_ms)::numeric, 1)") == []
