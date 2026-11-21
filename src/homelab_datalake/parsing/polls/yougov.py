# Vendored from the politico repository (scripts/pollslib/yougov.py, 593125d). Do not edit here: change it there and run scripts/vendor-pollslib.py.
"""Read the weighted results in YouGov's technical notice for Le HuffPost."""
import re

from .tables import norm


FORMAT = "yougov-huffpost-notice"
HYPOTHESIS = re.compile(r"\bHypothèse (\d+)\b(?:\s*[–-]\s*(\S[^\n]*?)(?= {4,}|$))?", re.M)
HEADERS = re.compile(r"\bBruts +Pondérés\b")
BASE = re.compile(r"\bBase +(\d+) +(\d+)")
RESULT = re.compile(r"(\S[^\n]*?) +([0-9]+%)[ ]+([0-9]+%)")


def is_notice(pages):
    opening = norm(" ".join(pages[:4]))
    return "realise par yougov" in opening and "notice technique" in opening and "le huffpost" in opening


def sample_sizes(pages):
    opening = norm(" ".join(" ".join(pages[:5]).split()))
    match = re.search(r"echantillon (?:[•:]\s*)?(\d+) personnes .{0,150}? dont (\d+) sont inscrites", opening)
    return tuple(map(int, match.groups())) if match else (None, None)


def read_pages(pages, names, sample):
    """Keep only weighted candidate rows, retaining each table's printed page and unweighted base."""
    ids = {norm(name): cid for cid, name in names.items()}
    expected = [{}, {}]
    scenarios = [{}, {}]
    problems = []
    def candidate_id(label):
        return ids.get(norm(label.split(" - ", 1)[0].strip()))

    for page, body in enumerate(pages, 1):
        hypotheses = list(HYPOTHESIS.finditer(body))
        text = norm(" ".join(body.split()))
        if not hypotheses or "votants decides" not in text:
            continue
        ri = 0 if "premier tour de l'election" in text else 1 if "second tour de l'election" in text else None
        if ri is None:
            problems.append(f"p. {page}: round not identified")
            continue
        numbers = [int(h[1]) for h in hypotheses]
        if re.search(r"^\s*Questionnaire\s*$", body, re.M):
            for index, hypothesis in enumerate(hypotheses):
                end = hypotheses[index + 1].start() if index + 1 < len(hypotheses) else len(body)
                expected[ri][int(hypothesis[1])] = {
                    cid for line in body[hypothesis.end():end].splitlines() if (cid := candidate_id(line))
                }
            continue
        header = next((line for line in body.splitlines() if HEADERS.search(line)), "")
        columns = list(HEADERS.finditer(header))
        if len(columns) not in (1, 2) or any(n in scenarios[ri] for n in numbers):
            problems.append(f"p. {page}: unrecognized results table or repeated hypothesis")
            continue
        split = columns[1].start() if len(columns) == 2 else len(header) + 100
        if len(hypotheses) > len(columns):
            problems.append(f"p. {page}: a hypothesis is missing its table")
            continue
        bases = [{} for _ in hypotheses]
        scores = [{} for _ in hypotheses]
        for line in body.splitlines():
            for base in BASE.finditer(line):
                col = int(base.end() >= split)
                if col < len(bases):
                    bases[col] = {"unweighted": int(base[1]), "weighted": int(base[2])}
            candidate_line = BASE.sub(lambda m: " " * len(m[0]), line)
            for result in RESULT.finditer(candidate_line):
                label = result[1].strip()
                candidate = candidate_id(label)
                col = int(result.end() >= split and len(hypotheses) > 1)
                if candidate is None or candidate in scores[col]:
                    problems.append(f"p. {page}: unknown or repeated results row {label!r}")
                else:
                    scores[col][candidate] = result[3].removesuffix("%")
        for hypothesis, base, raw in zip(hypotheses, bases, scores):
            n = int(hypothesis[1])
            label = f"Hypothèse {n}" + (f" – {hypothesis[2].strip()}" if hypothesis[2] else "")
            if not base or not sample or not 0 < base["weighted"] <= sample or not 0 < base["unweighted"] <= sample:
                problems.append(f"p. {page}: missing or invalid base for hypothesis {n}")
            if raw.keys() != expected[ri].get(n):
                problems.append(f"p. {page}: candidates do not match the questionnaire for hypothesis {n}")
            scenarios[ri][n] = {"label": label, "page": page, "raw": raw,
                                "scores": {cid: int(value) for cid, value in raw.items()},
                                "incomplete": False, "base": base}
    for ri, (wanted, found) in enumerate(zip(expected, scenarios), 1):
        if not wanted or wanted.keys() != found.keys():
            problems.append(f"round {ri}: results do not match the questionnaire hypotheses")
    return scenarios, problems
