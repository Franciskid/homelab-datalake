"""Files a person puts in the lake by hand, because the publisher will not serve them to a script.

A publisher that answers 403 or 503 to automated requests is not worked around here. Its file is downloaded in a browser, put in the lake with
`put`, and every read of it goes through `get`, which checks the bytes against the sha256 written down when the file arrived. Next to each file
the manifest keeps where it came from, how, when, and what to do next year.

    lake/manual_inputs/<source>/MANIFEST.json                    {"entries": [...], "probes": {...}}
    lake/manual_inputs/<source>/<first 16 of sha256>-<name>      the file, never overwritten (a new version is a new object)

An entry says: role (what the file is for), edition (the year of the publication), name, sha256, size, url, how ("manual: downloaded in a
browser" or "fetched: plain request"), obtained_on, note. `get(role)` returns the newest edition's newest copy.

    python -m homelab_datalake.manual_inputs put --source ssmsi --role bilan_data_zip --edition 2026 --file X.zip --url https://… --how "manual: …"
    python -m homelab_datalake.manual_inputs list --source ssmsi
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import UTC, datetime

from homelab_datalake.resources import LakeStorage

PREFIX = "manual_inputs"


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _manifest_key(source: str) -> str:
    return f"{PREFIX}/{source}/MANIFEST.json"


def read_manifest(lake: LakeStorage, source: str) -> dict:
    raw = lake.read_bytes(_manifest_key(source))
    if raw is None:
        return {"entries": [], "probes": {}}
    manifest = json.loads(raw)
    manifest.setdefault("entries", [])
    manifest.setdefault("probes", {})
    return manifest


def _write_manifest(lake: LakeStorage, source: str, manifest: dict) -> None:
    lake.write_bytes(_manifest_key(source), json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8"), "application/json")


def put(lake: LakeStorage, source: str, role: str, name: str, data: bytes, *, edition: int, url: str, how: str, note: str = "",
        obtained_on: str | None = None) -> dict:
    """Store a file and record it. The same bytes under the same role and edition are recorded once."""
    digest = sha256(data)
    manifest = read_manifest(lake, source)
    for e in manifest["entries"]:
        if e["role"] == role and e["edition"] == edition and e["sha256"] == digest:
            return e
    key = f"{PREFIX}/{source}/{digest[:16]}-{name}"
    lake.write_bytes(key, data)
    if lake.read_bytes(key) != data:  # read back: a manual input is not something to lose silently
        raise RuntimeError(f"{key}: the stored copy does not read back")
    entry = {"role": role, "edition": edition, "name": name, "sha256": digest, "size": len(data), "url": url, "how": how, "note": note,
             "obtained_on": obtained_on or datetime.now(UTC).date().isoformat(), "key": key}
    manifest["entries"].append(entry)
    _write_manifest(lake, source, manifest)
    return entry


def latest(lake: LakeStorage, source: str, role: str, edition: int | None = None) -> dict | None:
    """The newest edition's newest entry of a role (or of the given edition)."""
    hits = [e for e in read_manifest(lake, source)["entries"] if e["role"] == role and (edition is None or e["edition"] == edition)]
    return max(hits, key=lambda e: (e["edition"], e["obtained_on"]), default=None)


def get(lake: LakeStorage, source: str, role: str, edition: int | None = None) -> tuple[bytes, dict]:
    """The bytes of the newest copy, verified against the sha256 recorded when it arrived. Raises if there is none or it has changed."""
    entry = latest(lake, source, role, edition)
    if entry is None:
        raise FileNotFoundError(f"no manual input {source}/{role}: put it in the lake with `python -m homelab_datalake.manual_inputs put`")
    data = lake.read_bytes(entry["key"])
    if data is None:
        raise FileNotFoundError(f"{entry['key']} is in the manifest and not in the lake")
    if sha256(data) != entry["sha256"]:
        raise ValueError(f"{entry['key']}: sha256 {sha256(data)} is not the recorded {entry['sha256']}")
    return data, entry


def probed(lake: LakeStorage, source: str, key: str) -> str | None:
    """When something was last looked for (an ISO date), so that a yearly publication is not searched for every day."""
    return read_manifest(lake, source)["probes"].get(key)


def mark_probed(lake: LakeStorage, source: str, key: str, on: str | None = None) -> None:
    manifest = read_manifest(lake, source)
    manifest["probes"][key] = on or datetime.now(UTC).date().isoformat()
    _write_manifest(lake, source, manifest)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("put")
    p.add_argument("--source", required=True)
    p.add_argument("--role", required=True)
    p.add_argument("--edition", required=True, type=int)
    p.add_argument("--file", required=True)
    p.add_argument("--name")
    p.add_argument("--url", required=True)
    p.add_argument("--how", required=True)
    p.add_argument("--note", default="")
    p.add_argument("--obtained-on")
    p.add_argument("--sha256", help="refuse the file unless it has this sha256 (from the SHA256SUMS written when it was downloaded)")
    ls = sub.add_parser("list")
    ls.add_argument("--source", required=True)
    args = ap.parse_args()
    lake = LakeStorage.from_env()
    if args.cmd == "list":
        for e in read_manifest(lake, args.source)["entries"]:
            print(f"{e['role']:<18} {e['edition']}  {e['obtained_on']}  {e['sha256'][:12]}  {e['size']:>9}  {e['name']}  [{e['how'].split(':')[0]}]")
        return
    with open(args.file, "rb") as f:
        data = f.read()
    if args.sha256 and sha256(data) != args.sha256:
        raise SystemExit(f"{args.file}: sha256 {sha256(data)} is not the expected {args.sha256}")
    name = args.name or args.file.rsplit("/", 1)[-1]
    e = put(lake, args.source, args.role, name, data, edition=args.edition, url=args.url, how=args.how, note=args.note, obtained_on=args.obtained_on)
    print(f"{e['key']}  sha256 {e['sha256']}  {e['size']} bytes")


if __name__ == "__main__":
    main()
