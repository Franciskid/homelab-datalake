#!/usr/bin/env python3
"""Run one positions collection cycle on a laptop, without Dagster or the lake: the same code the scheduled jobs run.

    python scripts/positions_collect_local.py --job watch|votes|texts|backfill --root /tmp/positions-state --out /tmp/positions-export
    python scripts/positions_collect_local.py --job votes --root ... --out ... --since 2026-11-01     # export only the last weeks (dry run)
    python scripts/positions_collect_local.py --job texts --root ... --out ... --raw-cache /path/to/research/positions/raw

--root plays the container's /state/positions (database + downloads, created if missing), --out the export directory the Mac pulls.
--raw-cache links an existing download cache (e.g. the Mac's research/positions/raw) into --root so big dumps are not fetched again.
"""

import argparse
import json
import logging
import os
from pathlib import Path

from homelab_datalake.assets import positions_collection as pc


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--job", required=True, choices=["watch", "votes", "texts", "backfill"])
    ap.add_argument("--root", required=True, type=Path)
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--since", help="export only documents and ballots dated on or after this day")
    ap.add_argument("--raw-cache", type=Path)
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args.root.mkdir(parents=True, exist_ok=True)
    args.out.mkdir(parents=True, exist_ok=True)
    if args.raw_cache:
        target = args.root / "research" / "positions" / "raw"
        target.parent.mkdir(parents=True, exist_ok=True)
        if not target.exists():
            os.symlink(args.raw_cache.resolve(), target)
    result = pc.run_cycle(args.job, logging.getLogger("positions"), root=args.root, out=args.out, since=args.since)
    print(json.dumps({"failed": result["failed"], "export": (result["export"] or {}).get("delta"),
                      "steps": [{k: r.get(k) for k in ("step", "ok", "seconds", "summary")} for r in result["runs"]]}, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
