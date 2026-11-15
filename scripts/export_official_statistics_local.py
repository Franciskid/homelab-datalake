#!/usr/bin/env python3
"""Read every official source once and write official-statistics.json, without Dagster or the lake.

For a laptop: the same collectors the daily asset runs (assets/official_statistics.py), the same
export shape, written to a directory of your choice. politico.fraserv.fr's renderer reads it:

    python scripts/export_official_statistics_local.py --out /tmp/official        # everything
    python scripts/export_official_statistics_local.py --out /tmp/official --only spending

`--only spending` fetches just the Dépenses publiques inputs (the four public-spending Eurostat
queries, the State budget, the CAS Pensions revenue and the DREES accounts) and merges them
into an existing file in --out, leaving the other blocks as they are.
"""

import argparse
import json
import logging
import ssl
from datetime import UTC, datetime
from pathlib import Path

try:  # macOS python.org builds ship without a CA bundle; certifi (a dependency of requests) has one
    import certifi

    ssl._create_default_https_context = lambda: ssl.create_default_context(cafile=certifi.where())
except ImportError:
    pass

from homelab_datalake.assets import official_statistics as asset
from homelab_datalake.parsing import official_statistics as stats

SPENDING_KEYS = ("gov_10a_exp_fr", "gov_10a_taxag_fr", "nasa_10_nf_tr_fr", "nama_10_gdp_vol", "nama_10_pe", "earn_nt_taxwedge")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--only", choices=["spending"], default=None)
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    log = logging.getLogger("export")
    args.out.mkdir(parents=True, exist_ok=True)
    fetched_at = datetime.now(UTC).isoformat(timespec="seconds")
    previous = asset.read_previous_export(args.out)
    if args.only == "spending":
        blocks: dict[str, dict] = {}
        failed: list[str] = []
        for query in (q for q in stats.EUROSTAT_QUERIES if q.key in SPENDING_KEYS):
            try:
                blocks[stats.dataset_key("eurostat", query.key)] = stats.eurostat_block(
                    query, asset.fetch_eurostat(query), fetched_at)
            except Exception as error:  # noqa: BLE001
                log.warning("eurostat %s failed: %s", query.key, error)
                failed.append(query.key)
        _rows, more, more_failed = asset.collect_spending(fetched_at, log)
        blocks.update(more)
        failed += more_failed
        merged = dict((previous or {}).get("datasets") or {})
        merged.update(blocks)
        export = {"format": stats.EXPORT_FORMAT, "generated_at": fetched_at, "datasets": merged}
    else:
        _rows, blocks, failed = asset.collect(log)
        export = stats.build_export(blocks, previous, fetched_at)
    path = asset.write_export(args.out, export)
    log.info("wrote %s (%d datasets, failed: %s)", path, len(export["datasets"]), failed or "none")


if __name__ == "__main__":
    main()
