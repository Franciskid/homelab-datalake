"""Official statistics: JSON-stat and the SSMSI CSV parse into ledger rows, a revision is a
new version, and a publisher that fails keeps yesterday's figures in the export."""

import json
from datetime import UTC, datetime

import pyarrow as pa
from dagster import build_asset_context

from homelab_datalake import layout
from homelab_datalake.assets import official_statistics as asset
from homelab_datalake.parsing import ledger
from homelab_datalake.parsing import official_statistics as stats
from homelab_datalake.resources import LakeStorage

# A JSON-stat 2.0 dataset as Eurostat sends it: value and status as sparse objects.
PAYLOAD = {
    "version": "2.0", "class": "dataset", "label": "Obligations de quitter le territoire",
    "updated": "2026-11-13T23:00:00+0100",
    "id": ["freq", "unit", "geo", "time"], "size": [1, 1, 2, 2],
    "dimension": {
        "freq": {"category": {"index": {"A": 0}, "label": {"A": "Annuel"}}},
        "unit": {"category": {"index": {"PER": 0}, "label": {"PER": "Personne"}}},
        "geo": {"category": {"index": {"FR": 0, "DE": 1}, "label": {"FR": "France", "DE": "Allemagne"}}},
        "time": {"category": {"index": {"2024": 0, "2025": 1}, "label": {"2024": "2024", "2025": "2025"}}},
    },
    "value": {"0": 125000, "1": 137550, "3": 55240},
    "status": {"1": "p"},
}
QUERY = stats.EurostatQuery("migr_eiord", {"unit": ("PER",)}, "2010", "test")

SSMSI_CSV = (
    "﻿\"Code_departement\";\"Code_region\";\"annee\";\"indicateur\";\"unite_de_compte\";\"nombre\";\"taux_pour_mille\"\n"
    "\"01\";\"84\";\"2025\";\"Homicides\";\"Victime\";\"5\";\"0,0078\"\n"
    "\"02\";\"32\";\"2025\";\"Homicides\";\"Victime\";\"10\";\"0,0186\"\n"
    "\"03\";\"84\";\"2025\";\"Homicides\";\"Victime\";\"NA\";\"NA\"\n"
)


def test_jsonstat_flattens_sparse_values_with_status():
    rows, labels = stats.jsonstat_rows(PAYLOAD)
    assert {(r["geo"], r["time"], r["value"]) for r in rows} == {
        ("FR", "2024", 125000.0), ("FR", "2025", 137550.0), ("DE", "2025", 55240.0)}
    assert next(r for r in rows if r["geo"] == "FR" and r["time"] == "2025")["status"] == "p"
    assert labels["geo"]["DE"] == "Allemagne"


def test_eurostat_rows_key_the_series_without_geo_and_time():
    rows = stats.eurostat_ledger_rows(QUERY, PAYLOAD)
    assert {r["series_key"] for r in rows} == {"unit=PER"}
    assert rows[0]["source_url"].endswith("/migr_eiord/default/table?lang=fr")
    block = stats.eurostat_block(QUERY, PAYLOAD, "2026-11-14T05:40:00+00:00")
    assert block["dims"] == ["unit", "geo", "time"]
    assert ["PER", "FR", "2025", 137550.0] in block["rows"]


def test_every_query_asks_for_the_eu_countries():
    url = stats.EUROSTAT_QUERIES[0].url()
    assert "geo=FR" in url and "geo=EU27_2020" in url and "sinceTimePeriod=" in url
    assert len({q.key for q in stats.EUROSTAT_QUERIES}) == len(stats.EUROSTAT_QUERIES)


def test_ssmsi_adds_the_national_total_and_skips_suppressed_counts():
    rows = stats.ssmsi_rows(SSMSI_CSV)
    national = [r for r in rows if r["geo"] == "FR"]
    assert national == [{"indicateur": "Homicides", "unite_de_compte": "Victime", "geo": "FR", "time": "2025", "value": 15.0}]
    assert {r["geo"] for r in rows} == {"FR", "FR-D01", "FR-D02"}


def test_ssmsi_resource_is_the_newest_departmental_csv():
    dataset = {"resources": [
        {"format": "csv", "title": "REG - Base régionale", "last_modified": "2026-07-09", "url": "reg"},
        {"format": "csv", "title": "DEP - Base départementale", "last_modified": "2025-07-09", "url": "old"},
        {"format": "csv", "title": "DEP - Base départementale", "last_modified": "2026-07-09", "url": "new"},
        {"format": "parquet", "title": "DEP - parquet", "last_modified": "2027-01-01", "url": "pq"},
    ]}
    assert stats.ssmsi_resource(dataset)["url"] == "new"


def test_a_revision_is_a_new_version():
    first = stats.eurostat_ledger_rows(QUERY, PAYLOAD)
    t0, t1 = datetime(2026, 11, 13, tzinfo=UTC), datetime(2026, 11, 14, tzinfo=UTC)
    merged, _ = ledger.merge(None, first, stats.KEY_COLUMNS, t0, stats.VOLATILE_COLUMNS)
    revised = json.loads(json.dumps(PAYLOAD))
    revised["value"]["1"] = 138000
    revised["updated"] = "2026-11-14T11:00:00+0100"
    merged, merge_stats = ledger.merge(merged, stats.eurostat_ledger_rows(QUERY, revised),
                                       stats.KEY_COLUMNS, t1, stats.VOLATILE_COLUMNS)
    assert merge_stats.new_versions == 1  # only the revised figure; the date stamp alone is volatile
    fr2025 = [r for r in merged.to_pylist() if r["geo"] == "FR" and r["period"] == "2025"]
    assert sorted(float(r["value"]) for r in fr2025) == [137550.0, 138000.0]
    pa.Table.from_pylist(merged.to_pylist(), schema=asset.LEDGER_SCHEMA)


def test_a_failed_source_keeps_yesterdays_block_marked_stale():
    previous = {"datasets": {"ssmsi:delinquance_departementale": {"rows": [[1]], "status": "ok"},
                             "eurostat:migr_eiord": {"rows": [[0]], "status": "ok"}}}
    export = stats.build_export({"eurostat:migr_eiord": {"rows": [[2]], "status": "ok"}}, previous, "now")
    assert export["datasets"]["eurostat:migr_eiord"]["rows"] == [[2]]
    assert export["datasets"]["ssmsi:delinquance_departementale"]["status"] == "stale"


def test_the_asset_lands_the_ledger_and_writes_the_export(tmp_path, monkeypatch):
    monkeypatch.setattr(stats, "EUROSTAT_QUERIES", (QUERY,))
    monkeypatch.setattr(asset, "fetch_eurostat", lambda q: PAYLOAD)
    monkeypatch.setattr(asset, "collect_spending", lambda fetched_at, log: ([], {}, []))
    monkeypatch.setattr(asset, "fetch_ssmsi", lambda: (stats.ssmsi_rows(SSMSI_CSV),
                                                       {"title": "DEP - Base", "last_modified": "2026-07-09", "url": "u"}))
    export_dir = tmp_path / "export"
    export_dir.mkdir()
    monkeypatch.setenv("OFFICIAL_STATS_EXPORT_DIR", str(export_dir))
    lake = LakeStorage(local_root=str(tmp_path / "lake"))
    result = asset.official_statistics_raw(build_asset_context(), lake)
    assert all(check.passed for check in result.check_results)
    landed = lake.read_table(f"{layout.prefix('official_stat_observations')}/data.parquet")
    assert landed.num_rows == 3 + 3  # three Eurostat values, two départements and France
    export = json.loads((export_dir / asset.EXPORT_NAME).read_text())
    assert set(export["datasets"]) == {"eurostat:migr_eiord", "ssmsi:delinquance_departementale"}
    assert export["datasets"]["ssmsi:delinquance_departementale"]["rows"] == [["Homicides", "Victime", "FR", "2025", 15.0]]
