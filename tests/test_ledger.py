"""The ledger: every distinct version of every row, and nothing ever removed."""

import sqlite3
from datetime import UTC, datetime, timedelta

import pytest

from homelab_datalake import lakeio
from homelab_datalake.assets import media_pipeline
from homelab_datalake.parsing import ledger, sqlite_source
from homelab_datalake.parsing.media_pipeline import ALL_TABLES
from homelab_datalake.resources import LakeStorage

T1 = datetime(2026, 9, 15, 6, tzinfo=UTC)
T2 = T1 + timedelta(hours=6)
T3 = T2 + timedelta(hours=6)


def _latest(table, key="id"):
    latest = {}
    for row in table.to_pylist():
        if row[key] not in latest or row[ledger.LAST_SEEN] > latest[row[key]][ledger.LAST_SEEN]:
            latest[row[key]] = row
    return latest


def test_an_unchanged_row_only_moves_its_last_seen():
    first, _ = ledger.merge(None, [{"id": 1, "status": "pending"}], ("id",), T1)
    second, stats = ledger.merge(first, [{"id": 1, "status": "pending"}], ("id",), T2)
    assert second.num_rows == 1 and stats.new_versions == 0
    row = second.to_pylist()[0]
    assert row[ledger.FIRST_SEEN] == T1 and row[ledger.LAST_SEEN] == T2


def test_a_changed_row_keeps_both_versions_in_order():
    first, _ = ledger.merge(None, [{"id": 7, "status": "pending"}], ("id",), T1)
    second, stats = ledger.merge(first, [{"id": 7, "status": "available"}], ("id",), T2)
    assert stats.new_versions == 1
    assert [r["status"] for r in second.to_pylist()] == ["pending", "available"]
    assert _latest(second)["7"]["status"] == "available"


def test_a_row_the_source_deleted_is_kept_and_reported():
    first, _ = ledger.merge(None, [{"id": 1}, {"id": 2}], ("id",), T1)
    second, stats = ledger.merge(first, [{"id": 1}], ("id",), T2)
    assert second.num_rows == 2 and stats.keys_gone_from_source == 1


def test_a_volatile_column_does_not_mint_a_version_but_stays_current():
    first, _ = ledger.merge(None, [{"id": 1, "title": "Dune", "LastSearchTime": "a"}], ("id",), T1,
                            frozenset({"LastSearchTime"}))
    second, stats = ledger.merge(first, [{"id": 1, "title": "Dune", "LastSearchTime": "b"}], ("id",), T2,
                                 frozenset({"LastSearchTime"}))
    assert stats.new_versions == 0 and second.to_pylist()[0]["LastSearchTime"] == "b"


def test_a_column_added_at_the_source_is_carried_through():
    first, _ = ledger.merge(None, [{"id": 1, "a": "x"}], ("id",), T1)
    second, _ = ledger.merge(first, [{"id": 1, "a": "x", "b": "y"}], ("id",), T2)
    assert "b" in second.column_names and second.num_rows == 2


def test_credentials_in_history_urls_are_redacted_even_json_escaped():
    raw = ('{"downloadUrl": "http://jackett:9117/dl/c411/?jackett_apikey=ttp20a5nb\\u0026path=Q2Z\\u0026file=x",'
           ' "guid": "https://c411.org/api?t=get\\u0026id=a8\\u0026apikey=1bf34c81"}')
    clean = ledger.redact_secrets(raw)
    assert "ttp20a5nb" not in clean and "1bf34c81" not in clean
    assert "path=Q2Z" in clean and "id=a8" in clean
    assert clean.count(ledger.REDACTED) == 2


def test_no_registered_table_reads_a_credential_column():
    """The columns that authenticate, by name, for every app that has them."""
    for spec in ALL_TABLES:
        if spec.source_table in ("Indexers", "DownloadClients"):
            assert "Settings" in spec.exclude, spec.table_name
        if spec.table_name == "jellyseerr_users":
            assert {"password", "plexToken", "jellyfinAuthToken", "resetPasswordGuid"} <= spec.exclude
    assert not any(spec.source_table in ("session", "user_push_subscription", "user_settings") for spec in ALL_TABLES)


@pytest.fixture
def radarr_db(tmp_path):
    path = tmp_path / "radarr.db"
    connection = sqlite3.connect(path)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute('CREATE TABLE "History" ("Id" INTEGER PRIMARY KEY, "SourceTitle" TEXT, "Data" TEXT)')
    connection.execute('CREATE TABLE "Indexers" ("Id" INTEGER PRIMARY KEY, "Name" TEXT, "Settings" TEXT)')
    connection.execute("INSERT INTO History VALUES (1, 'Dune.2021', '{\"downloadUrl\": \"http://j/?apikey=SECRET\"}')")
    connection.execute("INSERT INTO Indexers VALUES (1, 'C411', '{\"apiKey\": \"SECRET\"}')")
    connection.commit()
    yield path
    connection.close()


def test_a_live_wal_database_is_copied_and_read(radarr_db):
    with sqlite_source.snapshot(radarr_db) as connection:
        rows, key, columns = sqlite_source.read_table(connection, "Indexers", frozenset({"Settings"}))
    assert key == ("Id",) and "Settings" not in columns and rows == [{"Id": 1, "Name": "C411"}]


def test_capture_lands_history_without_a_single_secret(radarr_db, tmp_path):
    lake = LakeStorage(local_root=str(tmp_path / "lake"))
    report = media_pipeline.capture(lake, "radarr", str(radarr_db), T1)
    assert "Movies" in report["missing"]  # the fixture only has two tables
    history = lakeio.read_strict(lake, media_pipeline.ledger_key(
        next(s for s in ALL_TABLES if s.table_name == "radarr_history")))
    indexers = lakeio.read_strict(lake, media_pipeline.ledger_key(
        next(s for s in ALL_TABLES if s.table_name == "radarr_indexers")))
    everything = str(history.to_pylist()) + str(indexers.to_pylist())
    assert "SECRET" not in everything and "Dune.2021" in everything
    again = media_pipeline.capture(lake, "radarr", str(radarr_db), T3)
    assert again["tables"]["radarr_history"]["new_versions"] == 0


def test_a_missing_database_says_where_it_looked(tmp_path):
    with pytest.raises(FileNotFoundError, match="mounted"), sqlite_source.snapshot(tmp_path / "nope.db"):
        pass
