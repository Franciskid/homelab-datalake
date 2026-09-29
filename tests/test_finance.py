import io
import json
from datetime import UTC, datetime

import pytest

from homelab_datalake import lakeio, layout
from homelab_datalake.assets import finance
from homelab_datalake.parsing import ledger
from homelab_datalake.resources import LakeStorage


@pytest.fixture
def lake(tmp_path):
    return LakeStorage(local_root=str(tmp_path / "lake"))


def export(**overrides):
    payload = {
        "exportedAt": "2026-09-28T06:00:00.000Z",
        "homeCurrency": "EUR",
        "referenceDate": "2026-09-27",
        "categories": [{"key": "technology", "label": "Technology", "kind": "expense", "group": "tech"}],
        "accounts": [{
            "id": "acc-1", "name": "Revolut EUR", "bank": "Revolut", "type": "checking", "role": None,
            "currency": "EUR", "balance": 1200.5, "balanceHome": 1200.5, "source": "imported",
            "accountGroup": "Revolut", "openedAt": None,
        }],
        "transactions": [{
            "id": "tx-1", "bookedOn": "2026-09-24", "accountId": "acc-1", "accountName": "Revolut EUR",
            "merchant": "Anthropic* Claude Sub", "amount": -21.6, "currency": "EUR", "amountHome": -21.6,
            "direction": "outflow", "category": "technology", "categoryLabel": "Technology",
            "categoryGroup": "tech", "transferCategory": None, "type": "subscription", "typeSource": "derived",
            "transferKind": None, "recurringSeriesId": "series-1", "needsReview": False, "tags": ["ai"],
            "notes": None,
        }],
        "recurringSeries": [{
            "id": "series-1", "name": "Claude", "type": "subscription", "status": "active",
            "category": "technology", "accountId": "acc-1", "accountName": "Revolut EUR",
            "frequency": "monthly", "currency": "EUR", "predictedNextAmount": 21.6,
            "monthlyProjectedAmount": 21.6, "monthlyProjectedHome": 21.6, "nextDueDate": "2026-10-24",
            "firstSeen": "2026-06-23", "lastSeen": "2026-09-24", "chargeCount": 4,
        }],
    }
    payload.update(overrides)
    return payload


def read(lake, table):
    return lakeio.read_strict(lake, f"{layout.prefix(table)}/data.parquet").to_pylist()


def test_every_export_field_lands_under_a_chosen_column():
    row = finance.rename("transactions", export()["transactions"][0])

    assert row["transaction_id"] == "tx-1"
    assert row["type"] == "subscription"
    assert row["category_group"] == "tech"
    assert set(row) == set(finance.FIELDS["transactions"].values())


def test_a_ledger_declares_every_column_it_writes():
    for table, (section, _keys, _volatile) in finance.LEDGERS.items():
        declared = set(finance.ledger_schema(table).names)
        assert set(finance.FIELDS[section].values()) <= declared
        assert set(ledger.META_COLUMNS) <= declared


def test_landing_writes_ledgers_and_the_days_balances(lake):
    at = datetime(2026, 9, 28, 6, 0, tzinfo=UTC)
    stats = finance.land(lake, export(), at)

    assert stats["finance_transactions"].current_keys == 1
    [transaction] = read(lake, "finance_transactions")
    assert transaction["merchant"] == "Anthropic* Claude Sub"
    assert transaction["tags"] == '["ai"]'
    balances = lakeio.read_strict(lake, f"{finance.BALANCES_PREFIX}/dt=2026-09-28/data.parquet").to_pylist()
    assert balances[0]["balance_home"] == 1200.5


def test_a_recategorised_transaction_keeps_its_old_version(lake):
    first = export()
    finance.land(lake, first, datetime(2026, 9, 28, 6, 0, tzinfo=UTC))
    recategorised = export()
    recategorised["transactions"][0] = {**first["transactions"][0], "category": "subs", "type": "bill"}
    finance.land(lake, recategorised, datetime(2026, 9, 28, 7, 0, tzinfo=UTC))

    versions = read(lake, "finance_transactions")
    assert sorted(version["type"] for version in versions) == ["bill", "subscription"]


def test_an_exchange_rate_refresh_does_not_mint_a_version(lake):
    finance.land(lake, export(), datetime(2026, 9, 28, 6, 0, tzinfo=UTC))
    moved = export()
    moved["transactions"][0] = {**moved["transactions"][0], "amountHome": -21.58}
    finance.land(lake, moved, datetime(2026, 9, 28, 7, 0, tzinfo=UTC))

    assert len(read(lake, "finance_transactions")) == 1


def test_a_transaction_the_tracker_folded_away_keeps_its_last_version(lake):
    finance.land(lake, export(), datetime(2026, 9, 28, 6, 0, tzinfo=UTC))
    stats = finance.land(lake, export(transactions=[]), datetime(2026, 9, 28, 7, 0, tzinfo=UTC))

    assert stats["finance_transactions"].keys_gone_from_source == 1
    assert len(read(lake, "finance_transactions")) == 1


def test_a_mass_disappearance_is_flagged_but_a_few_are_not():
    few = ledger.MergeStats(versions_before=1000, versions_after=1000, new_versions=0,
                            current_keys=1000, keys_gone_from_source=5)
    many = ledger.MergeStats(versions_before=1000, versions_after=1000, new_versions=0,
                             current_keys=100, keys_gone_from_source=900)

    assert finance.disappearance_ok(few)
    assert not finance.disappearance_ok(many)


def test_the_key_is_required(monkeypatch):
    monkeypatch.delenv("FINANCE_INTERNAL_KEY", raising=False)
    with pytest.raises(ValueError, match="FINANCE_INTERNAL_KEY"):
        finance.export_settings()


def test_an_answer_that_is_not_the_export_is_refused(monkeypatch):
    def fake_urlopen(request, timeout):
        assert request.get_header("X-internal-key") == "secret-key"
        return io.BytesIO(json.dumps({"error": {"code": "unauthorized"}}).encode())

    monkeypatch.setattr(finance.urllib.request, "urlopen", fake_urlopen)
    with pytest.raises(ValueError, match="missing"):
        finance.fetch_export("http://tracker.test/internal/finance/export", "secret-key")


def test_only_http_urls_are_fetched():
    with pytest.raises(ValueError, match="http"):
        finance.fetch_export("file:///etc/passwd", "secret-key")
