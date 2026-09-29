"""Personal finance, from the banking tracker: accounts, transactions, recurring charges.

Read from the tracker's service-key export (`GET /internal/finance/export`) rather than its
database. The export is built from the same snapshot the app renders, so a transaction's type
(purchase, subscription, bill, transfer...), its category group and its amount in the home
currency are exactly what the owner sees on screen -- recomputing them here from raw rows would
be a second implementation that drifts.

Transactions, accounts, recurring series and categories are *state*: the owner recategorises a
payment, the import folds a duplicate away, a subscription ends. So they are ledgers
(parsing/ledger.py): every distinct version of every row is kept, a deleted transaction keeps its
last version, and "what did this look like in July" stays answerable. Balances are the one
time series, one row per account per day read.

Amounts are signed as the tracker signs them: negative is money out.
"""

import json
import os
import urllib.request
from datetime import UTC, date, datetime

import pyarrow as pa
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetExecutionContext,
    DefaultScheduleStatus,
    MaterializeResult,
    RetryPolicy,
    RunRequest,
    ScheduleEvaluationContext,
    asset,
    define_asset_job,
    schedule,
)

from homelab_datalake import lakeio, layout
from homelab_datalake.parsing import ledger
from homelab_datalake.resources import LakeStorage

# The tracker's backend as the other homelab containers reach it (the finance MCP uses the same
# address). Overridable because a test, or a move of the tracker, needs a different one.
DEFAULT_EXPORT_URL = "http://host.docker.internal:4001/internal/finance/export"
KEY_HEADER = "x-internal-key"
REQUEST_TIMEOUT_SECONDS = 120

# lake table -> (export section, key columns, columns that change without meaning anything).
# A home-currency amount moves with every exchange-rate refresh; recording each as a new
# version would bury the real edits, so it is kept current instead of versioned. A balance is
# recorded properly in finance_account_balances, so the account ledger does not version it.
LEDGERS: dict[str, tuple[str, tuple[str, ...], frozenset[str]]] = {
    "finance_accounts": ("accounts", ("account_id",), frozenset({"balance", "balance_home"})),
    "finance_transactions": ("transactions", ("transaction_id",), frozenset({"amount_home"})),
    "finance_recurring_series": ("recurringSeries", ("series_id",), frozenset({"monthly_projected_home"})),
    "finance_categories": ("categories", ("category_key",), frozenset()),
}

# export field -> lake column, per section. Explicit, so a field the tracker adds later does
# not land under a name nobody chose, and a field it drops is a KeyError here rather than a
# silently null column.
FIELDS: dict[str, dict[str, str]] = {
    "accounts": {
        "id": "account_id", "name": "account_name", "bank": "bank", "type": "account_type",
        "role": "role", "currency": "currency", "balance": "balance", "balanceHome": "balance_home",
        "source": "source", "accountGroup": "account_group", "openedAt": "opened_at",
    },
    "transactions": {
        "id": "transaction_id", "bookedOn": "booked_on", "accountId": "account_id",
        "accountName": "account_name", "merchant": "merchant", "amount": "amount",
        "currency": "currency", "amountHome": "amount_home", "direction": "direction",
        "category": "category", "categoryLabel": "category_label", "categoryGroup": "category_group",
        "transferCategory": "transfer_category", "type": "type", "typeSource": "type_source",
        "transferKind": "transfer_kind", "recurringSeriesId": "recurring_series_id",
        "needsReview": "needs_review", "tags": "tags", "notes": "notes",
    },
    "recurringSeries": {
        "id": "series_id", "name": "series_name", "type": "type", "status": "status",
        "category": "category", "accountId": "account_id", "accountName": "account_name",
        "frequency": "frequency", "currency": "currency", "predictedNextAmount": "predicted_next_amount",
        "monthlyProjectedAmount": "monthly_projected_amount", "monthlyProjectedHome": "monthly_projected_home",
        "nextDueDate": "next_due_date", "firstSeen": "first_seen", "lastSeen": "last_seen",
        "chargeCount": "charge_count",
    },
    "categories": {"key": "category_key", "label": "category_label", "kind": "kind", "group": "category_group"},
}



def ledger_schema(table: str) -> pa.Schema:
    """Every column a finance ledger holds, as text (parsing/ledger.py), plus its version columns.

    Declared rather than left to the first landing, so the warehouse creates the full table even
    before anything has landed and the staging models always bind -- a finance export that is
    not configured yet must not break the nightly dbt run for every other source.
    """
    section = LEDGERS[table][0]
    return pa.schema([(column, pa.string()) for column in FIELDS[section].values()] + list(ledger.META_SCHEMA))


BALANCES_PREFIX = layout.prefix("finance_account_balances")
BALANCE_SCHEMA = pa.schema([
    ("dt", pa.date32()),
    ("captured_at", pa.timestamp("us", tz="UTC")),
    ("account_id", pa.string()),
    ("account_name", pa.string()),
    ("bank", pa.string()),
    ("account_type", pa.string()),
    ("role", pa.string()),
    ("currency", pa.string()),
    ("balance", pa.float64()),
    ("home_currency", pa.string()),
    ("balance_home", pa.float64()),
])

CHECK_NO_MASS_DISAPPEARANCE = "transactions_did_not_disappear_en_masse"
# A handful of transactions legitimately leave the tracker (a duplicate folded away, a pending
# entry that never booked). Hundreds leaving at once means the export answered for the wrong
# owner or an empty workspace -- the failure a green run would otherwise hide.
MAX_GONE_ABSOLUTE = 25
MAX_GONE_SHARE = 0.05


def export_settings() -> tuple[str, str]:
    url = os.environ.get("FINANCE_EXPORT_URL", DEFAULT_EXPORT_URL)
    key = os.environ.get("FINANCE_INTERNAL_KEY", "")
    if not key:
        raise ValueError(
            "FINANCE_INTERNAL_KEY is not set. It is the banking tracker's service key "
            "(the same value as FINANCE_INTERNAL_KEY in the tracker's .env)."
        )
    return url, key


def fetch_export(url: str, key: str) -> dict:
    if not url.startswith(("http://", "https://")):
        raise ValueError(f"FINANCE_EXPORT_URL must be http(s), got {url!r}")
    request = urllib.request.Request(url, headers={KEY_HEADER: key, "Accept": "application/json"})  # noqa: S310
    with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:  # noqa: S310
        payload = json.load(response)
    missing = [section for section in ("homeCurrency", *FIELDS) if section not in payload]
    if missing:
        raise ValueError(f"finance export is missing {missing}; is FINANCE_EXPORT_URL the tracker's /export?")
    return payload


def rename(section: str, record: dict) -> dict:
    mapping = FIELDS[section]
    return {column: record.get(field) for field, column in mapping.items()}


def balance_rows(payload: dict, captured_at: datetime) -> list[dict]:
    home = payload["homeCurrency"]
    rows = []
    for account in payload["accounts"]:
        rows.append({
            "dt": captured_at.date(),
            "captured_at": captured_at,
            "account_id": account["id"],
            "account_name": account.get("name"),
            "bank": account.get("bank"),
            "account_type": account.get("type"),
            "role": account.get("role"),
            "currency": account.get("currency"),
            "balance": float(account["balance"]),
            "home_currency": home,
            "balance_home": float(account["balanceHome"]),
        })
    return rows


def disappearance_ok(stats: ledger.MergeStats) -> bool:
    limit = max(MAX_GONE_ABSOLUTE, int(stats.current_keys * MAX_GONE_SHARE))
    return stats.keys_gone_from_source <= limit


def land(lake: LakeStorage, payload: dict, captured_at: datetime) -> dict[str, ledger.MergeStats]:
    """Merge one export into the ledgers and write the day's balances. Returns ledger stats."""
    stats: dict[str, ledger.MergeStats] = {}
    for table, (section, keys, volatile) in LEDGERS.items():
        key = f"{layout.prefix(table)}/data.parquet"
        rows = [rename(section, record) for record in payload[section]]
        merged, stats[table] = ledger.merge(lakeio.read_strict(lake, key), rows, keys, captured_at, volatile)
        lake.write_table(key, merged)

    # One file per day, rewritten by every run that day: the day's balance is the last one read.
    day: date = captured_at.date()
    lake.write_table(
        f"{BALANCES_PREFIX}/dt={day.isoformat()}/data.parquet",
        pa.Table.from_pylist(balance_rows(payload, captured_at), schema=BALANCE_SCHEMA),
    )
    return stats


@asset(
    group_name="finance",
    kinds={"python", "api"},
    retry_policy=RetryPolicy(max_retries=3, delay=60),
    check_specs=[AssetCheckSpec(
        name=CHECK_NO_MASS_DISAPPEARANCE, asset="finance_raw",
        description=(
            "Warns when far more transactions left the tracker than dedup or an unbooked "
            "pending entry explain -- an export for the wrong owner looks green otherwise."
        ),
    )],
    description=(
        "The banking tracker's accounts, transactions, recurring charges and categories as "
        "versioned ledgers, plus each account's balance for the day. Built from the same "
        "snapshot the app renders."
    ),
)
def finance_raw(context: AssetExecutionContext, lake: LakeStorage) -> MaterializeResult:
    url, key = export_settings()
    payload = fetch_export(url, key)
    captured_at = datetime.now(UTC)
    stats = land(lake, payload, captured_at)
    transactions = stats["finance_transactions"]

    context.log.info(
        "finance: %d transactions (%d new versions, %d gone), %d accounts, %d recurring series",
        transactions.current_keys, transactions.new_versions, transactions.keys_gone_from_source,
        len(payload["accounts"]), len(payload["recurringSeries"]),
    )
    return MaterializeResult(
        metadata={
            **{f"{table}_current": s.current_keys for table, s in stats.items()},
            **{f"{table}_new_versions": s.new_versions for table, s in stats.items()},
            "transactions_gone_from_source": transactions.keys_gone_from_source,
            "reference_date": payload.get("referenceDate"),
            "home_currency": payload["homeCurrency"],
        },
        check_results=[AssetCheckResult(
            check_name=CHECK_NO_MASS_DISAPPEARANCE,
            passed=disappearance_ok(transactions),
            severity=AssetCheckSeverity.WARN,
            metadata={
                "gone": transactions.keys_gone_from_source,
                "current": transactions.current_keys,
            },
        )],
    )


finance_job = define_asset_job(
    name="finance", selection=[finance_raw],
    description="Land the banking tracker's ledger and today's balances.",
)


@schedule(job=finance_job, cron_schedule="50 * * * *",
          default_status=DefaultScheduleStatus.STOPPED,
          description=(
              "Hourly. The banks sync at 05:20, but the owner recategorises and annotates "
              "during the day, and a ledger merge of a few thousand rows costs nothing."
          ))
def finance_schedule(context: ScheduleEvaluationContext):
    yield RunRequest(run_key=f"finance-{context.scheduled_execution_time:%Y%m%d%H}")


ASSETS = [finance_raw]
JOBS = [finance_job]
SCHEDULES = [finance_schedule]
