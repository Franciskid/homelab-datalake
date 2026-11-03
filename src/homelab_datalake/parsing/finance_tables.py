"""The banking tracker's own tables, as its export's `tables` section sends them.

Named here rather than discovered, because the lake layout and the warehouse need every dataset
before anything lands. A table the tracker adds later is reported by finance_raw's check rather
than landed under a name nobody chose.
"""

from __future__ import annotations

import re

# tracker table -> its key columns, as they land (snake_case).
TABLES: dict[str, tuple[str, ...]] = {
    "user": ("id",),
    "institution": ("id",),
    "connection": ("id",),
    "internalAccount": ("id",),
    "externalAccount": ("id",),
    "balanceSnapshot": ("id",),
    "category": ("id",),
    "transaction": ("id",),
    "transactionTag": ("id",),
    "transactionTagAssignment": ("transaction_id", "tag_id"),
    "categorizationRule": ("id",),
    "merchantAlias": ("id",),
    "reviewDecision": ("id",),
    "recurringPattern": ("id",),
    "bill": ("id",),
    "property": ("id",),
    "mortgage": ("id",),
    "propertyExpense": ("id",),
    "propertyValuation": ("id",),
    "propertyTransactionLink": ("id",),
    "investmentAccountProfile": ("id",),
    "investmentHolding": ("id",),
    "investmentTrade": ("id",),
    "holdingValuation": ("id",),
    "cashYieldProduct": ("id",),
    "professionalCompany": ("id",),
    "professionalCompanyAccount": ("id",),
    "taxProfile": ("id",),
    "importJob": ("id",),
    "syncRun": ("id",),
    "exchangeRate": ("id",),
    "fxRateSnapshot": ("id",),
    "marketResearchSnapshot": ("id",),
}


def snake(name: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


def lake_table(tracker_table: str) -> str:
    """`investmentTrade` -> `finance_db_investment_trade`."""
    return f"finance_db_{snake(tracker_table)}"
