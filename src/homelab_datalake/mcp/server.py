"""The MCP server: typed tools over the marts.

There is one tool per dataset rather than one `query(dataset=...)` tool, and that is
a deliberate ergonomic choice rather than duplication. A model choosing between
`llm_spend` and `camera_activity` is choosing between two things it can tell apart
from their names. A model filling in `dataset="llm_spend"` on a generic tool has to
have read the enumeration first, and gets no help from the tool description it is
actually looking at. The four wrappers share one validated implementation, so the
surface is wide and the code is not.

`describe_dataset` exists for the same reason. Without it the model must guess which
values a dimension takes, and guessing produces a filter that matches nothing --
which then looks like "no activity in August" rather than "no such workload". Giving
it the real values turns a plausible wrong answer into a correct one.
"""

from __future__ import annotations

import os
from typing import Annotated, Any

from mcp.server.auth.settings import AuthSettings
from mcp.server.mcpserver import MCPServer
from pydantic import Field

from homelab_datalake.mcp import query as q
from homelab_datalake.mcp.auth import REQUIRED_ROLE, verifier_from_env
from homelab_datalake.mcp.deliverables import ExportError, write_workbook
from homelab_datalake.mcp.semantic import DATASETS, GRAINS, TOTAL_GRAIN, get_dataset
from homelab_datalake.mcp.warehouse import Warehouse

# How many distinct values describe_dataset will list for one dimension. Enough to
# be useful for filtering, few enough that a high-cardinality column (request_host,
# model) does not fill the context with a list nobody reads.
MAX_SAMPLE_VALUES = 25

# Auth is decided at construction, from the environment, rather than patched on
# afterwards: a server that exists for a moment without its verifier is a server that
# can be started that way by mistake.
_verifier = verifier_from_env()
_auth = (
    AuthSettings(
        issuer_url=_verifier.issuer,
        resource_server_url=os.environ.get("MCP_PUBLIC_URL", "https://data.example.test/mcp"),
        required_scopes=[REQUIRED_ROLE],
    )
    if _verifier
    else None
)

server = MCPServer(
    name="homelab-datalake",
    token_verifier=_verifier,
    auth=_auth,
    instructions=(
        "Analytics over a self-hosted homelab: LLM spend, web traffic, camera events "
        "and the media library.\n\n"
        "These tools do not accept SQL. Pick a dataset tool, a date range, and the "
        "dimensions and measures you want by name; call describe_dataset first if you "
        "are unsure which values a dimension takes, because a filter that matches "
        "nothing returns an empty result that looks exactly like a quiet period.\n\n"
        "Every result carries a `notes` list. Read it before summarising: it is where "
        "truncation, empty results and snapshot semantics are reported. If a result "
        "has no rows, say so -- do not estimate, and do not describe it as a decline."
    ),
)

_warehouse = Warehouse()


def _ordered(values: list) -> list:
    """Sample values in an order that reads correctly.

    Numbers sort as numbers. Sorting them as text puts hour 10 between 1 and 2,
    which turns "when is it busiest" into a nonsense axis -- and the model has no
    way to know the order it was handed is wrong.
    """
    def key(value):
        if value is None:
            return (2, 0.0, "")
        if isinstance(value, bool):
            return (0, float(value), "")
        if isinstance(value, (int, float)):
            return (0, float(value), "")
        return (1, 0.0, str(value))

    return [None if v is None else v for v in sorted(values, key=key)]


def _describe_error(error: Exception) -> dict:
    """Errors come back as data, not exceptions.

    A tool that raises gives the model a stack trace; one that returns the reason and
    the valid options gives it enough to fix the call itself, which it usually does
    on the next turn.
    """
    return {"error": str(error), "rows": [], "row_count": 0}


def _run(dataset: str, **kwargs) -> dict:
    try:
        with _warehouse.cursor() as cursor:
            return q.run_query(cursor, dataset, **kwargs).as_dict()
    except (q.QueryError, KeyError) as error:
        return _describe_error(error)
    except FileNotFoundError as error:
        return _describe_error(error)


# --- discovery ----------------------------------------------------------------


@server.tool(
    description=(
        "List the datasets that can be queried, with what each one covers. Call this "
        "first if you do not already know which dataset answers the question."
    )
)
def list_datasets() -> dict:
    out = []
    for dataset in DATASETS.values():
        entry: dict[str, Any] = {
            "name": dataset.name,
            "description": dataset.description,
            "kind": dataset.kind,
            "dimensions": dataset.dimension_names,
            "measures": dataset.measure_names,
            "default_measures": list(dataset.default_measures),
        }
        try:
            with _warehouse.cursor() as cursor:
                lo, hi = q.available_range(cursor, dataset)
            entry["covers"] = {"from": lo, "to": hi}
        except Exception as error:  # noqa: BLE001 - one unreadable dataset must not
            # hide the other three; the reason travels with the entry instead.
            entry["covers"] = {"error": str(error)}
        out.append(entry)
    return {"datasets": out, "grains": [*GRAINS, TOTAL_GRAIN]}


@server.tool(
    description=(
        "Describe one dataset: every dimension and measure with what it means, the "
        "dates it covers, and the actual values each low-cardinality dimension takes. "
        "Use the listed values when building filters rather than guessing at them."
    )
)
def describe_dataset(
    dataset: Annotated[str, Field(description="One of the names from list_datasets.")],
) -> dict:
    try:
        spec = get_dataset(dataset)
    except KeyError as error:
        return _describe_error(error)

    try:
        with _warehouse.cursor() as cursor:
            lo, hi = q.available_range(cursor, spec)
            dimensions = []
            for dim in spec.dimensions:
                entry: dict[str, Any] = {"name": dim.name, "description": dim.description}
                # One extra, so "there are more than we are showing" is known rather
                # than inferred from hitting the cap exactly.
                rows = cursor.execute(
                    f"SELECT DISTINCT {dim.sql} FROM {spec.table} "  # noqa: S608
                    f"LIMIT {MAX_SAMPLE_VALUES + 1}"
                ).fetchall()
                values = [r[0] for r in rows]
                if len(values) > MAX_SAMPLE_VALUES:
                    entry["values"] = "many distinct values -- filter by an exact value you know"
                else:
                    entry["values"] = _ordered(values)
                dimensions.append(entry)
    except Exception as error:  # noqa: BLE001 - reported to the caller as data
        return _describe_error(error)

    return {
        "name": spec.name,
        "description": spec.description,
        "kind": spec.kind,
        "covers": {"from": lo, "to": hi},
        "dimensions": dimensions,
        "measures": [
            {"name": m.name, "description": m.description, "unit": m.unit}
            for m in spec.measures
        ],
        "default_measures": list(spec.default_measures),
        "grains": [*GRAINS, TOTAL_GRAIN],
    }


# --- the four dataset tools ---------------------------------------------------
#
# The parameter documentation is deliberately repeated per tool rather than referred
# to elsewhere: the model sees one tool's schema at the moment it calls it, and a
# description that says "see list_datasets" is a description that does not help.

Start = Annotated[str, Field(description="First day of the range, YYYY-MM-DD. Inclusive.")]
End = Annotated[str, Field(description="Last day of the range, YYYY-MM-DD. Inclusive.")]
Grain = Annotated[
    str,
    Field(description=(
        "Time bucket: day, week, month, quarter, year, or 'total' for no bucketing "
        "(one row per group over the whole range)."
    )),
]
Dimensions = Annotated[
    list[str] | None,
    Field(description="Dimension names to group by. Empty means no breakdown."),
]
Measures = Annotated[
    list[str] | None,
    Field(description="Measure names to compute. Omit for the dataset's defaults."),
]
Filters = Annotated[
    dict[str, list[str]] | None,
    Field(description=(
        "Restrict to given values, as {dimension: [values]}. Values must match "
        "exactly -- call describe_dataset to see them. null in the list matches rows "
        "where the dimension is null."
    )),
]
OrderBy = Annotated[
    str | None,
    Field(description="Column to sort by; must be one you asked for. Defaults to time."),
]
Descending = Annotated[bool, Field(description="Sort high to low. Default true.")]
Limit = Annotated[
    int, Field(description=f"Maximum rows, capped at {q.MAX_ROWS}.", ge=1, le=q.MAX_ROWS)
]


@server.tool(
    description=(
        "LLM cost and usage from the LiteLLM ledger: spend, tokens, latency and "
        "failures by model, provider or workload. Answers questions like 'what did I "
        "spend on Claude last month' and 'which workload burns the most tokens'. "
        "Prompts and completions are not stored and cannot be retrieved."
    )
)
def llm_spend(
    start: Start, end: End, grain: Grain = "day", dimensions: Dimensions = None,
    measures: Measures = None, filters: Filters = None, order_by: OrderBy = None,
    descending: Descending = True, limit: Limit = q.DEFAULT_ROWS,
) -> dict:
    return _run("llm_spend", start=start, end=end, grain=grain, dimensions=dimensions,
                measures=measures, filters=filters, order_by=order_by,
                descending=descending, limit=limit)


@server.tool(
    description=(
        "HTTP traffic through the reverse proxy: requests, distinct visitors, error "
        "rates, bytes and geography, by host, status or client type. Client addresses "
        "are hashed at ingestion, so counts and locations work but no individual "
        "visitor can be identified or looked up."
    )
)
def web_traffic(
    start: Start, end: End, grain: Grain = "day", dimensions: Dimensions = None,
    measures: Measures = None, filters: Filters = None, order_by: OrderBy = None,
    descending: Descending = True, limit: Limit = q.DEFAULT_ROWS,
) -> dict:
    return _run("web_traffic", start=start, end=end, grain=grain, dimensions=dimensions,
                measures=measures, filters=filters, order_by=order_by,
                descending=descending, limit=limit)


@server.tool(
    description=(
        "Camera attention events: how many, where, when, and what the vision model "
        "concluded. Good for activity patterns by hour, zone or camera. Contains no "
        "footage, no zone geometry and no identities."
    )
)
def camera_activity(
    start: Start, end: End, grain: Grain = "day", dimensions: Dimensions = None,
    measures: Measures = None, filters: Filters = None, order_by: OrderBy = None,
    descending: Descending = True, limit: Limit = q.DEFAULT_ROWS,
) -> dict:
    return _run("camera_activity", start=start, end=end, grain=grain, dimensions=dimensions,
                measures=measures, filters=filters, order_by=order_by,
                descending=descending, limit=limit)


@server.tool(
    description=(
        "The film library as it stood on a given day: titles, runtime, resolution, "
        "codecs and ratings. This is a snapshot rather than a stream of events, so a "
        "date range selects WHICH day to report, and the answer describes that one "
        "day. Counts must not be summed across days -- that counts each film once per "
        "day it existed."
    )
)
def media_library(
    start: Start, end: End, grain: Grain = TOTAL_GRAIN, dimensions: Dimensions = None,
    measures: Measures = None, filters: Filters = None, order_by: OrderBy = None,
    descending: Descending = True, limit: Limit = q.DEFAULT_ROWS,
) -> dict:
    return _run("media_library", start=start, end=end, grain=grain, dimensions=dimensions,
                measures=measures, filters=filters, order_by=order_by,
                descending=descending, limit=limit)


# --- the deliverable ----------------------------------------------------------


@server.tool(
    description=(
        "Run a query and write the result as a formatted Excel workbook, returning the "
        "file path. The workbook carries a second sheet recording the dataset, range, "
        "grain and the exact query used, so the figures can be checked later. An empty "
        "result is refused rather than exported, because a sheet with no rows reads as "
        "'zero'. Parameters are the same as the dataset tools."
    )
)
def export_workbook(
    dataset: Annotated[str, Field(description="Dataset name, as in list_datasets.")],
    title: Annotated[str, Field(description="What the workbook answers; also its filename.")],
    start: Start, end: End, grain: Grain = "day", dimensions: Dimensions = None,
    measures: Measures = None, filters: Filters = None, order_by: OrderBy = None,
    descending: Descending = True,
    limit: Limit = q.MAX_ROWS,
) -> dict:
    try:
        with _warehouse.cursor() as cursor:
            result = q.run_query(
                cursor, dataset, start=start, end=end, grain=grain, dimensions=dimensions,
                measures=measures, filters=filters, order_by=order_by,
                descending=descending, limit=limit,
            )
    except (q.QueryError, KeyError, FileNotFoundError) as error:
        return _describe_error(error)

    try:
        written = write_workbook(result, title)
    except ExportError as error:
        return {"error": str(error), "notes": result.notes, "row_count": result.row_count}

    return {
        "path": written.path,
        "filename": written.filename,
        "rows": written.rows,
        "bytes": written.bytes_written,
        "dataset": result.dataset,
        "range": {"start": result.start, "end": result.end},
        "notes": result.notes,
    }


def main() -> None:
    """Run the server.

    stdio for a local client, streamable-http for anything on the network. The HTTP
    transport refuses to start without an issuer configured: an analytics endpoint
    that answers anyone is a worse outcome than one that is down, because nothing
    about it looks wrong from the outside.
    """
    transport = os.environ.get("MCP_TRANSPORT", "stdio")
    if transport in ("streamable-http", "sse"):
        if _verifier is None:
            raise SystemExit(
                "MCP_OIDC_ISSUER is not set. Refusing to serve the warehouse over the "
                "network without authentication -- set it to the Keycloak realm URL, "
                "or use MCP_TRANSPORT=stdio for a local client."
            )
        server.settings.host = os.environ.get("MCP_HOST", "0.0.0.0")  # noqa: S104 - in a
        # container, bound to the compose network; nothing publishes this port and the
        # reverse proxy is the only route in.
        server.settings.port = int(os.environ.get("MCP_PORT", "3333"))
    server.run(transport=transport)


if __name__ == "__main__":
    main()
