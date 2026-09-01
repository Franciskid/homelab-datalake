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
from urllib.parse import urlparse

from mcp.server.auth.settings import AuthSettings
from mcp.server.mcpserver import MCPServer
from mcp.server.transport_security import TransportSecuritySettings
from pydantic import Field

from homelab_datalake.api import relations
from homelab_datalake.api.catalogue import DEFAULT_MANIFEST_PATH
from homelab_datalake.mcp import query as q
from homelab_datalake.mcp.auth import verifier_from_env
from homelab_datalake.mcp.deliverables import ExportError, write_workbook
from homelab_datalake.mcp.semantic import ALL_GRAINS, DATASETS, TOTAL_GRAIN, get_dataset
from homelab_datalake.mcp.warehouse import Warehouse

# How many distinct values describe_dataset will list for one dimension. Enough to
# be useful for filtering, few enough that a high-cardinality column (request_host,
# model) does not fill the context with a list nobody reads.
MAX_SAMPLE_VALUES = 25

# Auth is decided at construction, from the environment, rather than patched on
# afterwards: a server that exists for a moment without its verifier is a server that
# can be started that way by mistake.
_verifier = verifier_from_env()
# No `required_scopes`, and that is deliberate.
#
# Authorisation here is a Keycloak *client role* (`datalake:query` under
# resource_access), not an OAuth *scope*. Declaring it as a required scope made the
# protected-resource metadata advertise `scopes_supported: ["datalake:query"]`, so a
# spec-compliant client dutifully asked Keycloak for that scope -- and Keycloak
# rejected the whole authorization request, because no scope by that name exists in
# the realm:
#
#     error=invalid_scope  error_description=Invalid scopes: openid datalake:query
#
# Advertising a scope the authorization server has never heard of breaks every client
# that reads the metadata, while leaving a hand-made token working perfectly. Roles
# still travel in the token and the verifier still requires the role, so nothing is
# loosened: the check simply happens where it actually lives.
#
# The alternative is to create a matching client scope in Keycloak and keep both
# mechanisms. That is defensible, but it is a second thing to configure and a second
# thing to get out of step, for an authorisation decision that is already made.
_auth = (
    AuthSettings(
        issuer_url=_verifier.issuer,
        resource_server_url=os.environ.get("MCP_PUBLIC_URL", "https://data.example.test/mcp"),
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
        "The `grain` parameter decides whether you get totals or rows. Every value "
        "except one aggregates. `grain='event'` does not: it returns the individual "
        "records, each with its own exact timestamp, and it is the only way to answer "
        "a question about when something happened or to list each occurrence. If the "
        "question contains 'each', 'every', 'exact', or a time of day, reach for it "
        "first -- an hour-of-day dimension is a bucket, not a time.\n\n"
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
            "fields": dataset.field_names,
            "supports_event_grain": bool(dataset.attributes),
        }
        try:
            with _warehouse.cursor() as cursor:
                lo, hi = q.available_range(cursor, dataset)
            entry["covers"] = {"from": lo, "to": hi}
        except Exception as error:  # noqa: BLE001 - one unreadable dataset must not
            # hide the other three; the reason travels with the entry instead.
            entry["covers"] = {"error": str(error)}
        out.append(entry)
    return {
        "datasets": out,
        "grains": ALL_GRAINS,
        "grain_note": (
            "grain='event' returns the individual records with their own timestamps "
            "instead of aggregating them. Use it whenever the question is 'each' or "
            "'every' or asks for an exact time; the other grains cannot express that."
        ),
    }


@server.tool(
    description=(
        "Every table in the warehouse: what it holds, its documented columns, its "
        "primary key, its declared foreign keys with the exact ON clause, and which "
        "models it is built from. Name a table by its `query_as` value -- `schema` is "
        "dbt's internal build schema and does not exist in the database you query. "
        "Call this before writing SQL, rather than guessing a name and retrying on "
        "the binder error, and call it before telling anyone the warehouse does not "
        "hold something. Every edge is a declared and enforced relationship, not a "
        "guess from matching column names."
    )
)
def describe_schema() -> dict:
    """The join graph, from the same dbt manifest the descriptions come from.

    Read through the API's own module rather than re-derived here, so the graph an
    agent is given and the graph the SQL console shows cannot disagree.
    """
    try:
        return relations.build(DEFAULT_MANIFEST_PATH)
    except Exception as error:  # noqa: BLE001 - a caller that cannot get the graph
        # should be told so and fall back to describe_dataset, not see the tool vanish.
        return _describe_error(error)


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
        "fields": [
            {"name": a.name, "description": a.description} for a in spec.attributes
        ] + [
            {"name": d.name, "description": d.description}
            for d in spec.dimensions if d.name not in set(spec.attribute_names)
        ],
        "default_fields": list(spec.default_fields),
        "record_order": spec.record_order,
        "grains": ALL_GRAINS,
        "grain_note": (
            "Every grain except 'event' aggregates. 'event' returns one row per record "
            "with the fields listed above, including exact timestamps -- ask for it "
            "when the question wants each occurrence rather than a count of them. "
            "Dimensions can be used as fields too; measures cannot, because there is "
            "nothing to aggregate over a single row."
        ) if spec.attributes else (
            "This dataset has no row-level fields, so grain='event' is not available."
        ),
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
        "How finely to resolve time. 'day', 'week', 'month', 'quarter' and 'year' "
        "bucket and aggregate; 'total' aggregates the whole range into one row per "
        "group; 'event' does not aggregate at all and returns the individual records "
        "with their own exact timestamps. Use 'event' whenever the question says each, "
        "every, or asks when something happened -- no other grain can answer that, "
        "because they all group the rows away."
    )),
]
Dimensions = Annotated[
    list[str] | None,
    Field(description="Dimension names to group by. Empty means no breakdown."),
]
Measures = Annotated[
    list[str] | None,
    Field(description=(
        "Measure names to compute. Omit for the dataset's defaults. Not valid with "
        "grain='event', which returns rows rather than aggregates."
    )),
]
Fields = Annotated[
    list[str] | None,
    Field(description=(
        "Only with grain='event': which columns of each record to return, by name. "
        "Call describe_dataset to see them -- they include the exact timestamps that "
        "no dimension carries. Omit for the dataset's default set."
    )),
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
Descending = Annotated[
    bool | None,
    Field(description=(
        "Sort high to low. Defaults to true when aggregating, and to false at "
        "grain='event', where a listing of what happened reads forwards in time."
    )),
]
Limit = Annotated[
    int | None,
    Field(
        description=(
            f"Maximum rows, capped at {q.MAX_ROWS:,}. Omit for the default: "
            f"{q.DEFAULT_ROWS} aggregated rows, or {q.DEFAULT_RECORD_ROWS:,} records at "
            "grain='event'. Read the notes in the result -- a listing that hit the cap "
            "says so there, and it is not the whole answer."
        ),
        ge=1, le=q.MAX_ROWS,
    ),
]
ExportLimit = Annotated[
    int | None,
    Field(
        description=(
            f"Maximum rows, capped at {q.MAX_EXPORT_ROWS:,}. The ceiling is far higher "
            "here than on the query tools because these rows go to a file rather than "
            "into the conversation. Omit it unless you want fewer."
        ),
        ge=1, le=q.MAX_EXPORT_ROWS,
    ),
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
    measures: Measures = None, fields: Fields = None, filters: Filters = None,
    order_by: OrderBy = None, descending: Descending = None, limit: Limit = None,
) -> dict:
    return _run("llm_spend", start=start, end=end, grain=grain, dimensions=dimensions,
                measures=measures, fields=fields, filters=filters, order_by=order_by,
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
    measures: Measures = None, fields: Fields = None, filters: Filters = None,
    order_by: OrderBy = None, descending: Descending = None, limit: Limit = None,
) -> dict:
    return _run("web_traffic", start=start, end=end, grain=grain, dimensions=dimensions,
                measures=measures, fields=fields, filters=filters, order_by=order_by,
                descending=descending, limit=limit)


@server.tool(
    description=(
        "Camera attention events from the NVR: which camera and zone, the exact "
        "instant it happened, how many people, vehicles and animals were counted, "
        "what the vision model concluded, whether a person later corrected it, and "
        "the clip it belongs to.\n\n"
        "At the default grain this aggregates -- activity by hour, zone or camera. "
        "For the events themselves, with `happened_at` to the microsecond, ask for "
        "grain='event'. That is what answers 'every time the gate opened and when'; "
        "started_hour_of_day is a bucket and cannot. To count openings rather than "
        "observations, filter state_changed=true. Contains no footage."
    )
)
def camera_activity(
    start: Start, end: End, grain: Grain = "day", dimensions: Dimensions = None,
    measures: Measures = None, fields: Fields = None, filters: Filters = None,
    order_by: OrderBy = None, descending: Descending = None, limit: Limit = None,
) -> dict:
    return _run("camera_activity", start=start, end=end, grain=grain, dimensions=dimensions,
                measures=measures, fields=fields, filters=filters, order_by=order_by,
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
    measures: Measures = None, fields: Fields = None, filters: Filters = None,
    order_by: OrderBy = None, descending: Descending = None, limit: Limit = None,
) -> dict:
    return _run("media_library", start=start, end=end, grain=grain, dimensions=dimensions,
                measures=measures, fields=fields, filters=filters, order_by=order_by,
                descending=descending, limit=limit)


# --- the deliverable ----------------------------------------------------------


@server.tool(
    description=(
        "Run a query and write the result as a formatted Excel workbook, returning the "
        "file path on the datalake host.\n\n"
        "IMPORTANT: this does NOT attach anything to a conversation. The workbook is "
        "written to the datalake's own export directory, where the owner has to go and "
        "fetch it. If somebody asked for a spreadsheet in the chat, query the rows at "
        "grain='event' and pass them to create_excel instead, which attaches. Use this "
        "tool when the export is too large for that (create_excel takes 2000 rows a "
        "sheet) or when the file is wanted on the host rather than in the chat -- and "
        "when you do, say where it was written rather than calling it attached.\n\n"
        "The workbook carries a second sheet recording the dataset, range, grain and "
        "the exact query used, so the figures can be checked later. An empty result is "
        "refused rather than exported, because a sheet with no rows reads as 'zero'. "
        "Parameters are the same as the dataset tools."
    )
)
def export_workbook(
    dataset: Annotated[str, Field(description="Dataset name, as in list_datasets.")],
    title: Annotated[str, Field(description="What the workbook answers; also its filename.")],
    start: Start, end: End, grain: Grain = "day", dimensions: Dimensions = None,
    measures: Measures = None, fields: Fields = None, filters: Filters = None,
    order_by: OrderBy = None, descending: Descending = None,
    limit: ExportLimit = None,
) -> dict:
    try:
        with _warehouse.cursor() as cursor:
            result = q.run_query(
                cursor, dataset, start=start, end=end, grain=grain, dimensions=dimensions,
                measures=measures, fields=fields, filters=filters, order_by=order_by,
                descending=descending, limit=limit, ceiling=q.MAX_EXPORT_ROWS,
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
        # Said in the result and not only in the tool description, because the
        # description is read once when the tool is chosen and this is read at the
        # moment the answer is written. A path, a filename and a byte count read as
        # "here is your file", and two agent runs duly reported a workbook as
        # attached to a conversation that had no attachment in it.
        "delivery": (
            f"Written to {written.path} on the datalake host. This is NOT attached to "
            "the conversation and the person you are answering cannot open it from "
            "there. Say where it was written -- or, if they wanted it in the chat, "
            "fetch the rows at grain='event' and pass them to create_excel, which "
            "attaches."
        ),
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
        public = os.environ.get("MCP_PUBLIC_URL", "https://data.example.test/mcp")
        hostname = urlparse(public).hostname or "data.example.test"
        server.run(
            transport=transport,
            # Bound to the compose network inside a container: nothing publishes this
            # port, and the reverse proxy is the only route in.
            host=os.environ.get("MCP_HOST", "0.0.0.0"),  # noqa: S104 - see above
            port=int(os.environ.get("MCP_PORT", "3333")),
            # DNS-rebinding protection stays on, but it has to be told the public
            # name: nginx forwards the original Host header, so without this every
            # proxied request is rejected as an unexpected host.
            transport_security=TransportSecuritySettings(
                enable_dns_rebinding_protection=True,
                allowed_hosts=[hostname, f"{hostname}:443", "datalake-mcp:3333"],
                allowed_origins=[f"https://{hostname}"],
            ),
        )
        return
    server.run(transport=transport)


if __name__ == "__main__":
    main()
