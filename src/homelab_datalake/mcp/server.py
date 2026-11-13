"""The MCP server: typed tools over the marts.

One generic `query_dataset(dataset=..., ...)` tool serves every registered dataset,
rather than one hand-written tool per dataset. That used to be a deliberate choice
the other way -- a model choosing between `llm_spend` and `camera_activity` picks
between two things it can tell apart from their names, where `dataset="llm_spend"` on
a generic tool needs the enumeration read first. The wrapper-per-dataset design held
up while there were four; at fourteen (`media_playback`, `agent_tool_calls`,
`automation_runs`, and the rest) it stopped being wide-but-thin and started being a
dataset added to the registry that a model could not reach at all, because nobody had
written it a sixth tool. That happened: `media_playback` and eight others were fully
described by `list_datasets` and `describe_dataset` and had no way to be queried, and
an agent asked what was watched last week spent eight runs trying anyway -- calling
`media_library` with `media_playback`'s own field names, an argument FastMCP silently
dropped because nothing in the schema was listening for it. `query_dataset`'s
`dataset` parameter is a JSON-schema `enum` generated from the registry at import
time (see `_DATASET_NAMES` below), so a dataset that reaches `DATASETS` reaches the
tool in the same commit, with no second tool to write.

`describe_dataset` exists for the same reason it always did. Without it the model
must guess which values a dimension takes, and guessing produces a filter that
matches nothing -- which then looks like "no activity in August" rather than "no
such workload". Giving it the real values turns a plausible wrong answer into a
correct one. It now also names `datasets.<name>`, the SQL vocabulary view described
in `homelab_datalake.mcp.semantic` -- the same names, so a caller who moves from
this tool to writing SQL by hand (the Python sandbox next door) never has to
translate a rename it was never told about.
"""

from __future__ import annotations

import base64
import difflib
import json
import os
from pathlib import Path
from typing import Annotated, Any, Literal
from urllib.parse import urlparse

from mcp.server.auth.settings import AuthSettings
from mcp.server.mcpserver import MCPServer
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import BlobResourceContents, ContentBlock, EmbeddedResource
from pydantic import ConfigDict, Field, create_model, model_validator

from homelab_datalake.api import relations
from homelab_datalake.api.catalogue import DEFAULT_MANIFEST_PATH
from homelab_datalake.mcp import query as q
from homelab_datalake.mcp.auth import verifier_from_env
from homelab_datalake.mcp.deliverables import ExportError, write_workbook
from homelab_datalake.mcp.semantic import (
    ALL_GRAINS,
    DATASETS,
    dataset_catalogue_text,
    dataset_view_name,
    describe_entry,
    get_dataset,
    measure_entry,
)
from homelab_datalake.mcp.warehouse import Warehouse

# How many distinct values describe_dataset will list for one dimension, in full
# detail. Enough to be useful for filtering, few enough that a high-cardinality
# column (request_host, model) does not fill the context with a list nobody reads.
MAX_SAMPLE_VALUES = 25

# The same idea, tighter, for describe_dataset's compact form: an enum worth showing
# without being asked is one a caller can scan at a glance, which a cap of 25 no
# longer is once there can be several dozen dimensions on the page at once.
COMPACT_MAX_VALUES = 15

# The character budgets discovery is held to, measured as len(json.dumps(result,
# separators=(",", ":"))) -- the smallest a client's own JSON encoder will ever
# produce, so passing this is a real guarantee and not one that depends on how much
# whitespace happens to be lying around. See the module-level test in
# test_mcp_server.py that runs this over the real registry, not a fixture.
# 4,500 since 2026-11-11: each dataset costs ~230 characters (name, kind, coverage,
# first sentence); edge_clients and edge_decisions took 18 datasets to 4,308.
LIST_DATASETS_BUDGET = 4_500
DESCRIBE_DATASET_BUDGET = 4_000
# 6,500 since 2026-11-10: each mart costs one ~55-character line here, and
# agg_edge_client_daily took the listing to 6,023 against the old 6,000.
# 7,000 since 2026-11-13: the infra_config_changes dataset adds its datasets.* line,
# 6,544 in all.
DESCRIBE_SCHEMA_BUDGET = 7_000

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
        resource_server_url=os.environ.get("MCP_PUBLIC_URL", "https://data.fraserv.fr/mcp"),
    )
    if _verifier
    else None
)

server = MCPServer(
    name="homelab-datalake",
    token_verifier=_verifier,
    auth=_auth,
    instructions=(
        "Analytics over a self-hosted homelab: LLM spend, web traffic, camera events, "
        "the media library and playback, and the AI platform's own database.\n\n"
        "These tools do not accept SQL. Call list_datasets for the names, then "
        "query_dataset with a dataset name, a date range, and the dimensions and "
        "measures you want by name; call describe_dataset first if you are unsure "
        "which values a dimension takes, because a filter that matches nothing "
        "returns an empty result that looks exactly like a quiet period.\n\n"
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


def _compact_json(payload: dict) -> str:
    """A tool's dict answer, serialised the way it is actually measured against.

    FastMCP's own serialisation of a non-string return -- `pydantic_core.to_json(...,
    indent=2)` -- is pretty-printed, and pretty-printing is not free: measured on
    production data, `list_datasets` grew from 3,623 bytes (this module's own
    `json.dumps(..., separators=(",", ":"))`) to 4,643 over the wire, enough on its
    own to blow the 4,000-byte budget describe_dataset and describe_schema are also
    held to. The three discovery tools return the compact string themselves instead
    of a dict, which `_convert_to_content` (in the mcp library) passes through
    unchanged rather than re-encoding -- so what this function produces is exactly
    what a caller receives, verified against `server.call_tool(...)`'s own result,
    not just against this process's own idea of the size.
    """
    return json.dumps(payload, separators=(",", ":"))


def _run(dataset: str, **kwargs) -> dict:
    try:
        with _warehouse.cursor() as cursor:
            return q.run_query(cursor, dataset, **kwargs).as_dict()
    except (q.QueryError, KeyError) as error:
        return _describe_error(error)
    except FileNotFoundError as error:
        return _describe_error(error)


# --- discovery ----------------------------------------------------------------


def _first_sentence(text: str) -> str:
    """The first sentence of a description, as a standalone sentence.

    Good enough for a one-line summary because every dataset's `description` was
    written to lead with what one row is -- the rest is caveats and history, which
    belong to describe_dataset, not to a listing whose whole job is to be short.
    """
    first = (text or "").strip().split(". ", 1)[0].strip()
    return first if first.endswith((".", "!", "?")) else f"{first}."


def _short(text: str, limit: int = 140) -> str:
    """`text`, cut to at most `limit` characters at a word boundary.

    Used where a full description would be the single biggest thing on the page --
    `describe_schema`'s table listing, mainly -- and a reader who wants the rest
    already knows to ask for one table by name.
    """
    text = (text or "").strip()
    if len(text) <= limit:
        return text
    cut = text.rfind(" ", 0, limit)
    return text[: cut if cut > 20 else limit].rstrip(" .,;:") + "…"


@server.tool(
    description=(
        "List the datasets that can be queried, with a one-line summary of what one "
        "row is, whether it is an event stream or a snapshot, and the dates it "
        "covers. Call this first if you do not already know which dataset answers "
        "the question, then describe_dataset(dataset=name) for its dimensions, "
        "measures and fields before calling query_dataset."
    ),
    # Returning the compact JSON text itself, rather than a dict for the framework
    # to serialise, is what keeps this tool inside its budget on the wire -- see
    # `_compact_json`. `structured_output=False` stops the framework treating a
    # plain `str` return as a value to wrap in its own {"result": ...} schema,
    # which would otherwise put the *parsed* payload one key deeper than every
    # caller of this tool (including this module's own tests) expects it.
    structured_output=False,
)
def list_datasets() -> str:
    return _compact_json(_list_datasets_data())


def _list_datasets_data() -> dict:
    out = []
    for dataset in DATASETS.values():
        entry: dict[str, Any] = {
            "name": dataset.name,
            "kind": dataset.kind,
            "summary": _first_sentence(dataset.description),
        }
        try:
            with _warehouse.cursor() as cursor:
                lo, hi = q.available_range(cursor, dataset)
            entry["covers"] = {"from": lo, "to": hi}
        except Exception as error:  # noqa: BLE001 - one unreadable dataset must not
            # hide the others; the reason travels with the entry instead.
            entry["covers"] = {"error": str(error)}
        out.append(entry)
    return {
        "datasets": out,
        "grains": ALL_GRAINS,
        "next": (
            "describe_dataset(dataset=name) for its dimensions, measures and "
            "fields; query_dataset(dataset=name, ...) to run a question."
        ),
    }


def _dataset_schema_lines() -> list[str]:
    return [
        f"{dataset_view_name(dataset).replace(chr(34), '')} -- "
        f"{_short(_first_sentence(dataset.description), 55)}"
        for dataset in DATASETS.values()
    ]


@server.tool(
    description=(
        "Every table in the warehouse: the `datasets.*` views (the semantic layer's "
        "own SQL vocabulary -- see describe_dataset) and the raw dbt models under "
        "`marts`/`curated`/`raw`. With no `table`, lists every queryable name with a "
        "one-line summary. With `table`, returns that one table's documented "
        "columns, primary key, declared foreign keys with the exact ON clause, and "
        "which models it is built from -- for a `datasets.*` view, its columns and a "
        "pointer to describe_dataset instead, since a semantic-layer view carries no "
        "dbt-declared keys of its own. Name a mart table by its `query_as` value -- "
        "`schema` is dbt's internal build schema and does not exist in the database "
        "you query. Call this before writing SQL, rather than guessing a name and "
        "retrying on the binder error, and before telling anyone the warehouse does "
        "not hold something: every edge is a declared and enforced relationship, not "
        "a guess from matching column names. Prefer `datasets.*` for SQL you write "
        "by hand; its columns are already spelled like describe_dataset's names."
    ),
    structured_output=False,  # see list_datasets -- keeps this on its wire budget
)
def describe_schema(
    table: Annotated[
        str | None,
        Field(description=(
            "A query_as value from a prior call (mart or datasets.* view). Omit to "
            "list every queryable name."
        )),
    ] = None,
) -> str:
    return _compact_json(_describe_schema_data(table))


def _describe_schema_data(table: str | None = None) -> dict:
    """The join graph, from the same dbt manifest the descriptions come from.

    Read through the API's own module rather than re-derived here, so the graph an
    agent is given and the graph the SQL console shows cannot disagree. The
    `datasets.*` rows are not dbt models -- they carry no keys or joins of their
    own in the manifest -- so they are synthesised here from the same registry
    describe_dataset reads, and listed first: the preferred surface, per the tool
    description above.
    """
    try:
        graph = relations.build(DEFAULT_MANIFEST_PATH)
    except Exception as error:  # noqa: BLE001 - a caller that cannot get the graph
        # should be told so and fall back to describe_dataset, not see the tool vanish.
        return _describe_error(error)

    dataset_lines = _dataset_schema_lines()
    mart_lines = [
        f"{t['query_as']} -- {_short(t['description'] or '(undocumented)', 24)}"
        for t in graph["tables"]
    ]

    if table is None:
        return {
            "datasets": dataset_lines,
            "tables": mart_lines,
            "format": "'query_as -- one-line summary', one per queryable name.",
            "preferred": (
                "Prefer `datasets.*` above for hand-written SQL: those columns are "
                "spelled exactly like describe_dataset's dimension/field names, with "
                "no mart-specific rename to remember. `tables` are the underlying "
                "dbt models, for anything the semantic layer does not expose yet. "
                "Call describe_schema(table=...) with either kind of name for detail."
            ),
        }

    requested = table.strip()
    bare = requested.split(".")[-1].strip('"')
    if bare in DATASETS:
        spec = DATASETS[bare]
        try:
            with _warehouse.cursor() as cursor:
                columns = [
                    {"name": name, "type": type_name}
                    for name, type_name in _dataset_column_types(cursor, spec).items()
                ]
        except Exception as error:  # noqa: BLE001
            return _describe_error(error)
        return {
            "query_as": dataset_view_name(spec).replace('"', ""),
            "description": spec.description,
            "columns": columns,
            "primary_key": [],
            "references": [],
            "referenced_by": [],
            "note": (
                "This is a semantic-layer view, not a dbt model -- it carries no "
                "declared keys or joins of its own. Call "
                f"describe_dataset(dataset={spec.name!r}) for its dimensions, "
                "measures, fields and the values each dimension takes."
            ),
        }

    by_query_as = {t["query_as"]: t for t in graph["tables"]}
    if requested in by_query_as:
        return by_query_as[requested]

    known = [t["query_as"] for t in graph["tables"]] + [
        dataset_view_name(d).replace('"', "") for d in DATASETS.values()
    ]
    return _describe_error(ValueError(
        f"no table or dataset named {table!r}. "
        f"Close matches: {difflib.get_close_matches(table, known, n=5, cutoff=0.3)}. "
        "Call describe_schema() with no table to list every name."
    ))


# DuckDB's own spelling for a type, shortened for the compact listing -- unabridged
# in full detail, via `_full_dimension_entry`/`describe_entry`, which never call
# this. `timestamp with time zone` alone costs the budget over 200 characters on
# camera_activity's nine timestamp columns; the abbreviation is still unambiguous,
# since nothing in this registry uses a bare, zone-less `timestamp`.
_SHORT_TYPE = {
    "timestamp with time zone": "timestamptz",
    "time with time zone": "timetz",
    "boolean": "bool",
    "integer": "int",
    "bigint": "bigint",
    "double precision": "double",
}


def _dataset_column_types(cursor, dataset) -> dict[str, str]:
    """DuckDB's own type for every column of `datasets.<name>`, from the view itself.

    Reading it off the view rather than guessing from the registry is what makes
    this free of the layer's own renames: the view already did the renaming, so its
    `description` is simply what the connection reports back.
    """
    # S608: `dataset_view_name` returns a literal built from the registry, never
    # from caller input.
    cursor.execute(f"SELECT * FROM {dataset_view_name(dataset)} LIMIT 0")  # noqa: S608
    return {
        column[0]: _SHORT_TYPE.get(str(column[1]).lower(), str(column[1]).lower())
        for column in cursor.description
    }


def _low_cardinality_values(cursor, dataset, item, cap: int) -> list | None:
    """`item`'s distinct values, or None when there are more than `cap`.

    One extra fetched so "there are more than we are showing" is known rather than
    inferred from hitting the cap exactly.
    """
    rows = cursor.execute(
        f"SELECT DISTINCT {item.sql} FROM {dataset.table} LIMIT {cap + 1}"  # noqa: S608
    ).fetchall()
    values = [r[0] for r in rows]
    return None if len(values) > cap else _ordered(values)


def _full_dimension_entry(cursor, spec, dim) -> dict:
    entry: dict[str, Any] = describe_entry(dim)
    values = _low_cardinality_values(cursor, spec, dim, MAX_SAMPLE_VALUES)
    entry["values"] = (
        values if values is not None
        else "many distinct values -- filter by an exact value you know"
    )
    return entry


def _full_field_entries(spec) -> list[dict]:
    return [describe_entry(a) for a in spec.attributes] + [
        describe_entry(d) for d in spec.dimensions if d.name not in set(spec.attribute_names)
    ]


def _full_detail(cursor, spec) -> dict:
    """Everything the pre-`query_dataset` describe_dataset returned, unabridged."""
    return {
        "dimensions": [_full_dimension_entry(cursor, spec, dim) for dim in spec.dimensions],
        "measures": [measure_entry(m) for m in spec.measures],
        "default_measures": list(spec.default_measures),
        "fields": _full_field_entries(spec),
        "default_fields": list(spec.default_fields),
        "record_order": spec.record_order,
        "description": spec.description,
        "sql_note": (
            f"Writing SQL by hand? Prefer {dataset_view_name(spec).replace(chr(34), '')} "
            "-- its columns are already spelled like the names below, with no mart "
            "rename to remember. Querying the mart directly still works: a "
            "dimension or field with a `sql` key must be written using that exact "
            "expression there; one with no `sql` key is spelled exactly as its "
            "`name`. Every measure carries `sql` and is always computed -- a "
            "measure is never a bare column. Use the `values` listed below verbatim."
        ),
        "grain_note": (
            "Every grain except 'event' aggregates. 'event' returns one row per "
            "record with the fields listed above, including exact timestamps -- ask "
            "for it when the question wants each occurrence rather than a count of "
            "them. Dimensions can be used as fields too; measures cannot, because "
            "there is nothing to aggregate over a single row."
        ) if spec.attributes else (
            "This dataset has no row-level fields, so grain='event' is not available."
        ),
    }


def _compact_dim_line(cursor, spec, dim, types: dict[str, str], include_values: bool) -> str:
    """`name`, `name:type`, or `name:type=[v1,v2,...]` for a low-cardinality one."""
    line = f"{dim.name}:{types[dim.name]}" if types.get(dim.name) else dim.name
    if not include_values:
        return line
    values = _low_cardinality_values(cursor, spec, dim, COMPACT_MAX_VALUES)
    if values is not None:
        line += "=" + json.dumps(values, separators=(",", ":"))
    return line


def _compact_field_line(attr, types: dict[str, str]) -> str:
    return f"{attr.name}:{types[attr.name]}" if types.get(attr.name) else attr.name


def _compact_measure_line(measure, include_sql: bool) -> str:
    """`name (unit)`, plus `= <sql>` only when the budget allows it."""
    line = f"{measure.name} ({measure.unit})" if measure.unit else measure.name
    return f"{line} = {measure.sql}" if include_sql else line


def _compact_detail(
    cursor, spec, types: dict[str, str], measure_sql: bool, dim_values: bool = True,
) -> dict:
    """The terse form: one line per name, encoded as a string rather than an object.

    A dict-per-entry (`{"name": ..., "type": ...}`) was tried first and measured: at
    camera_activity's 32 dimensions and 46 attributes, the repeated keys alone came
    to more than the budget before a single value was counted. `"name:type"` says
    the same thing in a quarter of the bytes, which is what buys back the room for
    an enum on the dimensions worth showing one on.

    `measure_sql` and `dim_values` are the two knobs `describe_dataset` below pulls,
    in that order, when even this does not fit -- tried rich first, and only turned
    off for the one dataset (of fourteen, measured against the fixture) that needs
    the first one, with the second held in reserve for real data whose enums run
    larger than the fixture's. Per-entry descriptions and the `sql` an old dimension
    or field entry carried are gone entirely, not truncated: `datasets.<name>`
    (named once, up top) is what makes them unnecessary -- a caller writing SQL
    selects from the view by the name shown here and it simply binds.
    """
    dimensions = [
        _compact_dim_line(cursor, spec, dim, types, dim_values) for dim in spec.dimensions
    ]
    fields = [_compact_field_line(attr, types) for attr in spec.attributes]
    measures = [_compact_measure_line(m, measure_sql) for m in spec.measures]
    return {
        "description": _short(spec.description, 160),
        "dimensions": dimensions,
        "measures": measures,
        "fields": fields,
        "fields_note": (
            "Attributes only -- dimensions above are also selectable as fields at "
            "grain='event'."
        ) if spec.attributes else "This dataset has no row-level fields.",
        "default_measures": list(spec.default_measures),
        # default_fields is left out of the compact form on purpose, not trimmed
        # for budget -- see describe_dataset(detail='full') for it. Unlike
        # default_measures, which applies silently whenever `measures` is omitted
        # at every non-event grain, a caller of grain='event' still gets the same
        # defaults from query_dataset without having asked to see them first.
        "sql_note": (
            f"SQL runs against {dataset_view_name(spec).replace(chr(34), '')}; "
            "columns are the names before any ':'. A line reads 'name', "
            "'name:type', or 'name:type=[enum]'; a measure reads 'name (unit)', "
            "plus '= <sql>' when it fit. detail='full' or names=[...] for more."
        ) + ("" if dim_values else (
            " Enum values are omitted here for space; call describe_dataset with "
            "detail='full' or names=[...] to see what a dimension takes."
        )),
    }


def _named_detail(cursor, spec, names: list[str]) -> dict:
    """Full detail, filtered to just the requested names -- whichever kind they are."""
    wanted = set(names)
    dims = [
        _full_dimension_entry(cursor, spec, d) for d in spec.dimensions if d.name in wanted
    ]
    fields = [e for e in _full_field_entries(spec) if e["name"] in wanted]
    measures = [measure_entry(m) for m in spec.measures if m.name in wanted]
    found = {e["name"] for e in (*dims, *fields, *measures)}
    payload: dict[str, Any] = {"dimensions": dims, "fields": fields, "measures": measures}
    missing = sorted(wanted - found)
    if missing:
        payload["not_found"] = (
            f"{missing} matched nothing on {spec.name!r}. Call describe_dataset "
            "with no `names` to see what is there."
        )
    return payload


@server.tool(
    description=(
        "Describe one dataset: its dimensions, measures and fields, the dates it "
        "covers, and the values each low-cardinality dimension takes -- call this "
        "before building a filter rather than guessing at a value, and before "
        "writing SQL by hand against its `datasets.*` view. detail='compact' (the "
        "default) is a scan-sized listing: name, type, and an enum only where one is "
        "cheap to show. detail='full' adds full descriptions, exact measure SQL, "
        "and a much higher enum cap. `names` overrides `detail` and returns full "
        "detail for just those dimension/measure/field names, once a compact call "
        "has told you which ones matter."
    ),
    structured_output=False,  # see list_datasets -- keeps this on its wire budget
)
def describe_dataset(
    dataset: Annotated[str, Field(description="One of the names from list_datasets.")],
    detail: Annotated[
        Literal["compact", "full"],
        Field(description="'compact' (default) for a scan-sized listing, 'full' for everything."),
    ] = "compact",
    names: Annotated[
        list[str] | None,
        Field(description=(
            "Dimension, measure or field names to return full detail for, "
            "regardless of `detail`. Omit to describe the whole dataset."
        )),
    ] = None,
) -> str:
    return _compact_json(_describe_dataset_data(dataset, detail, names))


def _describe_dataset_data(
    dataset: str, detail: str = "compact", names: list[str] | None = None,
) -> dict:
    try:
        spec = get_dataset(dataset)
    except KeyError as error:
        return _describe_error(error)

    try:
        with _warehouse.cursor() as cursor:
            lo, hi = q.available_range(cursor, spec)
            types = _dataset_column_types(cursor, spec)
            if names:
                body = _named_detail(cursor, spec, names)
            elif detail == "full":
                body = _full_detail(cursor, spec)
            else:
                # Tried richest first -- measure SQL and every dimension's enum
                # included -- and only stripped back for the datasets that do not
                # fit as measured against the real registry (camera_activity, of
                # fourteen, needs the first step; none has needed the second against
                # the fixture, which is kept as a safety margin for enums that run
                # larger against real data than the fixture's).
                body = _compact_detail(cursor, spec, types, measure_sql=True)
                oversize = len(json.dumps(body, separators=(",", ":"))) > DESCRIBE_DATASET_BUDGET
                if oversize:
                    body = _compact_detail(cursor, spec, types, measure_sql=False)
                    oversize = len(json.dumps(body, separators=(",", ":"))) > DESCRIBE_DATASET_BUDGET
                if oversize:
                    body = _compact_detail(
                        cursor, spec, types, measure_sql=False, dim_values=False,
                    )
    except Exception as error:  # noqa: BLE001 - reported to the caller as data
        return _describe_error(error)

    body.update({
        "name": spec.name,
        "view": dataset_view_name(spec).replace('"', ""),
        "table": spec.table,
        "kind": spec.kind,
        "covers": {"from": lo, "to": hi},
        "grains": ALL_GRAINS,
    })
    return body


# --- query_dataset --------------------------------------------------------------
#
# One tool, not one per dataset -- see the module docstring for why the old design
# stopped fitting once the registry grew past the four datasets it was written for.
# The parameter documentation below is deliberately spelled out in full rather than
# referred to elsewhere: the model sees one tool's schema at the moment it calls it,
# and a description that says "see list_datasets" is a description that does not help.

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
    # Not `list[str]`. Half the dimensions on these datasets are booleans, and
    # describe_dataset reports their values as `[false, true]` -- so a caller that
    # read the description and filtered `state_changed: [true]` was told
    # "Input validation error: True is not of type 'string'". One did, could not tell
    # what had gone wrong, decided its query must have been too large, and split it
    # into thirty day-sized queries until it ran out of steps. A schema that refuses
    # the values its own documentation advertises is worse than an undocumented one.
    dict[str, list[str | bool | int | float | None]] | None,
    Field(description=(
        "Restrict to given values, as {dimension: [values]}. Use the value's own "
        "type: true rather than \"true\" for a boolean dimension, a number for a "
        "numeric one. Values must match exactly -- call describe_dataset to see "
        "them. null in the list matches rows where the dimension is null."
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


# The enum, generated from the registry at import time rather than written out by
# hand: a dataset that reaches `DATASETS` reaches this tool in the same commit, with
# no second list to keep in sync and no way to add a fifteenth dataset that the tool
# schema quietly does not know about. See test_query_dataset_enum_matches_registry.
_DATASET_NAMES = tuple(sorted(DATASETS))
DatasetName = Literal[_DATASET_NAMES]

_DATASET_PARAM_DESCRIPTION = (
    "Which dataset to query. Call describe_dataset(dataset=name) first if you are "
    "unsure which fields or values it has. One line per dataset, name (kind) -- "
    "what one row is, and the caveat that most often makes a plausible query wrong:\n"
    + dataset_catalogue_text()
)


@server.tool(
    description=(
        "Run a question against any dataset in the registry: a date range, and the "
        "dimensions, measures or fields you want by name. Call describe_dataset "
        "first for the exact names a dataset takes -- a name that does not exist on "
        "it is refused with the ones that do, and with the other datasets it exists "
        "on instead, if any.\n\n"
        "`grain` decides whether you get totals or rows. Every value except one "
        "aggregates. `grain='event'` does not: it returns the individual records, "
        "each with its own exact timestamp, and it is the only way to answer a "
        "question about when something happened or to list each occurrence. If the "
        "question contains 'each', 'every', 'exact', or a time of day, reach for it "
        "first -- an hour-of-day dimension is a bucket, not a time.\n\n"
        "Every result carries a `notes` list. Read it before summarising: it is "
        "where truncation, empty results and snapshot semantics are reported. If a "
        "result has no rows, say so -- do not estimate, and do not describe it as a "
        "decline."
    )
)
def query_dataset(
    dataset: Annotated[DatasetName, Field(description=_DATASET_PARAM_DESCRIPTION)],
    start: Start, end: End, grain: Grain = "day", dimensions: Dimensions = None,
    measures: Measures = None, fields: Fields = None, filters: Filters = None,
    order_by: OrderBy = None, descending: Descending = None, limit: Limit = None,
) -> dict:
    return _run(dataset, start=start, end=end, grain=grain, dimensions=dimensions,
                measures=measures, fields=fields, filters=filters, order_by=order_by,
                descending=descending, limit=limit)


# --- the deliverable ----------------------------------------------------------

XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

# Above this a workbook is not sent inline. Ten megabytes is what the ai.fraserv
# platform stores for one generated file, and there is nothing to gain by pushing a
# larger payload through a gateway that will only refuse it at the other end. An
# export that big is also one nobody opens in a chat window.
ATTACHABLE_BYTES = 10 * 1024 * 1024


def _as_attachment(written) -> EmbeddedResource:
    """The workbook itself, as an MCP embedded resource.

    This is the whole reason `export_workbook` is worth calling from a chat. A path is
    not a deliverable to somebody reading a conversation on another machine; the file
    is. MCP has a shape for exactly this, so the bytes ride back with the answer and a
    host that knows what to do with a resource block does it -- no download endpoint,
    no signed URL, no second round trip, and nothing datalake-specific for the host to
    have learned.

    The base64 never reaches a model: a host reads text blocks for that, and this is
    not one.
    """
    return EmbeddedResource(
        type="resource",
        resource=BlobResourceContents(
            # The real path, so the URI is a true statement about where this came
            # from -- and its last segment is the filename a host will save it under.
            uri=Path(written.path).as_uri(),
            mimeType=XLSX_MIME,
            blob=base64.b64encode(written.data).decode("ascii"),
        ),
    )


@server.tool(
    description=(
        "Run a query and hand back the result as a formatted Excel workbook.\n\n"
        "THE DEFAULT WAY TO TURN A DATALAKE QUESTION INTO A SPREADSHEET. The file "
        "comes back with the answer, so a client that can take delivery of one (the "
        "ai.fraserv platform does) puts it in front of the person who asked. Say the "
        "workbook is attached; do not read them a file path. One call, no code, and "
        "the rows never pass through the conversation.\n\n"
        "Two cases go elsewhere. If the owner wants the work kept as a reusable "
        "script, or a saved script already covers this task, run that instead -- a "
        "workbook is not worth losing a workflow the owner reviewed and kept. If the "
        "answer needs something one query cannot express -- a join across marts, a "
        "computed column, several sheets -- use the Python sandbox. A tool that pastes "
        "rows into its own arguments (create_excel) is the last resort, for when "
        "neither of those worked.\n\n"
        "The workbook carries a second sheet recording the dataset, range, grain and "
        "the exact query used, so the figures can be checked later. An empty result is "
        "refused rather than exported, because a sheet with no rows reads as 'zero'. "
        "Read `delivery` in the result before writing your answer: an export too large "
        "to travel says so there, and is on the datalake host instead. Parameters are "
        "the same as query_dataset."
    )
)
def export_workbook(
    dataset: Annotated[DatasetName, Field(description="Dataset name, as in list_datasets.")],
    title: Annotated[str, Field(description="What the workbook answers; also its filename.")],
    start: Start, end: End, grain: Grain = "day", dimensions: Dimensions = None,
    measures: Measures = None, fields: Fields = None, filters: Filters = None,
    order_by: OrderBy = None, descending: Descending = None,
    limit: ExportLimit = None,
) -> list[dict[str, Any] | ContentBlock]:
    try:
        with _warehouse.cursor() as cursor:
            result = q.run_query(
                cursor, dataset, start=start, end=end, grain=grain, dimensions=dimensions,
                measures=measures, fields=fields, filters=filters, order_by=order_by,
                descending=descending, limit=limit, ceiling=q.MAX_EXPORT_ROWS,
            )
    except (q.QueryError, KeyError, FileNotFoundError) as error:
        return [_describe_error(error)]

    try:
        written = write_workbook(result, title)
    except ExportError as error:
        return [{"error": str(error), "notes": result.notes, "row_count": result.row_count}]

    summary: dict[str, Any] = {
        "filename": written.filename,
        "rows": written.rows,
        "bytes": written.bytes_written,
        "dataset": result.dataset,
        "range": {"start": result.start, "end": result.end},
        "path": written.path,
        # Said in the result and not only in the tool description, because the
        # description is read once when the tool is chosen and this is read at the
        # moment the answer is written. A path, a filename and a byte count read as
        # "here is your file", and when that was all this returned, two agent runs
        # duly reported a workbook as attached to a conversation that had no
        # attachment in it.
        "delivery": (
            f"{written.filename} travels with this result and the client has it. "
            "Tell the person it is attached. Do not read them the path -- it names a "
            "directory on the datalake host that they cannot open."
        ),
        "notes": result.notes,
    }
    if written.bytes_written > ATTACHABLE_BYTES:
        summary["delivery"] = (
            f"NOT attached: {written.bytes_written} bytes is over the "
            f"{ATTACHABLE_BYTES} a client will take. The workbook is on the datalake "
            f"host at {written.path}. Say where it was written, or export a narrower "
            "range or fewer fields so it can travel."
        )
        return [summary]

    return [summary, _as_attachment(written)]


# --- refusing an argument no tool declared --------------------------------------
#
# FastMCP validates a call's arguments against a pydantic model built from the
# function signature, and that model's default is `extra="ignore"` -- an argument
# no parameter declares is silently dropped before the tool ever runs, rather than
# refused. That is how `media_library` was called with `dataset: "media_playback"`
# for eight runs straight: the model kept adding an argument the tool had no
# parameter for, the schema said nothing, and the call quietly ran against the
# wrong dataset instead of failing with a message that would have said why.
#
# `extra="forbid"` fixes the silent half of that on its own -- a call with an
# unknown key then raises `pydantic.ValidationError`, which `MCPServer` already
# turns into a tool error carrying the field name. `_reject_unknown_arguments`
# below exists for the other half: a bare "extra fields not permitted" does not
# say what *would* have worked, and a model correcting a wrong call needs that in
# the same message it is already reading, not a second tool call to learn it. A
# `model_validator(mode="before")` runs ahead of pydantic's own extra check and
# raises first, with the valid names attached.
#
# Applied once here, after every `@server.tool` above has registered, to every
# tool in the manager -- new tools added later are covered with no extra line,
# which is the same "no code change" property `query_dataset`'s enum has, for the
# same reason: the alternative is a list that is right the day it is written and
# silently stops being checked against anything.


def _reject_unknown_arguments(server: MCPServer) -> None:
    for tool in server._tool_manager.list_tools():
        # accessor exists for the registered tools; see the module docstring above
        # for why patching the arg model in place is the chosen fix.
        model = tool.fn_metadata.arg_model
        valid = sorted({field.alias or name for name, field in model.model_fields.items()})
        tool_name = tool.name

        def _check(cls, data, _valid=valid, _tool=tool_name):
            if isinstance(data, dict):
                unknown = sorted(set(data) - set(_valid))
                if unknown:
                    names = ", ".join(repr(u) for u in unknown)
                    raise ValueError(
                        f"unknown argument{'s' if len(unknown) > 1 else ''} {names} "
                        f"for {_tool}; valid: {', '.join(_valid) or '(none)'}"
                    )
            return data

        hardened = create_model(
            model.__name__,
            __base__=model,
            __validators__={
                "_reject_unknown_arguments": model_validator(mode="before")(classmethod(_check)),
            },
        )
        hardened.model_config = ConfigDict(**{**model.model_config, "extra": "forbid"})
        hardened.model_rebuild(force=True)
        tool.fn_metadata.arg_model = hardened
        tool.parameters = hardened.model_json_schema(by_alias=True)


_reject_unknown_arguments(server)


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
        public = os.environ.get("MCP_PUBLIC_URL", "https://data.fraserv.fr/mcp")
        hostname = urlparse(public).hostname or "data.fraserv.fr"
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
