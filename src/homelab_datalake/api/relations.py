"""The join graph, so a model does not have to guess how the tables connect.

There are two ways to ask this warehouse a question and, until this existed, neither
told a caller how the tables relate.

* The MCP semantic layer is a single-dataset design by deliberate choice -- dataset,
  grain, dimensions, measures, filters -- and has no concept of a join. That argument
  is good and this does not weaken it: nothing here composes SQL.
* The SQL path takes free text against a schema the caller infers from column names.
  Which is the retry loop: guess a join, get a binder error, guess again.

So this serves the graph as *data*. It is assembled from `dbt parse`'s manifest, which
is already built into the image and already read next door for column descriptions, so
there is nothing new to generate and nothing new to schedule.

**Why the edges come from `relationships` tests.** dbt writes a declared foreign key
into the manifest as a test node carrying `to` and `field`. Declaring the edge that way
means one statement produces both the documentation *and* an enforced test -- so an
edge that stops being true fails the nightly build instead of quietly misleading every
query written against it afterwards. An edge inferred by matching column names would
have neither property, and would confidently pair `raw.camera_events.id` with
`raw.camera_clips.id`, which are different things that happen to share a name.

Primary keys come from the same manifest. dbt infers them from a `unique` and
`not_null` pair on one column, so they are already there for the asking.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Edge:
    """One declared foreign key: `from_table.from_column` -> `to_table.to_column`."""

    from_table: str
    from_column: str
    to_table: str
    to_column: str

    @property
    def join_sql(self) -> str:
        """The ON clause, spelled out, so a caller does not have to assemble it."""
        return (
            f"{self.from_table}.{self.from_column} = {self.to_table}.{self.to_column}"
        )

    def as_dict(self) -> dict:
        # `join_sql` is a property, and `asdict` does not serialise properties -- so
        # the graph advertised "the exact ON clause" and shipped four bare names.
        out = asdict(self)
        out["join_sql"] = self.join_sql
        return out


#: What dbt calls the mart schema and what a caller has to type are not the same
#: word. dbt-duckdb prefixes the configured schema with the target's, so `marts`
#: becomes `main_marts` in the build database -- and the serving copy every reader
#: actually queries renames it back to `marts`. A graph reporting the build name
#: hands out a table reference that fails to bind, which is not a small error: it is
#: the one an agent already hit, spent a step on, and worked around by giving up on
#: the query. So both names travel, and the queryable one is called `query_as`.
_BUILD_PREFIX = "main_"


def queryable_schema(schema: str) -> str:
    return schema[len(_BUILD_PREFIX):] if schema.startswith(_BUILD_PREFIX) else schema


@dataclass
class Table:
    name: str
    schema: str
    #: How to name this table in SQL sent to /query or to `datalake.query`.
    query_as: str = ""
    description: str = ""
    columns: list[dict] = field(default_factory=list)
    primary_key: list[str] = field(default_factory=list)
    #: Edges where this table is the child -- what it points at.
    references: list[Edge] = field(default_factory=list)
    #: Edges where this table is the parent -- what points at it.
    referenced_by: list[Edge] = field(default_factory=list)
    #: Model-level lineage: which models this one is built from.
    built_from: list[str] = field(default_factory=list)


def _ref_name(value: str) -> str:
    """`ref('fct_camera_clip')` -> `fct_camera_clip`.

    dbt stores the kwarg as the literal Jinja a person wrote, not as a resolved id,
    so it has to be unwrapped. A `source()` reference is left alone: it names a raw
    table, and a caller can still read it.
    """
    inner = value.strip()
    if inner.startswith("ref("):
        inner = inner[4:].rstrip(")")
    return inner.strip().strip("\"'")


def build(manifest_path: str) -> dict:
    """The relations graph as a plain dict, ready to serialise.

    A missing or unreadable manifest returns an empty graph rather than raising, for
    the same reason the catalogue does: the endpoint answering "I know of no relations"
    is more useful than a 500, and the emptiness is visible in the response.
    """
    try:
        with open(manifest_path, encoding="utf-8") as handle:
            manifest = json.load(handle)
    except (OSError, ValueError) as error:
        logger.warning("no dbt manifest at %s (%s); relations will be empty",
                       manifest_path, error)
        return {"tables": [], "edges": [], "manifest": None}

    nodes = manifest.get("nodes", {})
    models = {
        node["name"]: node
        for node in nodes.values()
        if node.get("resource_type") == "model"
    }

    tables: dict[str, Table] = {}
    for name, node in models.items():
        schema = node.get("schema", "")
        # Columns, because a caller that cannot see them writes SQL against the
        # names it can guess. The manifest holds only the documented ones, which is
        # a floor rather than the whole truth -- said plainly in `columns_note`
        # below, so an absent column reads as "not documented" and not as "not there".
        columns = [
            {"name": column_name, "description": (spec.get("description") or "").strip()}
            for column_name, spec in (node.get("columns") or {}).items()
        ]
        tables[name] = Table(
            name=name,
            schema=schema,
            query_as=f"{queryable_schema(schema)}.{name}" if schema else name,
            description=(node.get("description") or "").strip(),
            columns=columns,
            primary_key=list(node.get("primary_key") or []),
            built_from=sorted(
                parent.split(".")[-1]
                for parent in node.get("depends_on", {}).get("nodes", [])
            ),
        )

    edges: list[Edge] = []
    for node in nodes.values():
        meta = node.get("test_metadata") or {}
        if meta.get("name") != "relationships":
            continue
        kwargs = meta.get("kwargs") or {}
        child = (node.get("attached_node") or "").split(".")[-1]
        parent = _ref_name(str(kwargs.get("to", "")))
        column = kwargs.get("column_name")
        to_column = kwargs.get("field")
        if not (child and parent and column and to_column):
            continue
        edge = Edge(child, str(column), parent, str(to_column))
        edges.append(edge)
        if child in tables:
            tables[child].references.append(edge)
        if parent in tables:
            tables[parent].referenced_by.append(edge)

    def table_dict(table: Table) -> dict:
        out = asdict(table)
        out["references"] = [e.as_dict() for e in table.references]
        out["referenced_by"] = [e.as_dict() for e in table.referenced_by]
        return out

    return {
        "manifest": manifest.get("metadata", {}).get("generated_at"),
        "tables": [table_dict(t) for t in sorted(tables.values(), key=lambda t: t.name)],
        "edges": [e.as_dict() for e in edges],
        "how_to_name_a_table": (
            "Use `query_as`. `schema` is dbt's build schema and does not exist in the "
            "database you are querying."
        ),
        "columns_note": (
            "`columns` lists the documented columns, which may be fewer than the table "
            "has. An absent name means undocumented, not missing -- run "
            "`DESCRIBE <query_as>` to see the rest."
        ),
    }
