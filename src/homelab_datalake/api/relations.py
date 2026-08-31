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


@dataclass
class Table:
    name: str
    schema: str
    description: str = ""
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
        tables[name] = Table(
            name=name,
            schema=node.get("schema", ""),
            description=(node.get("description") or "").strip(),
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

    return {
        "manifest": manifest.get("metadata", {}).get("generated_at"),
        "tables": [asdict(t) for t in sorted(tables.values(), key=lambda t: t.name)],
        "edges": [asdict(e) for e in edges],
    }
