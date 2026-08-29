"""The semantic layer: what may be asked, in the vocabulary of the business.

This is the part of the MCP server that matters, and it is deliberately small.

The tempting design is `run_sql(query: str)` -- one tool, every question answerable,
a weekend of work. It is the wrong one, for reasons that are worth stating plainly
because they are the whole argument:

* **A string parameter is an unbounded interface.** Anything the model can write, the
  database will try. Cost, correctness and safety all become "hope the model got it
  right", and the failure is silent: a wrong join returns a number, not an error.
* **The schema becomes the API.** Renaming a column breaks callers you cannot see.
  Here the model names `spend_usd` and the mapping to a column lives in one file.
* **"Which of these?" beats "write me one".** A model choosing from an enumeration is
  doing a task it is reliable at. Composing correct SQL against an unfamiliar schema
  is a task it is *plausible* at, which is worse than unreliable.

So a question is a small typed structure -- dataset, date range, grain, dimensions,
measures, filters -- and every part of it is looked up in this registry. The model
never supplies a fragment of SQL. Identifiers come from here; literals are bound
parameters. Injection is not mitigated, it is unrepresentable: there is no path from
model output to SQL text.

The cost of this design is real and worth naming: a question nobody anticipated needs
a change here rather than a cleverer prompt. That is the trade being made -- a narrower
surface that is right, over a wider one that is usually right.
"""

from __future__ import annotations

from dataclasses import dataclass

# Grain is a fixed vocabulary rather than a date-format string, for the same reason
# everything else here is: `date_trunc` takes a unit, and taking that unit from a
# caller is a string reaching SQL.
GRAINS: dict[str, str] = {
    "day": "day",
    "week": "week",
    "month": "month",
    "quarter": "quarter",
    "year": "year",
}
TOTAL_GRAIN = "total"  # no time bucketing at all: one row per dimension combination


@dataclass(frozen=True)
class Dimension:
    """Something to group or filter by. `sql` is an expression over the mart."""

    name: str
    sql: str
    description: str


@dataclass(frozen=True)
class Measure:
    """Something to count. `sql` is an aggregate expression."""

    name: str
    sql: str
    description: str
    unit: str = ""


@dataclass(frozen=True)
class Dataset:
    name: str
    table: str
    kind: str  # "event" or "snapshot" -- see below, it changes the default behaviour
    date_column: str
    description: str
    dimensions: tuple[Dimension, ...]
    measures: tuple[Measure, ...]
    default_measures: tuple[str, ...]

    @property
    def dimension_names(self) -> list[str]:
        return [d.name for d in self.dimensions]

    @property
    def measure_names(self) -> list[str]:
        return [m.name for m in self.measures]

    def dimension(self, name: str) -> Dimension:
        for d in self.dimensions:
            if d.name == name:
                return d
        raise KeyError(name)

    def measure(self, name: str) -> Measure:
        for m in self.measures:
            if m.name == name:
                return m
        raise KeyError(name)


# --- LLM spend ----------------------------------------------------------------
# The headline dataset: small, dense, no PII, and the question it answers is one
# sentence long -- "what did I spend on Claude in August?"

LLM_SPEND = Dataset(
    name="llm_spend",
    table="marts.fct_llm_call",
    kind="event",
    date_column="partition_date",
    description=(
        "One row per LLM call, from LiteLLM's spend ledger: model, tokens, latency and "
        "cost. Prompts and completions are never stored, so nothing here can quote what "
        "was asked."
    ),
    dimensions=(
        Dimension("model", "model", "The exact model that served the call."),
        Dimension("model_group", "model_group", "The alias the call was routed through."),
        Dimension("provider", "provider", "anthropic, openai, ollama, and so on."),
        Dimension("workload", "workload", "What kind of work the call was doing."),
        Dimension("call_type", "call_type", "completion, embedding, and so on."),
        Dimension("mcp_tool", "mcp_tool", "The MCP tool involved, when there was one."),
        Dimension("status", "status", "success or failure."),
        Dimension("is_self_hosted", "is_self_hosted", "True for locally hosted models."),
        Dimension("was_cache_hit", "was_cache_hit", "True when the response was cached."),
    ),
    measures=(
        Measure("calls", "count(*)", "Number of calls.", "calls"),
        Measure("spend_usd", "round(sum(spend_usd)::numeric, 6)", "Total cost.", "USD"),
        Measure("total_tokens", "sum(total_tokens)", "Prompt plus completion tokens.", "tokens"),
        Measure("prompt_tokens", "sum(prompt_tokens)", "Input tokens.", "tokens"),
        Measure("completion_tokens", "sum(completion_tokens)", "Output tokens.", "tokens"),
        Measure("failures", "count(*) FILTER (WHERE status = 'failure')", "Failed calls.", "calls"),
        Measure("cache_hits", "count(*) FILTER (WHERE was_cache_hit)", "Cached responses.", "calls"),
        Measure("sessions", "count(DISTINCT session_id)", "Distinct sessions.", "sessions"),
        # Median rather than mean: one 90-second agent run drags an average far enough
        # to hide what a normal call actually costs in time.
        Measure("median_duration_ms", "round(median(duration_ms)::numeric, 1)",
                "Median wall-clock duration. Median, not mean, because a single long "
                "agent run distorts an average badly.", "ms"),
        Measure("median_ttft_ms", "round(median(time_to_first_token_ms)::numeric, 1)",
                "Median time to first token.", "ms"),
        Measure("usd_per_call", "round((sum(spend_usd) / nullif(count(*), 0))::numeric, 6)",
                "Mean cost per call, computed as total over count -- not an average of "
                "per-call ratios, which would weight a cheap call the same as an "
                "expensive one.", "USD"),
    ),
    default_measures=("calls", "spend_usd", "total_tokens"),
)


# --- Web traffic --------------------------------------------------------------

WEB_TRAFFIC = Dataset(
    name="web_traffic",
    table="marts.fct_http_request",
    kind="event",
    date_column="partition_date",
    description=(
        "One row per HTTP request through Nginx Proxy Manager. Client addresses never "
        "reach this table: they are hashed at ingestion, after geo and network are "
        "derived, so location and network questions work but no visitor is identifiable."
    ),
    dimensions=(
        Dimension("npm_instance", "npm_instance", "Which proxy instance served it."),
        Dimension("request_host", "request_host", "The hostname requested."),
        Dimension("path_group", "path_group", "Path bucketed into a route family."),
        Dimension("method", "method", "HTTP method."),
        Dimension("scheme", "scheme", "http or https."),
        Dimension("status", "cast(status AS VARCHAR)", "HTTP status code."),
        Dimension("status_class", "status_class", "success, redirect, client_error, server_error."),
        Dimension("client_kind", "client_kind", "The sort of client: browser, bot, and so on."),
        Dimension("client_trust", "client_trust",
                  "resolved when the real client address was known; cdn_edge when only "
                  "Cloudflare's edge was logged. See the note on `visitors`."),
        Dimension("client_ip_version", "client_ip_version", "4 or 6."),
        Dimension("is_internal", "is_internal", "True for requests from the LAN."),
        Dimension("is_automated", "is_automated", "True for bots and scanners."),
        Dimension("geo_country_code", "geo_country_code", "ISO country code, when known."),
        Dimension("geo_country_name", "geo_country_name", "Country name, when known."),
        Dimension("geo_city", "geo_city", "City, when known."),
        Dimension("geo_as_org", "geo_as_org", "The network's owner."),
    ),
    measures=(
        Measure("requests", "count(*)", "Number of requests.", "requests"),
        # The filter is load-bearing, not defensive. Before the 27 Aug 2026 real-IP
        # cutover every Cloudflare-proxied request logged the edge, so counting
        # distinct hashes across the whole history would average real people with a
        # handful of CDN machines and report a confidently wrong number.
        Measure("visitors", "count(DISTINCT client_hash) FILTER (WHERE client_trust = 'resolved')",
                "Distinct visitors, counting only requests where the real client address "
                "was known. Requests that logged only a CDN edge are excluded, so this "
                "means one thing across its whole range.", "visitors"),
        Measure("visitor_networks",
                "count(DISTINCT client_network) FILTER (WHERE client_trust = 'resolved')",
                "Distinct /24 or /48 networks.", "networks"),
        Measure("requests_behind_cdn", "count(*) FILTER (WHERE client_trust = 'cdn_edge')",
                "Requests where only the CDN edge was logged.", "requests"),
        Measure("bytes_sent", "sum(bytes_sent)", "Bytes served.", "bytes"),
        Measure("requests_success", "count(*) FILTER (WHERE status_class = 'success')",
                "2xx responses.", "requests"),
        Measure("requests_client_error", "count(*) FILTER (WHERE status_class = 'client_error')",
                "4xx responses.", "requests"),
        Measure("requests_server_error", "count(*) FILTER (WHERE status_class = 'server_error')",
                "5xx responses.", "requests"),
        Measure("server_error_pct",
                "round(100.0 * count(*) FILTER (WHERE status_class = 'server_error') "
                "/ nullif(count(*), 0), 3)",
                "Share of responses that were 5xx.", "%"),
        Measure("countries", "count(DISTINCT geo_country_code)", "Distinct countries.", "countries"),
    ),
    default_measures=("requests", "visitors", "server_error_pct"),
)


# --- Camera activity ----------------------------------------------------------

CAMERA_ACTIVITY = Dataset(
    name="camera_activity",
    table="marts.fct_camera_event",
    kind="event",
    date_column="partition_date",
    description=(
        "One row per camera attention event: which camera and zone, what the vision "
        "model made of it, and whether it was confirmed or dismissed. No zone geometry, "
        "no media, no identities -- the export refuses to send them."
    ),
    dimensions=(
        Dimension("camera_id", "camera_id", "Which camera."),
        Dimension("zone_name", "zone_name", "Which zone within the view."),
        Dimension("rule_type", "rule_type", "The rule that fired."),
        Dimension("lifecycle", "lifecycle",
                  "candidate (analysis still pending), confirmed, or dismissed."),
        Dimension("direction", "direction", "Direction of travel, when the rule reports one."),
        Dimension("zone_state", "zone_state", "The state the zone was observed in."),
        Dimension("dismissal_reason", "dismissal_reason", "Why an event was dismissed."),
        Dimension("started_weekday", "started_weekday", "Day of week, in UTC."),
        # Left as an integer, not cast to text like the other numeric dimensions:
        # this is the axis of "when does it get busy", and as text it orders
        # 0, 1, 10, 11, 2 -- which produces a chart that is simply wrong.
        Dimension("started_hour_of_day", "started_hour_of_day",
                  "Hour of day 0-23, in UTC. The dimension for 'when does it get busy'."),
        Dimension("is_weekend", "is_weekend", "True for Saturday and Sunday."),
        Dimension("is_unusual", "is_unusual", "Flagged unusual by the vision model."),
        Dimension("should_notify", "should_notify", "Would have raised a notification."),
    ),
    measures=(
        Measure("events", "count(*)", "Number of events.", "events"),
        Measure("confirmed", "count(*) FILTER (WHERE is_confirmed)", "Confirmed events.", "events"),
        Measure("dismissed", "count(*) FILTER (WHERE lifecycle = 'dismissed')",
                "Dismissed events.", "events"),
        Measure("awaiting_analysis", "count(*) FILTER (WHERE is_awaiting_analysis)",
                "Still waiting on vision analysis.", "events"),
        Measure("notified", "count(*) FILTER (WHERE should_notify)", "Would have notified.",
                "events"),
        Measure("unusual", "count(*) FILTER (WHERE is_unusual)", "Flagged unusual.", "events"),
        Measure("duplicates", "count(*) FILTER (WHERE is_duplicate)", "Marked duplicate.",
                "events"),
        # Confidence is null when the model would not commit, and avg() skips nulls, so
        # the count of scored events has to travel beside the average -- otherwise an
        # average over three events reads identically to one over three hundred.
        Measure("scored_events", "count(*) FILTER (WHERE confidence IS NOT NULL)",
                "Events the model gave a confidence for. Always read avg_confidence "
                "beside this: the average skips unscored events silently.", "events"),
        Measure("avg_confidence", "round(avg(confidence)::numeric, 4)",
                "Mean confidence over scored events only.", ""),
        Measure("median_duration_seconds", "round(median(duration_seconds)::numeric, 1)",
                "Median event duration.", "s"),
    ),
    default_measures=("events", "confirmed", "dismissed"),
)


# --- Media library ------------------------------------------------------------
# A snapshot, not a stream of events -- which changes what a date range means. See
# the note in query.py: summing titles across snapshot dates counts the same film
# once per day it existed.

MEDIA_LIBRARY = Dataset(
    name="media_library",
    table="marts.fct_library_title",
    kind="snapshot",
    date_column="snapshot_date",
    description=(
        "The film library as it stood on a given day: one row per title per snapshot. "
        "No file paths, no playback history, no per-user data. Because it is a snapshot "
        "rather than a stream of events, a query returns a single day -- the latest "
        "snapshot on or before the end of the range."
    ),
    dimensions=(
        Dimension("resolution_class", "resolution_class", "4K, 1080p, 720p, SD, low."),
        Dimension("video_codec", "video_codec", "Video codec."),
        Dimension("audio_codec", "audio_codec", "Audio codec."),
        Dimension("container", "container", "File container."),
        Dimension("decade", "cast(decade AS VARCHAR)", "Production decade."),
        Dimension("production_year", "cast(production_year AS VARCHAR)", "Year of production."),
        Dimension("official_rating", "official_rating", "Age rating."),
        Dimension("studio", "studio", "Production studio."),
        Dimension("is_long_feature", "is_long_feature", "Runtime over the long-feature cut-off."),
        Dimension("is_high_bit_depth", "is_high_bit_depth", "10-bit or better."),
        Dimension("is_enrichable", "is_enrichable",
                  "True when the title carries a TMDB id. Named as a column so a query "
                  "can exclude unenrichable titles rather than silently averaging over "
                  "them."),
        Dimension("is_runtime_plausible", "is_runtime_plausible",
                  "False for the handful of DVD rips that report a runtime of over a "
                  "day. Kept as reported and flagged, rather than corrected or dropped."),
        # From TMDB, joined on the id Jellyfin stored. All null when no API key is
        # configured, which is why `is_enriched` is a dimension rather than a footnote:
        # a query can exclude unenriched titles instead of averaging over their nulls.
        Dimension("production_country", "production_country",
                  "Primary production country. From the library snapshot, so it is "
                  "populated regardless of whether TMDB enrichment is configured."),
        Dimension("original_language", "original_language",
                  "Language the film was shot in. Needs TMDB enrichment -- the library "
                  "has no field for it -- so it is null when no API key is set."),
        Dimension("is_enriched", "is_enriched",
                  "True when the TMDB figures were fetched for this title. Affects only "
                  "budget, revenue, popularity, vote count and original language; "
                  "ratings, titles, countries and dates come from the library itself."),
    ),
    measures=(
        Measure("titles", "count(DISTINCT item_id)", "Distinct titles.", "titles"),
        Measure("runtime_hours",
                "round((sum(runtime_minutes) FILTER (WHERE is_runtime_plausible) / 60.0)::numeric, 1)",
                "Total runtime, excluding implausible values.", "hours"),
        Measure("avg_runtime_minutes",
                "round(avg(runtime_minutes) FILTER (WHERE is_runtime_plausible)::numeric, 1)",
                "Mean runtime, excluding implausible values.", "min"),
        # This *is* the TMDB rating. Jellyfin fetches it from TMDB during a scan and
        # stores it as CommunityRating, at [rate withheld] coverage -- so there is no second
        # rating measure behind an API key, because there is no second rating.
        Measure("avg_community_rating", "round(avg(community_rating)::numeric, 2)",
                "Mean rating over titles that have one. This is TMDB's vote average, "
                "which the library already holds -- no enrichment needed.", ""),
        Measure("rated_titles", "count(*) FILTER (WHERE community_rating IS NOT NULL)",
                "Titles carrying a community rating.", "titles"),
        Measure("enrichable_titles", "count(*) FILTER (WHERE is_enrichable)",
                "Titles carrying a TMDB id.", "titles"),
        Measure("enriched_titles", "count(*) FILTER (WHERE is_enriched)",
                "Titles the TMDB figures were fetched for. Read this beside the budget, "
                "revenue and vote measures: they cover enriched titles only.", "titles"),
        Measure("total_votes", "sum(tmdb_vote_count)",
                "TMDB votes across the shelf. Needs enrichment; the rating itself does "
                "not.", "votes"),
        Measure("median_budget_usd", "round(median(budget_usd)::numeric, 0)",
                "Median production budget over titles TMDB has a figure for. Median "
                "rather than mean: one blockbuster moves an average of a few hundred "
                "films a long way.", "USD"),
        Measure("median_revenue_usd", "round(median(revenue_usd)::numeric, 0)",
                "Median box office, over titles TMDB has a figure for.", "USD"),
        Measure("titles_with_budget", "count(*) FILTER (WHERE budget_usd IS NOT NULL)",
                "Titles TMDB records a budget for. The denominator for the budget "
                "measures -- an unknown budget is null, not zero.", "titles"),
    ),
    default_measures=("titles", "runtime_hours", "avg_community_rating"),
)


DATASETS: dict[str, Dataset] = {
    d.name: d for d in (LLM_SPEND, WEB_TRAFFIC, CAMERA_ACTIVITY, MEDIA_LIBRARY)
}


def get_dataset(name: str) -> Dataset:
    try:
        return DATASETS[name]
    except KeyError:
        raise KeyError(
            f"unknown dataset {name!r}. Available: {sorted(DATASETS)}"
        ) from None


def validation_statements(dataset: Dataset) -> list[tuple[str, str]]:
    """Statements that bind every expression this dataset declares, and return nothing.

    `WHERE false` means the binder does all its work -- resolving every column and
    every function -- while the scan returns no rows, so the check costs nothing and
    still fails on a column that is gone.

    This exists because the semantic layer names columns in a mart it does not own.
    Renaming a column in dbt would otherwise leave a tool that compiles, deploys, and
    fails only when somebody asks the question it serves.
    """
    dims = ", ".join(f'{d.sql} AS "{d.name}"' for d in dataset.dimensions)
    measures = ", ".join(f'{m.sql} AS "{m.name}"' for m in dataset.measures)
    # S608: every fragment is a literal defined in this module. Nothing here is
    # reachable from a caller -- that is the property the whole file exists to hold.
    return [
        ("date_column", f"SELECT {dataset.date_column} FROM {dataset.table} WHERE false"),  # noqa: S608
        ("dimensions", f"SELECT {dims} FROM {dataset.table} WHERE false"),  # noqa: S608
        ("measures", f"SELECT {measures} FROM {dataset.table} WHERE false"),  # noqa: S608
    ]


def validate(con) -> list[str]:
    """Bind every dataset against a live database. Returns a list of problems."""
    problems: list[str] = []
    for dataset in DATASETS.values():
        for part, sql in validation_statements(dataset):
            try:
                con.execute(sql).fetchall()
            except Exception as error:  # noqa: BLE001 - every failure is reported, not
                # just the first, so one deploy fixes all of them rather than one a day.
                problems.append(f"{dataset.name}.{part}: {error}")
    return problems
