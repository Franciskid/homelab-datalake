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

# The bottom of the ladder: no bucketing and no aggregation, one row per record with
# its own timestamp. It lives in the same vocabulary as the other grains because that
# is the one place a caller reliably looks -- `grains` is in every list_datasets and
# describe_dataset response. Offering it as a separate tool instead would leave a
# model reading "day, week, month, quarter, year, total" and concluding, reasonably
# and wrongly, that the finest thing this data can say is a day.
EVENT_GRAIN = "event"
ALL_GRAINS: list[str] = [*GRAINS, TOTAL_GRAIN, EVENT_GRAIN]


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
class Attribute:
    """A column of one record, projected as itself rather than aggregated.

    The distinction from Dimension is not cosmetic. A dimension is something you may
    group by, so it has to be low enough in cardinality for a GROUP BY to mean
    something -- which is exactly why `started_at` was never one. An attribute has no
    such constraint: it is only ever selected, one value per row, and a timestamp
    unique to every event is the normal case rather than a mistake.

    Without this distinction a mart column that is unique per row has nowhere to live
    in the semantic layer, and the layer ends up describing the data as though the
    column did not exist.
    """

    name: str
    sql: str
    description: str


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
    # Row-level retrieval. A dataset with no attributes cannot be asked for records,
    # and says so when asked -- rather than answering with an aggregate that looks
    # like the list somebody wanted.
    attributes: tuple[Attribute, ...] = ()
    default_fields: tuple[str, ...] = ()
    # What records are ordered by when the caller does not say. Chronological, for a
    # ledger of things that happened.
    record_order: str | None = None
    # Columns of the underlying mart that are deliberately absent from this dataset,
    # each with the reason. Declaring them is what makes `missing_columns` a real
    # check rather than a list of known noise: a column that is neither exposed nor
    # named here is drift, and drift is reported.
    not_exposed: tuple[tuple[str, str], ...] = ()

    @property
    def dimension_names(self) -> list[str]:
        return [d.name for d in self.dimensions]

    @property
    def measure_names(self) -> list[str]:
        return [m.name for m in self.measures]

    @property
    def attribute_names(self) -> list[str]:
        return [a.name for a in self.attributes]

    @property
    def field_names(self) -> list[str]:
        """Everything projectable one-row-per-record: attributes and dimensions both.

        A dimension is a perfectly good column to put in a listing -- `zone_name` next
        to `started_at` is the obvious export -- and forcing it to be declared twice
        would guarantee the two lists drift.
        """
        return self.attribute_names + [d.name for d in self.dimensions
                                       if d.name not in set(self.attribute_names)]

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

    def field(self, name: str) -> Attribute | Dimension:
        for a in self.attributes:
            if a.name == name:
                return a
        return self.dimension(name)


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
        Dimension("team_id", "team_id", "The LiteLLM team the key belongs to."),
        Dimension("client_trust", "client_trust",
                  "resolved when the caller's address was known, unknown otherwise."),
        Dimension("geo_country_code", "geo_country_code", "Where the call came from."),
        Dimension("geo_as_org", "geo_as_org", "The calling network's owner."),
    ),
    measures=(
        Measure("calls", "count(*)", "Number of calls.", "calls"),
        Measure("spend_usd", "round(sum(spend_usd)::decimal(18, 9), 6)", "Total cost.", "USD"),
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
        Measure("usd_per_call", "round((sum(spend_usd) / nullif(count(*), 0))::decimal(18, 9), 6)",
                "Mean cost per call, computed as total over count -- not an average of "
                "per-call ratios, which would weight a cheap call the same as an "
                "expensive one.", "USD"),
    ),
    default_measures=("calls", "spend_usd", "total_tokens"),
    attributes=(
        Attribute("call_key", "call_key", "The call's stable id."),
        Attribute("partition_date", "partition_date", "The UTC day the call belongs to."),
        Attribute("started_at", "started_at", "When the call started, to the microsecond, UTC."),
        Attribute("started_hour", "started_hour", "started_at truncated to the hour."),
        Attribute("spend_usd", "spend_usd", "What this one call cost."),
        Attribute("usd_per_token", "usd_per_token", "Cost per token for this call."),
        Attribute("total_tokens", "total_tokens", "Prompt plus completion tokens."),
        Attribute("prompt_tokens", "prompt_tokens", "Input tokens."),
        Attribute("completion_tokens", "completion_tokens", "Output tokens."),
        Attribute("duration_ms", "duration_ms", "Wall-clock duration."),
        Attribute("time_to_first_token_ms", "time_to_first_token_ms", "Time to first token."),
        Attribute("session_id", "session_id", "The session the call belonged to."),
        Attribute("api_key_hash", "api_key_hash",
                  "Pseudonym of the key that made the call -- the key itself is never "
                  "stored, so this counts and groups but does not authenticate."),
        Attribute("end_user_hash", "end_user_hash", "Pseudonym of the end user."),
        Attribute("client_hash", "client_hash", "Pseudonym of the calling address."),
    ),
    default_fields=(
        "started_at", "model", "workload", "status", "spend_usd", "total_tokens",
        "duration_ms", "session_id", "call_key",
    ),
    record_order="started_at",
)


# --- Web traffic --------------------------------------------------------------

WEB_TRAFFIC = Dataset(
    name="web_traffic",
    table="marts.fct_http_request",
    kind="event",
    date_column="partition_date",
    description=(
        "One row per HTTP request: both Nginx Proxy Manager instances, plus "
        "mcp.ai.example.test's own access log (npm_instance='mcp-api') from 2026-09-14. "
        "Client addresses are hashed at ingestion, after geo and network are derived, "
        "so location and network questions work without needing an identifiable "
        "visitor. Since 2026-09-14 the raw address and the raw query string are also "
        "kept, in client_address and query_string -- read those instead of the hash "
        "only for a reason that needs the real value, such as looking up one request "
        "by the address that made it. Both are null before that date, and null for any "
        "older row whose source line had already rotated off NPM's disk (roughly a "
        "five-week window) by the day this shipped."
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
        Dimension("upstream_status", "cast(upstream_status AS VARCHAR)",
                  "What the backend returned, before the proxy had its say."),
        Dimension("log_format", "log_format", "Which proxy log format the row was parsed from."),
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
    attributes=(
        Attribute("request_key", "request_key", "The request's stable id."),
        Attribute("partition_date", "partition_date", "The UTC day the request belongs to."),
        Attribute("requested_at", "requested_at", "When it arrived, to the second, in UTC."),
        Attribute("requested_hour", "requested_hour", "requested_at truncated to the hour."),
        # Not a dimension: one path per request makes a GROUP BY meaningless, which
        # is what path_group exists for. As a field in a listing it is the thing you
        # actually wanted to read.
        Attribute("path", "path", "The exact path requested, query string excluded."),
        # query_string and client_address are Attributes for the same reason path is
        # and not a Dimension: a raw query string and a raw address are close to
        # unique per request, so a GROUP BY on either would not group anything. Both
        # are raw and private-warehouse-only, added 2026-09-14 -- see the dataset
        # description and homelab_datalake.privacy.
        Attribute("query_string", "query_string", "The raw query string, without the leading '?'."),
        Attribute("upstream_target", "upstream_target", "Which backend served it."),
        Attribute("bytes_sent", "bytes_sent", "Response size."),
        Attribute("user_agent", "user_agent", "The client's user agent string."),
        Attribute("client_address", "client_address",
                  "The client address, raw. client_hash below is the pseudonym most "
                  "questions should use; this is for looking one request up by the "
                  "address that made it."),
        Attribute("client_hash", "client_hash",
                  "Pseudonym of the client address. The address itself is discarded at "
                  "ingestion, after geo and network are derived from it."),
        Attribute("client_network", "client_network", "The /24 or /48 the client sat in."),
        Attribute("geo_asn", "geo_asn", "The network's autonomous system number."),
    ),
    default_fields=(
        "requested_at", "request_host", "method", "path", "status", "bytes_sent",
        "client_kind", "geo_country_code", "request_key",
    ),
    record_order="requested_at",
)


# --- Camera activity ----------------------------------------------------------

CAMERA_ACTIVITY = Dataset(
    name="camera_activity",
    table="marts.fct_camera_event",
    kind="event",
    date_column="partition_date",
    description=(
        "One row per camera attention event: which camera and zone, when it happened "
        "to the microsecond, what the vision model made of it, how many people, "
        "vehicles and animals it counted, whether a human later corrected it, and the "
        "clip it belongs to. Ask for grain='event' to get the events themselves with "
        "their exact timestamps; any other grain aggregates them."
    ),
    dimensions=(
        Dimension("camera_id", "camera_id", "Which camera."),
        Dimension("zone_id", "zone_id", "The zone's stable id."),
        Dimension("zone_name", "zone_name", "Which zone within the view."),
        Dimension("rule_id", "rule_id", "The stable id of the rule that fired."),
        Dimension("rule_type", "rule_type", "The rule that fired."),
        Dimension("lifecycle", "lifecycle",
                  "candidate (analysis still pending), confirmed, or dismissed."),
        Dimension("is_confirmed", "is_confirmed", "True for confirmed events."),
        Dimension("is_dismissed", "is_dismissed", "True for dismissed events."),
        Dimension("is_awaiting_analysis", "is_awaiting_analysis",
                  "True while the vision analysis has not come back."),
        Dimension("direction", "direction", "Direction of travel, when the rule reports one."),
        Dimension("zone_state", "zone_state", "The state the zone was observed in."),
        Dimension("previous_zone_state", "previous_zone_state",
                  "The state it was in before. Read with state_changed: a state event "
                  "that repeats the previous state is an observation, not a transition."),
        Dimension("state_changed", "state_changed",
                  "True when this event actually changed the zone's state. Filter on "
                  "this to count openings and closings rather than observations."),
        Dimension("state_source", "state_source",
                  "How the state was decided: the vision model, a rule, or a person."),
        Dimension("dismissal_reason", "dismissal_reason", "Why an event was dismissed."),
        Dimension("is_duplicate", "is_duplicate", "True when folded into another event."),
        Dimension("started_weekday", "started_weekday", "Day of week, in UTC."),
        # Left as an integer, not cast to text like the other numeric dimensions:
        # this is the axis of "when does it get busy", and as text it orders
        # 0, 1, 10, 11, 2 -- which produces a chart that is simply wrong.
        Dimension("started_hour_of_day", "started_hour_of_day",
                  "Hour of day 0-23, in UTC. The dimension for 'when does it get busy'. "
                  "This is a bucket: for the actual time an event happened, ask for "
                  "grain='event' and the happened_at field."),
        Dimension("is_weekend", "is_weekend", "True for Saturday and Sunday."),
        Dimension("is_unusual", "is_unusual", "Flagged unusual by the vision model."),
        Dimension("should_notify", "should_notify", "Would have raised a notification."),
        Dimension("was_corrected", "was_corrected",
                  "True when a person overrode what the model concluded."),
        Dimension("corrected_by_name", "corrected_by_name", "Who made the correction."),
        Dimension("lifecycle_was_corrected", "lifecycle_was_corrected",
                  "True when the correction changed confirmed/dismissed."),
        Dimension("zone_state_was_corrected", "zone_state_was_corrected",
                  "True when the correction changed open/closed."),
        Dimension("has_clip", "has_clip", "True when a recording is attached."),
        Dimension("clip_kind", "clip_kind", "What sort of recording it is."),
        Dimension("clip_storage_quality", "clip_storage_quality", "The quality it was kept at."),
        Dimension("clip_ai_source", "clip_ai_source", "Which analyser produced the clip's verdict."),
        Dimension("clip_ai_type", "clip_ai_type", "The kind of analysis run on the clip."),
        Dimension("clip_is_archived", "clip_is_archived", "True for archived clips."),
        Dimension("clip_is_deleted", "clip_is_deleted", "True once the media is gone."),
    ),
    measures=(
        Measure("events", "count(*)", "Number of events.", "events"),
        Measure("confirmed", "count(*) FILTER (WHERE is_confirmed)", "Confirmed events.", "events"),
        Measure("dismissed", "count(*) FILTER (WHERE is_dismissed)",
                "Dismissed events.", "events"),
        Measure("awaiting_analysis", "count(*) FILTER (WHERE is_awaiting_analysis)",
                "Still waiting on vision analysis.", "events"),
        Measure("notified", "count(*) FILTER (WHERE should_notify)", "Would have notified.",
                "events"),
        Measure("unusual", "count(*) FILTER (WHERE is_unusual)", "Flagged unusual.", "events"),
        Measure("duplicates", "count(*) FILTER (WHERE is_duplicate)", "Marked duplicate.",
                "events"),
        Measure("state_changes", "count(*) FILTER (WHERE state_changed)",
                "Events that actually changed the zone's state -- an opening or a "
                "closing, rather than an observation of a state already held.", "events"),
        # These five are why the dataset exists at all for the person who asked for
        # it: an alerts chart is people-per-hour, and before these it could only be
        # rebuilt by exporting rows and summing them somewhere else.
        Measure("people", "sum(count_person)", "People counted across the events.", "people"),
        Measure("vehicles", "sum(count_vehicle)", "Vehicles counted.", "vehicles"),
        Measure("animals", "sum(count_animal)", "Animals counted.", "animals"),
        Measure("other_objects", "sum(count_other)", "Anything else counted.", "objects"),
        Measure("objects", "sum(count_total)", "Everything counted, of any kind.", "objects"),
        # A min and a max over a timestamp, which is what makes "when did the gate
        # first open that day" answerable at grain='day' instead of needing the rows.
        Measure("first_event_at", "min(happened_at)",
                "Earliest event in the bucket, to the microsecond. With a filter on "
                "zone_state this is the first opening (or closing) of the day.", "UTC"),
        Measure("last_event_at", "max(happened_at)",
                "Latest event in the bucket, to the microsecond.", "UTC"),
        Measure("corrections", "count(*) FILTER (WHERE was_corrected)",
                "Events a person overrode.", "events"),
        Measure("events_with_clip", "count(*) FILTER (WHERE has_clip)",
                "Events with a recording attached.", "events"),
        Measure("clip_bytes", "sum(clip_size_bytes)", "Recorded footage, in bytes.", "bytes"),
        Measure("annotations", "sum(clip_annotation_count)",
                "Notes people left on the attached clips.", "annotations"),
        # Confidence is null when the model would not commit, and avg() skips nulls, so
        # the count of scored events has to travel beside the average -- otherwise an
        # average over three events reads identically to one over three hundred.
        Measure("scored_events", "count(*) FILTER (WHERE confidence IS NOT NULL)",
                "Events the model gave a confidence for. Always read avg_confidence "
                "beside this: the average skips unscored events silently.", "events"),
        Measure("avg_confidence", "round(avg(confidence)::decimal(18, 9), 4)",
                "Mean confidence over scored events only.", ""),
        Measure("median_duration_seconds", "round(median(duration_seconds)::numeric, 1)",
                "Median event duration.", "s"),
        Measure("total_duration_seconds", "sum(duration_seconds)",
                "Time the zone spent in this state, summed.", "s"),
    ),
    default_measures=("events", "confirmed", "dismissed"),
    # --- row level -------------------------------------------------------------
    attributes=(
        Attribute("event_id", "event_id", "The event's stable id. Joins to fct_camera_clip."),
        Attribute("partition_date", "partition_date", "The UTC day the event belongs to."),
        Attribute("happened_at", "happened_at",
                  "When it happened, to the microsecond, in UTC. The column to order a "
                  "listing by: it is occurred_at when the camera reported one and "
                  "started_at otherwise, so it is never null."),
        Attribute("started_at", "started_at", "When the event opened, in UTC."),
        Attribute("occurred_at", "occurred_at",
                  "The instant the camera itself attributes the event to, when it "
                  "reports one. Null on events that never had it."),
        Attribute("completed_at", "completed_at", "When the event closed, in UTC."),
        Attribute("state_observed_at", "state_observed_at",
                  "When the zone state in this row was observed."),
        Attribute("started_hour", "started_hour", "started_at truncated to the hour."),
        Attribute("duration_seconds", "duration_seconds", "How long the event lasted."),
        Attribute("summary", "summary", "What the vision model said about it, in words."),
        Attribute("confidence", "confidence", "The model's confidence, 0 to 1, or null."),
        Attribute("count_person", "count_person", "People counted in this event."),
        Attribute("count_vehicle", "count_vehicle", "Vehicles counted in this event."),
        Attribute("count_animal", "count_animal", "Animals counted in this event."),
        Attribute("count_other", "count_other", "Anything else counted in this event."),
        Attribute("count_total", "count_total", "Everything counted in this event."),
        Attribute("duplicate_of", "duplicate_of", "The event this one was folded into."),
        # The correction block. The row already carries the corrected value in
        # lifecycle, zone_state and the rest; these say that it was corrected, by
        # whom, and what the machine had originally concluded.
        Attribute("corrected_at", "corrected_at", "When the correction was made."),
        Attribute("corrected_by_hash", "corrected_by_hash",
                  "Pseudonym of whoever corrected it, for counting without naming."),
        Attribute("machine_lifecycle", "machine_lifecycle",
                  "The lifecycle the model concluded, before any correction."),
        Attribute("machine_direction", "machine_direction", "The direction the model concluded."),
        Attribute("machine_zone_state", "machine_zone_state", "The state the model concluded."),
        Attribute("machine_previous_zone_state", "machine_previous_zone_state",
                  "The previous state the model recorded."),
        Attribute("machine_state_observed_at", "machine_state_observed_at",
                  "When the model observed the state, before correction."),
        Attribute("machine_dismissal_reason", "machine_dismissal_reason",
                  "Why the model dismissed it, before correction."),
        Attribute("machine_duplicate_of", "machine_duplicate_of",
                  "What the model folded it into, before correction."),
        Attribute("machine_summary", "machine_summary", "The model's original wording."),
        Attribute("machine_confidence", "machine_confidence", "The model's original confidence."),
        Attribute("machine_should_notify", "machine_should_notify",
                  "Whether the model would have notified, before correction."),
        Attribute("machine_count_person", "machine_count_person", "People the model counted."),
        Attribute("machine_count_vehicle", "machine_count_vehicle", "Vehicles the model counted."),
        Attribute("machine_count_animal", "machine_count_animal", "Animals the model counted."),
        Attribute("machine_count_other", "machine_count_other", "Other objects the model counted."),
        # The clip.
        Attribute("clip_id", "clip_id", "The recording attached to this event."),
        Attribute("transition_clip_id", "transition_clip_id",
                  "The recording of the state transition, when one was kept separately."),
        Attribute("clip_title", "clip_title", "The recording's title."),
        Attribute("clip_duration_seconds", "clip_duration_seconds", "How long the recording runs."),
        Attribute("clip_size_bytes", "clip_size_bytes", "How large the recording is."),
        Attribute("clip_local_confidence", "clip_local_confidence",
                  "The on-device analyser's confidence in the clip."),
        Attribute("clip_annotation_count", "clip_annotation_count",
                  "How many notes people left on it."),
        # The four lifecycle blobs, kept as JSON because their shape is the source's
        # to change. They are here so "everything the camera recorded" is literally
        # true rather than nearly true.
        Attribute("backfill_json", "backfill_json",
                  "The backfill record, as JSON, when the event was reconstructed."),
        Attribute("repair_json", "repair_json", "The repair record, as JSON."),
        Attribute("restoration_json", "restoration_json", "The restoration record, as JSON."),
        Attribute("timing_correction_json", "timing_correction_json",
                  "The timing correction record, as JSON."),
        Attribute("unmapped_json", "unmapped_json",
                  "Any source field the lake has no typed column for yet. Normally null; "
                  "when it is not, the source has grown a field and this is it."),
        Attribute("extracted_at", "extracted_at", "When the lake read this event."),
    ),
    default_fields=(
        "happened_at", "camera_id", "zone_name", "rule_type", "zone_state",
        "previous_zone_state", "state_changed", "direction", "lifecycle",
        "count_person", "count_vehicle", "count_animal", "count_total",
        "duration_seconds", "confidence", "summary", "was_corrected", "event_id",
    ),
    record_order="happened_at",
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
        "The **movie** library as it stood on a given day: one row per film per "
        "snapshot. No file paths, and no playback or per-user data here specifically "
        "-- for who watched what and for how long, ask media_playback instead. "
        "Movies only: series, seasons, episodes, box sets and other videos are a "
        "separate, much larger catalogue (roughly [count withheld] items against [count withheld] movies) "
        "landed in the warehouse's fct_library_item as a plain daily snapshot, not "
        "yet exposed through this semantic layer. Because this dataset is a snapshot "
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
    attributes=(
        Attribute("title_key", "title_key", "The row's stable id for this snapshot."),
        Attribute("item_id", "item_id", "The title's Jellyfin id, stable across snapshots."),
        Attribute("snapshot_date", "snapshot_date", "The day this snapshot was taken."),
        Attribute("title", "title", "The title as the library shows it."),
        Attribute("original_title", "original_title", "The title in its original language."),
        Attribute("premiere_date", "premiere_date", "When it was released."),
        Attribute("added_at", "added_at", "When it was added to the library."),
        Attribute("runtime_minutes", "runtime_minutes",
                  "Runtime as reported. Check is_runtime_plausible before averaging it."),
        Attribute("community_rating", "community_rating", "TMDB's vote average."),
        Attribute("critic_rating", "critic_rating", "The critic score, when there is one."),
        Attribute("video_height", "video_height", "Frame height in pixels."),
        Attribute("video_width", "video_width", "Frame width in pixels."),
        Attribute("audio_channels", "audio_channels", "Number of audio channels."),
        Attribute("tmdb_id", "tmdb_id", "The TMDB id, when the library knows one."),
        Attribute("imdb_id", "imdb_id", "The IMDb id, when the library knows one."),
        Attribute("budget_usd", "budget_usd", "Production budget, from TMDB."),
        Attribute("revenue_usd", "revenue_usd", "Box office, from TMDB."),
        Attribute("revenue_to_budget", "revenue_to_budget",
                  "Revenue over budget. Null when either is missing -- not zero."),
        Attribute("tmdb_popularity", "tmdb_popularity", "TMDB's popularity score."),
        Attribute("tmdb_vote_count", "tmdb_vote_count", "How many people voted on TMDB."),
    ),
    default_fields=(
        "title", "production_year", "resolution_class", "runtime_minutes",
        "community_rating", "video_codec", "studio", "item_id",
    ),
    record_order="title",
)


# --- Media playback ------------------------------------------------------------
# Added 2026-09-14, the day the owner reversed the decision that had kept this out of
# the warehouse -- see the docstring at the top of parsing/jellyfin.py for why.

MEDIA_PLAYBACK = Dataset(
    name="media_playback",
    table="marts.fct_jellyfin_playback",
    kind="event",
    date_column="partition_date",
    description=(
        "One row per playback session from the Playback Reporting plugin: who "
        "played what, on what client and device, for how long the player was open. "
        "That last point is not a technicality -- the plugin records how long the "
        "player stayed open, which is not proof anything was watched to completion. "
        "A paused film left open overnight and a film actually finished both "
        "accumulate seconds the same way. An episode's series is resolved by id "
        "against the library's series/season/episode catalogue where that reaches "
        "it; series_resolved_by_id says whether a given row used that or fell back "
        "to a name parsed from the title (measured 2026-09-15: [rate withheld] of episode "
        "rows). genre is the title's single primary genre rather than the full "
        "list, to avoid multiplying watch time by how many genres a film carries, "
        "and is only ever populated for a Movie -- the catalogue that resolves "
        "series and season does not carry genres per episode."
    ),
    dimensions=(
        Dimension("person", "user_name", "Who played it, by account name. Null for "
                  "a session whose account no longer appears in the current "
                  "snapshot -- see user_id in the row-level fields for the raw id."),
        Dimension("title", "title", "The title as the plugin recorded it."),
        Dimension("series", "series_name",
                  "The show a played episode belongs to -- resolved by id against "
                  "the library where that reaches it, parsed from the episode's own "
                  "title otherwise. Null for anything that is not an episode. See "
                  "series_resolved_by_id for which source supplied a given row."),
        Dimension("media_type", "item_type",
                  "Movie, Episode, Video (a home recording outside the library's "
                  "scope) or TvChannel."),
        Dimension("genre", "genre",
                  "The title's primary genre. Only ever populated for a Movie -- "
                  "the library never catalogues a genre for a series or an episode."),
        Dimension("series_resolved_by_id", "series_resolved_by_id",
                  "True when series/season/episode came from an id join against the "
                  "library catalogue; false when they fell back to a name parsed "
                  "from the title, or when the session is not an episode at all."),
        Dimension("client", "client_name", "The Jellyfin client used to play it."),
        Dimension("device", "device_name", "The physical device or browser."),
        Dimension("playback_method", "playback_method",
                  "DirectPlay, DirectStream, or a Transcode naming the codec path."),
        Dimension("weekday", "started_weekday", "Day of week the session started, in UTC."),
        Dimension("hour_of_day", "started_hour_of_day",
                  "Hour of day the session started, 0 to 23, in UTC."),
        Dimension("is_weekend", "is_weekend", "True for Saturday and Sunday, in UTC."),
        Dimension("is_in_library", "is_in_library",
                  "True when the item resolves against the current movie library. "
                  "Always false for an episode, a home video or a TV channel."),
        Dimension("is_episode", "is_episode", "True when item_type is Episode."),
    ),
    measures=(
        Measure("plays", "count(*)", "Number of playback sessions.", "sessions"),
        Measure("watch_hours", "round((sum(play_duration_seconds) / 3600.0)::numeric, 2)",
                "Total time the player was open, in hours. Not a finished-viewing "
                "count -- see the dataset description.", "hours"),
        Measure("watch_minutes", "round((sum(play_duration_seconds) / 60.0)::numeric, 1)",
                "Total time the player was open, in minutes.", "minutes"),
        Measure("active_viewers", "count(DISTINCT user_id)",
                "Distinct accounts that played something.", "people"),
        Measure("distinct_titles", "count(DISTINCT item_id)",
                "Distinct items played.", "titles"),
        Measure("median_session_minutes",
                "round((median(play_duration_seconds) / 60.0)::numeric, 1)",
                "Median session length. Median rather than mean: a film left open "
                "overnight drags an average far past what a normal session looks "
                "like.", "minutes"),
    ),
    default_measures=("plays", "watch_hours", "active_viewers"),
    attributes=(
        Attribute("playback_key", "playback_key", "The session's stable id."),
        Attribute("partition_date", "partition_date", "The UTC day the session started."),
        Attribute("partition_hour", "partition_hour",
                  "The start of the hourly partition this session was read for, in UTC."),
        Attribute("extracted_at", "extracted_at", "When the lake read this session."),
        Attribute("started_at", "started_at",
                  "When the session started, to the microsecond, in UTC."),
        Attribute("started_hour", "started_hour", "started_at truncated to the hour."),
        Attribute("user_id", "user_id", "The account's raw Jellyfin id."),
        Attribute("item_id", "item_id", "The item's raw Jellyfin id."),
        Attribute("series_id", "series_id",
                  "The series' raw Jellyfin id, when series_resolved_by_id is true. "
                  "Null on the name-parse fallback -- there is no id to fall back to."),
        Attribute("season_number", "season_number",
                  "The season number, when the id join resolves. No parsed "
                  "fallback exists for this -- a title alone does not carry it."),
        Attribute("episode_number", "episode_number",
                  "The episode number, when the id join resolves. No parsed "
                  "fallback exists for this either."),
        Attribute("play_duration_seconds", "play_duration_seconds",
                  "How long the player was open, in seconds."),
        Attribute("library_community_rating", "library_community_rating",
                  "TMDB's vote average for the title, when it resolves against the "
                  "movie library. Null otherwise."),
    ),
    default_fields=(
        "started_at", "person", "title", "series", "media_type", "play_duration_seconds",
        "client", "device", "playback_key",
    ),
    record_order="started_at",
)


# --- Server activity -------------------------------------------------------------

MEDIA_SERVER_ACTIVITY = Dataset(
    name="media_server_activity",
    table="marts.fct_jellyfin_activity",
    kind="event",
    date_column="partition_date",
    description=(
        "One row per Jellyfin server activity log entry: sign-ins, playback starts "
        "and stops, library scans, account changes -- whatever Jellyfin itself "
        "decided to log, with its own free-text description. person is null for a "
        "large share of rows on purpose: many entries are not about an account at "
        "all, and among the ones that are, some name an account that has since been "
        "removed and no longer appears in the current snapshot."
    ),
    dimensions=(
        Dimension("activity_type", "activity_type",
                  "The kind of entry -- VideoPlayback, SessionStarted, "
                  "AuthenticationSucceeded, and so on."),
        Dimension("severity", "severity", "Information, Warning, or Error."),
        Dimension("person", "user_name",
                  "The account the entry concerns, by name, when it concerns one "
                  "and that account still appears in the current snapshot."),
        Dimension("weekday", "happened_weekday", "Day of week the entry was logged, in UTC."),
        Dimension("hour_of_day", "happened_hour_of_day",
                  "Hour of day the entry was logged, 0 to 23, in UTC."),
        Dimension("is_weekend", "is_weekend", "True for Saturday and Sunday, in UTC."),
    ),
    measures=(
        Measure("entries", "count(*)", "Number of activity log entries.", "entries"),
        Measure("distinct_users", "count(DISTINCT user_id)",
                "Distinct accounts named across the entries.", "people"),
        Measure("errors", "count(*) FILTER (WHERE is_error)", "Entries logged at Error severity.", "entries"),
        Measure("warnings", "count(*) FILTER (WHERE is_warning)", "Entries logged at Warning severity.", "entries"),
    ),
    default_measures=("entries", "errors", "warnings"),
    attributes=(
        Attribute("activity_id", "activity_id", "The activity log's own stable id."),
        Attribute("partition_date", "partition_date", "The UTC day the entry was logged."),
        Attribute("extracted_at", "extracted_at", "When the lake read this entry."),
        Attribute("happened_at", "happened_at", "When it was logged, to the second, in UTC."),
        Attribute("happened_hour", "happened_hour", "happened_at truncated to the hour."),
        Attribute("name", "name", "Jellyfin's own free-text description of the entry."),
        Attribute("overview", "overview", "A longer free-text note, when there is one."),
        Attribute("short_overview", "short_overview", "A short free-text note, when there is one."),
        Attribute("user_id", "user_id", "The account's raw Jellyfin id, when the entry names one."),
        Attribute("item_id", "item_id", "The item's raw Jellyfin id, when the entry names one."),
        Attribute("unmapped_json", "unmapped_json",
                  "JSON, or null. Any field the activity log sent that the raw schema "
                  "does not name."),
    ),
    default_fields=(
        "happened_at", "activity_type", "name", "severity", "person", "activity_id",
    ),
    record_order="happened_at",
)


# --- The AI platform's own database -------------------------------------------
# Six datasets over the marts in fct_ai_gateway_call.sql through
# fct_automation_run.sql -- see homelab_datalake.parsing.ai_platform_registry for
# where all of it comes from, and the owner's 2026-09-14 decision to land it in
# full. The caveats below are not decoration: a model reading these needs to know
# a streamed call can legitimately report zero tokens, that prompt/response/content
# are full, untruncated text, and that a camera frame is a file referenced by a key,
# not a value sitting in the row.

AI_GATEWAY_CALLS = Dataset(
    name="ai_gateway_calls",
    table="marts.fct_ai_gateway_call",
    kind="event",
    date_column="partition_date",
    description=(
        "One row per call through the api.ai gateway: application, key, person, "
        "requested and actual model, full prompt and response, tokens, cost, "
        "latency, status, and any camera frames attached. A streamed call whose "
        "caller never asked the gateway for usage figures reports zero tokens -- "
        "that is a real state, not evidence the call was free, and summing tokens "
        "over a period that includes such calls understates the true count. "
        "`prompt` and `response` are the full text sent and received, by the "
        "owner's 2026-09-14 decision. A camera frame is never a value in this "
        "table: `frame_keys` names the lake objects it was decoded to."
    ),
    dimensions=(
        Dimension("application_name", "application_name", "Which application made the call."),
        Dimension("key_label", "key_label", "The API key's human-readable name."),
        Dimension("call_source", "call_source",
                  "What kind of caller this was -- an agent run, a schedule, a script."),
        Dimension("client_name", "client_name", "The calling client's self-reported name."),
        Dimension("requested_model", "requested_model", "The model alias the caller asked for."),
        Dimension("actual_model", "actual_model", "The model that actually served the call."),
        Dimension("was_rerouted", "was_rerouted",
                  "True when the served model differs from the one requested."),
        Dimension("status", "status", "success, failure, or whatever else the gateway recorded."),
        Dimension("failure_code", "failure_code", "Why the call failed, when it did."),
        Dimension("has_frames", "has_frames", "True when a camera frame was attached to this call."),
    ),
    measures=(
        Measure("calls", "count(*)", "Number of calls.", "calls"),
        Measure("cost_eur", "round(sum(cost_eur)::decimal(18, 6), 6)", "Total cost.", "EUR"),
        Measure("input_tokens", "sum(input_tokens)", "Prompt tokens. Zero is real -- see the dataset note.", "tokens"),
        Measure("output_tokens", "sum(output_tokens)", "Completion tokens.", "tokens"),
        Measure("total_tokens", "sum(total_tokens)", "Prompt plus completion tokens.", "tokens"),
        Measure("failures", "count(*) FILTER (WHERE status = 'failure')", "Failed calls.", "calls"),
        Measure("calls_with_frames", "count(*) FILTER (WHERE has_frames)",
                "Calls with at least one camera frame attached.", "calls"),
        Measure("frames", "sum(frame_count)", "Camera frames decoded across the calls.", "frames"),
        Measure("frame_bytes", "sum(frame_bytes)", "Bytes of decoded camera frames.", "bytes"),
        Measure("median_latency_ms", "round(median(latency_ms)::numeric, 1)",
                "Median end-to-end latency.", "ms"),
        Measure("eur_per_call", "round((sum(cost_eur) / nullif(count(*), 0))::decimal(18, 6), 6)",
                "Mean cost per call, total over count.", "EUR"),
    ),
    default_measures=("calls", "cost_eur", "total_tokens"),
    attributes=(
        Attribute("call_key", "call_key", "The call's stable id."),
        Attribute("partition_date", "partition_date", "The UTC day the call was recorded."),
        Attribute("created_at", "created_at", "When the call was recorded, to the microsecond, UTC."),
        Attribute("created_hour", "created_hour", "created_at truncated to the hour."),
        Attribute("application_id", "application_id", "Which application made the call."),
        Attribute("api_key_id", "api_key_id", "Which API key made the call."),
        Attribute("client_version", "client_version", "The calling client's self-reported version."),
        Attribute("actor_label", "actor_label", "A human-readable label for who or what triggered the call."),
        Attribute("actor_external_id", "actor_external_id", "An external identifier for the actor."),
        Attribute("conversation_id", "conversation_id", "The conversation this call belongs to, where it belongs to one."),
        Attribute("prompt", "prompt", "The full prompt sent to the model."),
        Attribute("response", "response", "The model's full response."),
        Attribute("results", "results", "JSON. Structured tool/function-call results, where there were any."),
        Attribute("metadata", "metadata", "JSON. Everything else recorded, minus the camera frames."),
        Attribute("input_tokens", "input_tokens", "Prompt tokens for this one call."),
        Attribute("output_tokens", "output_tokens", "Completion tokens for this one call."),
        Attribute("total_tokens", "total_tokens", "Prompt plus completion tokens for this one call."),
        Attribute("cost_eur", "cost_eur", "What this one call cost."),
        Attribute("latency_ms", "latency_ms", "End-to-end latency for this one call."),
        Attribute("frame_count", "frame_count", "How many camera frames this call had, successfully decoded."),
        Attribute("frame_bytes", "frame_bytes", "Total bytes across this call's decoded frames."),
        Attribute("frame_keys", "frame_keys", "Lake object keys for this call's decoded frames, in order."),
    ),
    default_fields=(
        "created_at", "application_name", "key_label", "requested_model", "actual_model",
        "status", "cost_eur", "total_tokens", "latency_ms", "call_key",
    ),
    record_order="created_at",
)


AGENT_RUNS = Dataset(
    name="agent_runs",
    table="marts.fct_agent_run",
    kind="event",
    date_column="partition_date",
    description=(
        "One row per agent run -- every turn of work the platform did, whether or "
        "not it called the gateway. A run can hold several gateway calls, one, or "
        "none: this is the dataset for 'how many runs failed today', and "
        "ai_gateway_calls is the dataset for what those runs actually spent."
    ),
    dimensions=(
        Dimension("agent_name", "agent_name", "The agent the run's conversation runs against."),
        Dimension("status", "status", "pending, running, completed, failed, awaiting_input, and so on."),
        Dimension("requested_model", "requested_model", "The model the run asked for."),
        Dimension("actual_route", "actual_route", "The model route that actually served it."),
        Dimension("was_rerouted", "was_rerouted", "True when the actual route differs from the one requested."),
        Dimension("failure_code", "failure_code", "Why the run failed, when it did."),
        Dimension("is_awaiting_input", "is_awaiting_input", "True while the run is waiting on human input."),
        Dimension("is_report_run", "is_report_run", "True when the run was generating a scheduled report."),
    ),
    measures=(
        Measure("runs", "count(*)", "Number of runs.", "runs"),
        Measure("cost_eur", "round(sum(cost_eur)::decimal(18, 6), 6)", "Total cost.", "EUR"),
        Measure("input_tokens", "sum(input_tokens)", "Prompt tokens across the runs.", "tokens"),
        Measure("output_tokens", "sum(output_tokens)", "Completion tokens across the runs.", "tokens"),
        Measure("total_tokens", "sum(total_tokens)", "Prompt plus completion tokens across the runs.", "tokens"),
        Measure("reasoning_tokens", "sum(reasoning_tokens)",
                "Hidden-reasoning tokens, where models report them.", "tokens"),
        Measure("failures", "count(*) FILTER (WHERE status = 'failed')", "Failed runs.", "runs"),
        Measure("median_duration_ms", "round(median(duration_ms)::numeric, 1)",
                "Median wall-clock duration, over finished runs.", "ms"),
        Measure("eur_per_run", "round((sum(cost_eur) / nullif(count(*), 0))::decimal(18, 6), 6)",
                "Mean cost per run, total over count.", "EUR"),
    ),
    default_measures=("runs", "cost_eur", "failures"),
    attributes=(
        Attribute("run_key", "run_key", "The run's stable id."),
        Attribute("partition_date", "partition_date", "The UTC day the run started."),
        Attribute("started_at", "started_at", "When the run began, to the microsecond, UTC."),
        Attribute("started_hour", "started_hour", "started_at truncated to the hour."),
        Attribute("completed_at", "completed_at", "When the run finished, or null while still going."),
        Attribute("duration_ms", "duration_ms", "Wall-clock duration, where the run has finished."),
        Attribute("conversation_id", "conversation_id", "The conversation this run belongs to."),
        Attribute("conversation_title", "conversation_title", "That conversation's title."),
        Attribute("agent_id", "agent_id", "Which agent the conversation runs against."),
        Attribute("initiated_by_user_id", "initiated_by_user_id", "Who started the run."),
        Attribute("input_tokens", "input_tokens", "Prompt tokens for this one run."),
        Attribute("output_tokens", "output_tokens", "Completion tokens for this one run."),
        Attribute("total_tokens", "total_tokens", "Prompt plus completion tokens for this one run."),
        Attribute("reasoning_tokens", "reasoning_tokens", "Hidden-reasoning tokens for this one run."),
        Attribute("cost_eur", "cost_eur", "What this one run cost."),
        Attribute("awaiting_input_deadline", "awaiting_input_deadline",
                  "When a run waiting on human input times out."),
        Attribute("report_definition_id", "report_definition_id",
                  "The report definition this run was generating, where it was a report run."),
        Attribute("metadata", "metadata", "JSON. Everything else recorded about the run."),
    ),
    default_fields=(
        "started_at", "agent_name", "conversation_title", "status", "cost_eur",
        "total_tokens", "duration_ms", "run_key",
    ),
    record_order="started_at",
)


AGENT_TOOL_CALLS = Dataset(
    name="agent_tool_calls",
    table="marts.fct_agent_tool_call",
    kind="event",
    date_column="partition_date",
    description=(
        "One row per tool call an agent run made -- the tool_call slice of the "
        "platform's run_steps. Never passes through the LLM gateway, so "
        "ai_gateway_calls does not see these: 'how many times did an agent call "
        "export_workbook today' has its answer here. tool_name, server_alias, "
        "call_status and duration_ms are read out of the step's own free-form "
        "payload on a best-effort basis -- the application's internal shape for "
        "that payload was not a documented contract when this was built, so a "
        "missing field lands as null rather than a broken row."
    ),
    dimensions=(
        Dimension("tool_name", "tool_name", "The tool invoked. Best-effort -- see the dataset note."),
        Dimension("server_alias", "server_alias", "Which MCP server served the tool. Best-effort."),
        Dimension("call_status", "call_status", "The call's outcome. Best-effort."),
    ),
    measures=(
        Measure("tool_calls", "count(*)", "Number of tool calls.", "calls"),
        Measure("failures", "count(*) FILTER (WHERE call_status = 'failed')", "Failed calls.", "calls"),
        Measure("distinct_tools", "count(DISTINCT tool_name)", "Distinct tools invoked.", "tools"),
        Measure("median_duration_ms", "round(median(duration_ms)::numeric, 1)",
                "Median call duration, where the payload records one.", "ms"),
    ),
    default_measures=("tool_calls", "failures", "distinct_tools"),
    attributes=(
        Attribute("tool_call_key", "tool_call_key", "The run_step's stable id."),
        Attribute("partition_date", "partition_date", "The UTC day the call happened."),
        Attribute("occurred_at", "occurred_at", "When the call happened, to the microsecond, UTC."),
        Attribute("occurred_hour", "occurred_hour", "occurred_at truncated to the hour."),
        Attribute("run_id", "run_id", "The run this tool call belongs to."),
        Attribute("sequence", "sequence", "The step's position within its run."),
        Attribute("arguments_json", "arguments_json", "JSON. The call's arguments, where the payload carries them."),
        Attribute("result_json", "result_json", "JSON. What the tool returned, where the payload carries it."),
        Attribute("error_message", "error_message", "The error the call raised, when the payload records one."),
        Attribute("duration_ms", "duration_ms", "How long this one call took, where the payload records it."),
        Attribute("step_title", "step_title", "The step's own short label."),
        Attribute("step_description", "step_description", "The step's own longer description."),
        Attribute("payload", "payload", "JSON. The step's raw detail, unparsed."),
    ),
    default_fields=(
        "occurred_at", "tool_name", "server_alias", "call_status", "duration_ms",
        "run_id", "tool_call_key",
    ),
    record_order="occurred_at",
)


CONVERSATIONS = Dataset(
    name="conversations",
    table="marts.fct_conversation_message",
    kind="event",
    date_column="partition_date",
    description=(
        "One row per message in every conversation, full text, by the owner's "
        "2026-09-14 decision -- see IMPLEMENTATION_PLAN.md section 15. The conversation's title and agent "
        "travel with each message so a listing reads like a transcript rather than "
        "a bag of rows keyed by ids nobody recognises."
    ),
    dimensions=(
        Dimension("agent_name", "agent_name", "The agent the message's conversation runs against."),
        Dimension("role", "role", "user, assistant, system, or tool."),
    ),
    measures=(
        Measure("messages", "count(*)", "Number of messages.", "messages"),
        Measure("distinct_conversations", "count(DISTINCT conversation_id)",
                "Distinct conversations touched.", "conversations"),
        Measure("avg_content_length", "round(avg(content_length)::numeric, 1)",
                "Mean message length, in characters.", "characters"),
        Measure("total_content_length", "sum(content_length)", "Total characters written.", "characters"),
    ),
    default_measures=("messages", "distinct_conversations"),
    attributes=(
        Attribute("message_key", "message_key", "The message's stable id."),
        Attribute("partition_date", "partition_date", "The UTC day the message was written."),
        Attribute("created_at", "created_at", "When the message was written, to the microsecond, UTC."),
        Attribute("created_hour", "created_hour", "created_at truncated to the hour."),
        Attribute("conversation_id", "conversation_id", "The conversation this message belongs to."),
        Attribute("conversation_title", "conversation_title", "That conversation's title."),
        Attribute("agent_id", "agent_id", "Which agent the conversation runs against."),
        Attribute("conversation_owner_user_id", "conversation_owner_user_id",
                  "Who owns the conversation this message belongs to."),
        Attribute("author_user_id", "author_user_id",
                  "Who wrote the message, for a human-authored one. Null for the assistant's own messages."),
        Attribute("run_id", "run_id", "The run that produced this message, where one did."),
        Attribute("content", "content", "The message's full text."),
        Attribute("content_length", "content_length", "Character length of this one message."),
        Attribute("metadata", "metadata", "JSON. Everything else recorded about the message."),
    ),
    default_fields=(
        "created_at", "conversation_title", "agent_name", "role", "content_length", "message_key",
    ),
    record_order="created_at",
)


EMAIL_SENDS = Dataset(
    name="email_sends",
    table="marts.fct_email_send",
    kind="event",
    date_column="partition_date",
    description="Every email the platform has sent or attempted to send.",
    dimensions=(
        Dimension("status", "status", "sent, failed, or pending."),
        Dimension("was_sent", "was_sent", "True when the send succeeded."),
        Dimension("was_approved", "was_approved", "True when the send went through an approval."),
        Dimension("has_attachments", "has_attachments", "True when anything was attached."),
    ),
    measures=(
        Measure("sends", "count(*)", "Number of sends attempted.", "sends"),
        Measure("sent", "count(*) FILTER (WHERE was_sent)", "Sends that succeeded.", "sends"),
        Measure("failed", "count(*) FILTER (WHERE NOT was_sent)", "Sends that did not succeed.", "sends"),
        Measure("avg_recipients", "round(avg(recipient_count)::numeric, 1)",
                "Mean recipients per send.", "recipients"),
        Measure("total_recipients", "sum(recipient_count)", "Recipients across the sends.", "recipients"),
    ),
    default_measures=("sends", "sent", "failed"),
    attributes=(
        Attribute("email_send_key", "email_send_key", "The send's stable id."),
        Attribute("partition_date", "partition_date", "The UTC day the send was attempted."),
        Attribute("created_at", "created_at", "When the send was attempted, to the microsecond, UTC."),
        Attribute("created_hour", "created_hour", "created_at truncated to the hour."),
        Attribute("owner_user_id", "owner_user_id", "Who triggered the send."),
        Attribute("run_id", "run_id", "The run that triggered the send, where one did."),
        Attribute("approval_id", "approval_id", "The approval that authorised the send, where one was required."),
        Attribute("template_script_id", "template_script_id", "The email template script used, where one was."),
        Attribute("template_version", "template_version", "Which version of that template."),
        Attribute("subject", "subject", "The email's subject line."),
        Attribute("recipients", "recipients", "JSON. Who the email was sent to."),
        Attribute("recipient_count", "recipient_count", "How many recipients this one send had."),
        Attribute("attachments", "attachments", "JSON. What was attached, where anything was."),
        Attribute("attachment_count", "attachment_count", "How many attachments this one send had."),
        Attribute("failure_reason", "failure_reason", "Why the send failed, when it did."),
    ),
    default_fields=(
        "created_at", "subject", "status", "recipient_count", "was_sent", "email_send_key",
    ),
    record_order="created_at",
)


AUTOMATION_RUNS = Dataset(
    name="automation_runs",
    table="marts.fct_automation_run",
    kind="event",
    date_column="partition_date",
    description=(
        "One row per run of an automation, with its step runs folded in: counts by "
        "outcome, and a JSON array of the steps themselves for the times the "
        "counts are not enough."
    ),
    dimensions=(
        Dimension("automation_name", "automation_name", "Which automation this is a run of."),
        Dimension("status", "status", "pending, running, succeeded, failed."),
        Dimension("had_a_failed_step", "had_a_failed_step", "True when any step in this run failed."),
    ),
    measures=(
        Measure("runs", "count(*)", "Number of automation runs.", "runs"),
        Measure("failed_runs", "count(*) FILTER (WHERE status = 'failed')", "Runs that failed.", "runs"),
        Measure("runs_with_a_failed_step", "count(*) FILTER (WHERE had_a_failed_step)",
                "Runs where at least one step failed, whatever the run's own final status.", "runs"),
        Measure("median_duration_ms", "round(median(duration_ms)::numeric, 1)",
                "Median wall-clock duration, over finished runs.", "ms"),
        Measure("steps_total", "sum(steps_total)", "Steps across the runs.", "steps"),
        Measure("steps_failed", "sum(steps_failed)", "Failed steps across the runs.", "steps"),
    ),
    default_measures=("runs", "failed_runs", "steps_total"),
    attributes=(
        Attribute("automation_run_key", "automation_run_key", "The run's stable id."),
        Attribute("partition_date", "partition_date", "The UTC day the run record was created."),
        Attribute("created_at", "created_at", "When the run record was created, UTC."),
        Attribute("started_at", "started_at", "When the run began."),
        Attribute("finished_at", "finished_at", "When the run ended, or null while still running."),
        Attribute("duration_ms", "duration_ms", "Wall-clock duration, where the run has finished."),
        Attribute("automation_id", "automation_id", "Which automation this is a run of."),
        Attribute("dagster_run_id", "dagster_run_id",
                  "The underlying Dagster run id, where the automation executes as one."),
        Attribute("steps_total", "steps_total", "How many steps this run has."),
        Attribute("steps_succeeded", "steps_succeeded", "How many of them succeeded."),
        Attribute("steps_failed", "steps_failed", "How many failed."),
        Attribute("steps_pending", "steps_pending", "How many are still pending or running."),
        Attribute("last_step_finished_at", "last_step_finished_at", "When the last step to finish did."),
        Attribute("step_runs_json", "step_runs_json",
                  "JSON array of every step run -- id, type, label, status, timing, error."),
    ),
    default_fields=(
        "started_at", "automation_name", "status", "steps_total", "steps_failed",
        "automation_run_key",
    ),
    record_order="started_at",
)


DATASETS: dict[str, Dataset] = {
    d.name: d for d in (
        LLM_SPEND, WEB_TRAFFIC, CAMERA_ACTIVITY, MEDIA_LIBRARY, MEDIA_PLAYBACK,
        MEDIA_SERVER_ACTIVITY,
        AI_GATEWAY_CALLS, AGENT_RUNS, AGENT_TOOL_CALLS, CONVERSATIONS, EMAIL_SENDS,
        AUTOMATION_RUNS,
    )
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
    statements = [
        ("date_column", f"SELECT {dataset.date_column} FROM {dataset.table} WHERE false"),  # noqa: S608
        ("dimensions", f"SELECT {dims} FROM {dataset.table} WHERE false"),  # noqa: S608
        ("measures", f"SELECT {measures} FROM {dataset.table} WHERE false"),  # noqa: S608
    ]
    if dataset.attributes:
        attrs = ", ".join(f'{a.sql} AS "{a.name}"' for a in dataset.attributes)
        statements.append(
            ("attributes", f"SELECT {attrs} FROM {dataset.table} WHERE false")  # noqa: S608
        )
    return statements


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



# --- drift: what the mart holds that the semantic layer does not say ----------
#
# `validate()` above catches a column that was *renamed*: the expression stops
# binding and the deploy fails. It cannot catch a column that was *added*, because
# nothing here refers to it -- everything still binds perfectly, and the new column
# is simply invisible to every caller.
#
# That is not a hypothetical. fct_camera_event grew from 29 columns to 78, including
# five timestamps, and the semantic layer went on describing it as a table whose
# finest time resolution was an hour bucket. An agent asked for exact opening times,
# read describe_dataset, and concluded -- correctly, from what it was told -- that
# the data did not exist. It then delivered something else.
#
# So the check has to run in the other direction: every column of the mart must be
# either referenced by this dataset or explicitly declared as not exposed. It is the
# same shape as the `_unmapped` capture at ingestion, one layer up, and for the same
# reason: an omission that nobody is told about is indistinguishable from a decision.

# Words that appear in a SQL expression and are not column names. Kept deliberately
# short: this list only has to be good enough that a *mart column* is never mistaken
# for a keyword, and every real column that slips through the net is reported as
# missing, which is the safe direction to be wrong in.
_SQL_WORDS = frozenset("""
    select from where filter cast as and or not null is in between case when then else end
    distinct count sum avg min max median round nullif coalesce decimal numeric varchar
    integer bigint double boolean date timestamp interval asc desc order by group having
    true false on using left right inner outer join over partition
""".split())


def referenced_columns(dataset: Dataset) -> set[str]:
    """Every identifier the dataset's expressions mention.

    A regex rather than a SQL parser, and that is a considered choice: the input is
    the handful of expressions in this file, not arbitrary SQL, and the failure mode
    of being too eager is a column reported as covered when it is only mentioned --
    while the failure mode of a parser dependency is a build that breaks on a DuckDB
    function it has not heard of.
    """
    import re as _re

    expressions = [dataset.date_column]
    expressions += [d.sql for d in dataset.dimensions]
    expressions += [m.sql for m in dataset.measures]
    expressions += [a.sql for a in dataset.attributes]
    found: set[str] = set()
    for expression in expressions:
        for word in _re.findall(r"[A-Za-z_][A-Za-z0-9_]*", expression):
            lowered = word.lower()
            if lowered not in _SQL_WORDS:
                found.add(lowered)
    return found


def missing_columns(dataset: Dataset, columns: list[str]) -> list[str]:
    """Mart columns this dataset neither exposes nor declares it is leaving out.

    `columns` is the real column list of `dataset.table`, so this is a pure function
    over a fact obtained elsewhere -- which is what lets it be tested without a
    warehouse and run against the live one.
    """
    declared = {name.lower() for name, _reason in dataset.not_exposed}
    referenced = referenced_columns(dataset)
    return sorted(
        column for column in columns
        if column.lower() not in referenced and column.lower() not in declared
    )


def coverage(con) -> dict[str, list[str]]:
    """Run `missing_columns` for every dataset against a live database.

    Returns {dataset: [columns]} holding only the datasets that have drifted, so an
    empty dict is the passing case and the failure message is the fix.
    """
    out: dict[str, list[str]] = {}
    for dataset in DATASETS.values():
        schema, _, table = dataset.table.rpartition(".")
        rows = con.execute(
            "SELECT column_name FROM duckdb_columns() WHERE table_name = ? "
            "AND (? = '' OR schema_name = ? OR schema_name LIKE '%_' || ?)",
            [table, schema, schema, schema],
        ).fetchall()
        columns = [r[0] for r in rows]
        if not columns:
            out[dataset.name] = [f"table {dataset.table} was not found"]
            continue
        missing = missing_columns(dataset, columns)
        if missing:
            out[dataset.name] = missing
    return out
