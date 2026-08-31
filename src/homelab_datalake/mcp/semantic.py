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
        Attribute("upstream_target", "upstream_target", "Which backend served it."),
        Attribute("bytes_sent", "bytes_sent", "Response size."),
        Attribute("user_agent", "user_agent", "The client's user agent string."),
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
