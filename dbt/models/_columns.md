{% docs _readme %}
Column descriptions that are shared by more than one model.

A column that survives from staging into a mart means the same thing in both, and
writing that meaning out twice is how the two copies come to disagree. So every such
column is described once here and referenced by a `doc()` call from the model's schema file; a description
written inline in a `.yml` file is one that belongs to exactly one model.

Names are prefixed by domain -- `req_`, `llm_`, `cam_`, `lib_` -- because doc block
names are global to the project and `status` means something different to a request
and to a model call.
{% enddocs %}


{# ------------------------------------------------------------------ requests #}

{% docs req_partition_date %}
The UTC day the request was made, taken from the timestamp in the log line rather than
from the file it was found in. logrotate moves a day's lines between files weekly, so
the filename is not evidence of the day.
{% enddocs %}

{% docs req_npm_instance %}
Which proxy logged the request: `primary` (44 hosts) or `secondary` (the camera) behind
Nginx Proxy Manager, each on its own machine and failing independently; or
`mcp-api`, which is not NPM at all -- it is mcp.ai.example.test's own access log,
landed in this same table from 2026-09-14 because it sits behind no proxy this
warehouse otherwise sees. See `parsing.mcp_api_log` for its field mapping.
{% enddocs %}

{% docs req_requested_at %}
When nginx logged the request, in UTC. nginx writes a local offset and it is normalised
at ingest, which is what keeps a 23:[count withheld] request on the same day whoever runs the job.
{% enddocs %}

{% docs req_requested_hour %}
`requested_at` truncated to the hour, in UTC. Precomputed because "when is this host
busy" is asked of every host, and a truncation in the query is a truncation in the
session's timezone.
{% enddocs %}

{% docs req_log_format %}
Which format the row was parsed from: NPM's `proxy` or `standard`, stock nginx
`combined`, or `mcp_api_json` for mcp.ai's own JSON access log. Only `proxy`
carries the upstream and cache fields, so a null in those is a property of the
format rather than missing data.
{% enddocs %}

{% docs req_request_host %}
The Host header the client sent. Client-controlled, so scanners put bare addresses in
it; kept exactly as sent rather than cleaned, because a scanner probing the IP directly
is a fact worth counting rather than noise to launder.
{% enddocs %}

{% docs req_method %}
The HTTP method, upper-cased. Null when the request line did not parse -- almost always
TLS bytes sent to port 80.
{% enddocs %}

{% docs req_scheme %}
`http` or `https`, lower-cased. Absent from the `combined` format, which does not log it.
{% enddocs %}

{% docs req_path %}
The request path with the query string removed. Query strings carry password-reset
tokens, session ids and API keys often enough that storing them is a bet with no upside.
{% enddocs %}

{% docs req_path_group %}
The path cut to its first two segments. Full paths have unbounded cardinality -- every
media id is its own path -- and two segments is what somebody actually wants grouped in
a dashboard.
{% enddocs %}

{% docs req_query_string %}
The query string exactly as sent, without the leading `?`. Added 2026-09-14, raw and
private-warehouse-only -- see homelab_datalake.privacy for the decision. `has_query`
and `query_param_names` are unchanged and still the columns any export or public
dataset should read. Null when the request carried no `?` at all; `''` when it carried one with nothing
after it -- `has_query` is true in the second case and false in the first, which is
what tells the two apart. Also null for every row from before 2026-09-14, and for an
older row whose source line had already rotated off disk by the day this shipped.
{% enddocs %}

{% docs req_status %}
The HTTP status nginx returned to the client.
{% enddocs %}

{% docs req_status_class %}
`status` banded into informational, success, redirect, client_error, server_error. The
band is what error-rate questions group by; the code is what a person looks at once the
band says something is wrong.
{% enddocs %}

{% docs req_upstream_status %}
The status the upstream returned, as text rather than a number: a retry produces
several, comma-separated ("502, 200"). Only the `proxy` format has it.
{% enddocs %}

{% docs req_cache_status %}
nginx's `$upstream_cache_status` -- HIT, MISS, EXPIRED and so on. Only the `proxy`
format has it.
{% enddocs %}

{% docs req_upstream_target %}
The upstream nginx forwarded the request to. Only the `proxy` format has it.
{% enddocs %}

{% docs req_bytes_sent %}
Response body size in bytes, excluding headers. Zero rather than null when the log line
carried no size, so sums do not silently skip rows.
{% enddocs %}

{% docs req_user_agent %}
The User-Agent header verbatim. The only evidence `client_kind` is derived from.
{% enddocs %}

{% docs req_host_is_address %}
Whether the Host header is an address rather than a name, which means a scanner probing
the IP directly. Worth naming because it is most of the 4xx traffic.
{% enddocs %}

{% docs req_client_kind %}
`bot`, `browser`, `other`, or `unknown`, from the user agent alone. Deliberately
conservative: the stronger signal is the network's autonomous system, and geo
enrichment is optional, so this works without it and says which evidence it used
instead of pretending to certainty.
{% enddocs %}

{% docs req_is_automated %}
Whether the request was a bot by user agent, or was aimed at a bare address. The
practical "not a person" flag, and wider than `client_kind = 'bot'` alone.
{% enddocs %}

{% docs req_source_file %}
Basename of the log file the line was read from. Diagnostic: it says which rotation
generation a row came from, and nothing groups by it.
{% enddocs %}


{# ------------------------------------------------- the privacy boundary, shared #}

{% docs client_address %}
The client address exactly as the source logged it, raw. Added 2026-09-14 alongside
the derived columns below, which are unchanged -- see homelab_datalake.privacy for
the decision and the guarantees that still hold. Private-warehouse-only: no export,
seed or publish path may select it. Null before 2026-09-14, and null for any older
row whose source line had already rotated off disk by the day this shipped -- NPM
keeps roughly five weeks, so it cannot be filled in retroactively past what was
still there to re-read.
{% enddocs %}

{% docs client_hash %}
Salted HMAC-SHA256 of the client address, truncated to 16 bytes. The address itself is
destroyed in memory at ingest and never written, so this is what makes a
distinct-visitor count possible without keeping one. HMAC rather than a plain digest:
there are only 2^32 IPv4 addresses, and an unkeyed digest of one is brute-forced in
minutes.
{% enddocs %}

{% docs client_network %}
The client address truncated to its /24 (IPv4) or /48 (IPv6). Wide enough not to single
out a household, narrow enough to separate a datacentre range from a home ISP.
{% enddocs %}

{% docs client_trust %}
Whether the logged address named a visitor (`resolved`) or a Cloudflare machine standing
in for one (`cdn_edge`); `unknown` when there was no usable address. Two meaningful
values and not three: after the 27 Aug 2026 real-IP cutover a restored address and an
always-direct one are byte-identical, so claiming to tell them apart would assert
something the data does not contain.
{% enddocs %}

{% docs client_ip_version %}
4 or 6 -- the IP version of the address, recorded before it was discarded.
{% enddocs %}

{% docs client_is_internal %}
Whether the address was in a private range, which means the request came from inside the
LAN. Read from the flag the privacy boundary set rather than re-derived, because the
address is gone by this point -- which is the boundary working.
{% enddocs %}

{% docs geo_country_code %}
ISO country code from MaxMind, resolved while the address was still in hand and before
the privacy boundary discarded it. Null throughout when no GeoLite2 database is
configured: geo enrichment is optional on purpose, so CI never needs a licence key.
{% enddocs %}

{% docs geo_country_name %}
The country's English name from MaxMind. Null when geo enrichment is not configured.
{% enddocs %}

{% docs geo_city %}
City from MaxMind's city database. Frequently null even when enrichment is on -- MaxMind
resolves a city for far fewer addresses than a country.
{% enddocs %}

{% docs geo_asn %}
Autonomous system number of the network the address belonged to.
{% enddocs %}

{% docs geo_as_org %}
The organisation owning that autonomous system -- the operator name a person recognises,
such as an ISP or a cloud provider.
{% enddocs %}


{# ---------------------------------------------------------------- model calls #}

{% docs llm_partition_date %}
The UTC day the call started. LiteLLM stores timestamps without a zone, already in UTC,
and the ingest attaches UTC explicitly so the partition filter cannot drift with a
session setting.
{% enddocs %}

{% docs llm_started_at %}
When the proxy accepted the request, in UTC.
{% enddocs %}

{% docs llm_started_hour %}
`started_at` truncated to the hour, in UTC.
{% enddocs %}

{% docs llm_model %}
The model as named in the request, including any provider prefix.
{% enddocs %}

{% docs llm_model_group %}
LiteLLM's routing group: the alias several concrete models can sit behind. Null when the
request named a model directly.
{% enddocs %}

{% docs llm_provider %}
Who served the call. Null on a large minority of rows, which is why the self-hosted flag
coalesces rather than comparing directly -- in SQL `null = 'ollama'` is null, and a
boolean that is sometimes null drops rows from every `where flag = false`.
{% enddocs %}

{% docs llm_workload %}
`completion`, `embedding`, `mcp` or `other`, grouped from `call_type`. The coarse split
worth charting; `call_type` is the detail underneath it.
{% enddocs %}

{% docs llm_call_type %}
What LiteLLM was asked to do -- `acompletion`, `completion`, `responses`, an embedding
call, or one of the MCP calls.
{% enddocs %}

{% docs llm_mcp_tool %}
The namespaced MCP tool a tool call invoked. Null for an ordinary completion.
{% enddocs %}

{% docs llm_status %}
`success` or `failure` as the proxy recorded it, or `unknown` where the ledger left it
empty. Not assumed successful: a call whose outcome was not written down is not evidence
that it worked.
{% enddocs %}

{% docs llm_spend_usd %}
What LiteLLM billed, in US dollars. Taken from the ledger and never recomputed: a second
copy of every model's price list would drift from the proxy's and then quietly disagree
with the bill.
{% enddocs %}

{% docs llm_total_tokens %}
Prompt and completion tokens together, as counted by the provider.
{% enddocs %}

{% docs llm_prompt_tokens %}
Tokens in the input.
{% enddocs %}

{% docs llm_completion_tokens %}
Tokens generated.
{% enddocs %}

{% docs llm_duration_ms %}
End-to-end duration of the call in milliseconds, from the proxy's view. What the bill
reflects, which is a different question from what a person feels.
{% enddocs %}

{% docs llm_ttft_ms %}
Milliseconds from the request being accepted to the first token arriving. What a person
actually feels as latency. Null for a call that streamed nothing.
{% enddocs %}

{% docs llm_was_cache_hit %}
Whether the proxy served the call from its cache. LiteLLM writes three states -- hit,
miss, and empty -- flattened here to a boolean where unknown counts as a miss, because
an uncached call is what got billed.
{% enddocs %}

{% docs llm_team_id %}
The LiteLLM team the API key belongs to. Null when the key is in no team.
{% enddocs %}

{% docs llm_end_user_hash %}
Salted hash of LiteLLM's end_user field. The field is caller-supplied and can carry an
address or an account name, so it goes through the same boundary as everything else
identifying.
{% enddocs %}

{% docs llm_api_key_hash %}
Salted hash of the API key. The column upstream mixes 64-character hashes and
24-character literals with nothing to tell them apart, so it is treated as a credential
either way. "Spend per key" survives; the risk does not.
{% enddocs %}

{% docs llm_session_id %}
Groups the calls of one conversation or one agent run. Null for a call made outside a
session.
{% enddocs %}

{% docs llm_is_self_hosted %}
Whether the call went to a model running on this hardware rather than to a paid API.
The split that separates "what did this cost" from "what did this use".
{% enddocs %}


{# --------------------------------------------------------------------- camera #}

{% docs cam_partition_date %}
The UTC day the event started. The export is read on a half-open [from, to) range, so
adjacent days can neither double-count an event on the boundary nor drop one.
{% enddocs %}

{% docs cam_extracted_at %}
When this pipeline read the export. The late-arrival mechanism rests on it: re-reading
yesterday tomorrow legitimately returns different rows, and this says which reading a
row came from.
{% enddocs %}

{% docs cam_event_id %}
The camera's event id. Stable across re-reads, and the grain of the fact table.
{% enddocs %}

{% docs cam_camera_id %}
Which camera raised the event.
{% enddocs %}

{% docs cam_zone_id %}
The attention zone the rule belongs to. The zone's shape is never exported -- polygon
coordinates are a map of the property.
{% enddocs %}

{% docs cam_zone_name %}
The zone's display name, as a person named it.
{% enddocs %}

{% docs cam_rule_type %}
What kind of rule fired -- `motion` or `state`. Deliberately not a statement about what
happened: most real passages are persisted as `motion` and only become crossings once
the clip analysis lands, so this is the trigger and not the conclusion.
{% enddocs %}

{% docs cam_lifecycle %}
`candidate` when first written, then `confirmed` or `dismissed` once vision analysis has
run. A row still `candidate` hours later means the analysis pipeline is stuck.
{% enddocs %}

{% docs cam_is_confirmed %}
Whether analysis confirmed the event. `lifecycle = 'confirmed'` as a boolean, so a
filter cannot be spelled three different ways in three queries.
{% enddocs %}

{% docs cam_is_awaiting_analysis %}
Whether the event is still a candidate. A few are normal -- the day's last events are in
flight when the partition is first read -- but a high share means the analysis pipeline
has stopped.
{% enddocs %}

{% docs cam_started_at %}
When the event began, in UTC.
{% enddocs %}

{% docs cam_completed_at %}
When the event was resolved, in UTC. Null while it is still a candidate.
{% enddocs %}

{% docs cam_duration_seconds %}
Seconds from start to completion. Null while the event is unresolved, so an average over
it describes finished events only.
{% enddocs %}

{% docs cam_clip_id %}
The clip that filmed the event, where one was kept. Only the id: no path, playlist or
thumbnail is exported.
{% enddocs %}

{% docs cam_direction %}
The provisional direction of a crossing. Kept even when the threshold check rejected the
crossing, so a rejection is told apart by other fields rather than by this being cleared.
{% enddocs %}

{% docs cam_zone_state %}
The zone's physical state at the event -- for a gate, `open` or `closed`. `partly_open`
is a transient reading of the same position rather than a third state. Only set for
state rules.
{% enddocs %}

{% docs cam_previous_zone_state %}
The zone's state before this event. Equal to `zone_state` means nothing moved, which the
camera dismisses as `unchanged_state`.
{% enddocs %}

{% docs cam_confidence %}
The vision model's confidence in its conclusion, 0 to 1. Null is a real value and is not
a low score: it means the model declined to commit, and coalescing it to 0 would turn a
refusal into a confident "no".
{% enddocs %}

{% docs cam_is_unusual %}
Whether the analysis judged the event out of the ordinary for that zone and hour.
{% enddocs %}

{% docs cam_should_notify %}
Whether the camera decided this was worth telling somebody about. Never true for a
dismissed, duplicate or unchanged-state event.
{% enddocs %}

{% docs cam_dismissal_reason %}
Why a dismissed event was dismissed -- `unchanged_state` when a state rule fired on no
movement, or a reason from the analysis such as `semantic_no_event`. Null for an event
that was not dismissed.
{% enddocs %}

{% docs cam_duplicate_of %}
The id of the event this one duplicates, when the camera recognised it as a repeat.
{% enddocs %}

{% docs cam_summary %}
The model's short description of what it saw, up to 800 characters. Free text, so
nothing groups by it.
{% enddocs %}


{# -------------------------------------------------------------------- library #}

{% docs lib_snapshot_date %}
The day this reading of the library was taken. Today's date rather than yesterday's: a
snapshot describes the library now, and dating it yesterday would invent a past that
never existed.
{% enddocs %}

{% docs lib_item_id %}
Jellyfin's item id. Stable across snapshots, and what SCD2 history is keyed on.
{% enddocs %}

{% docs lib_title %}
The title as Jellyfin displays it, which in this library is usually the localised name.
{% enddocs %}

{% docs lib_original_title %}
The title as released. Comes from Jellyfin, which fetched it from TMDB during the scan
at [rate withheld] coverage, so TMDB is not asked for it again.
{% enddocs %}

{% docs lib_production_year %}
Year of production, as Jellyfin holds it.
{% enddocs %}

{% docs lib_decade %}
`production_year` rounded down to its decade, derived at ingest so every reader agrees
on where the boundary is.
{% enddocs %}

{% docs lib_premiere_date %}
Release date. Null for titles Jellyfin never matched to a provider record.
{% enddocs %}

{% docs lib_added_at %}
When the file was added to the library, in UTC. The column that answers "what is new",
and it is about the file rather than the film.
{% enddocs %}

{% docs lib_community_rating %}
TMDB's vote average, which is what Jellyfin's CommunityRating is. Kept from the snapshot
rather than re-fetched -- the enrichment step deliberately does not ask TMDB for a
figure the library already holds.
{% enddocs %}

{% docs lib_critic_rating %}
Critic score where Jellyfin has one. Sparse across this library.
{% enddocs %}

{% docs lib_official_rating %}
Age or content certification, in whichever national scheme Jellyfin matched.
{% enddocs %}

{% docs lib_runtime_minutes %}
Runtime as the container metadata reports it. Some mpeg2video DVD rips are an order of
magnitude too long; the value is kept exactly as reported and flagged by
`is_runtime_plausible` rather than corrected, because inventing a plausible runtime
would put a fabricated number in the warehouse and hide the problem.
{% enddocs %}

{% docs lib_is_runtime_plausible %}
Whether the runtime is between 1 and 600 minutes. False marks the handful of rips whose
container metadata is wrong, so every aggregate can exclude them deliberately -- and the
count of them is itself worth watching.
{% enddocs %}

{% docs lib_is_long_feature %}
Whether the runtime is 150 minutes or more.
{% enddocs %}

{% docs lib_container %}
The file container -- mkv, mp4 and so on.
{% enddocs %}

{% docs lib_video_codec %}
Codec of the primary video stream.
{% enddocs %}

{% docs lib_video_height %}
Height in pixels of the primary video stream. Real values are not neat standards --
1080, 1040, 1036, 816, 804 -- because encodes are letterboxed and cropped.
{% enddocs %}

{% docs lib_video_width %}
Width in pixels of the primary video stream.
{% enddocs %}

{% docs lib_resolution_class %}
Height bucketed into 4K / 1080p / 720p / SD / low, with each boundary set below the
nominal height so a cropped 1080p encode still counts as 1080p rather than becoming its
own category.
{% enddocs %}

{% docs lib_is_high_bit_depth %}
Whether the primary video stream is 10-bit or deeper, which is what says a file is HDR.
Not the codec: hevc is routinely 8-bit and 10-bit h264 exists.
{% enddocs %}

{% docs lib_audio_codec %}
Codec of the primary audio stream.
{% enddocs %}

{% docs lib_audio_channels %}
Channel count of the primary audio stream -- 2 for stereo, 6 for 5.1.
{% enddocs %}

{% docs lib_studio %}
The first studio Jellyfin lists for the title.
{% enddocs %}

{% docs lib_production_country %}
First entry of Jellyfin's ProductionLocations, fetched from TMDB during the scan at
[rate withheld] coverage -- so TMDB is not asked for it again.
{% enddocs %}

{% docs lib_tmdb_id %}
TMDB identifier, stored by Jellyfin when it matched the file. Present on [rate withheld] of
titles, which is what lets enrichment be a key lookup rather than a title search -- so a
film cannot pick up another film's box office.
{% enddocs %}

{% docs lib_imdb_id %}
IMDb identifier, where Jellyfin matched one.
{% enddocs %}

{% docs lib_original_language %}
ISO code of the language the film was made in. One of the few facts Jellyfin has no
field for, so it comes from TMDB and is null when no API key is configured.
{% enddocs %}

{% docs lib_budget_usd %}
Production budget in US dollars, from TMDB. Null rather than 0 when TMDB has no figure:
a film that cost nothing and a film nobody recorded a budget for are different rows, and
averaging them together understates every box-office question.
{% enddocs %}

{% docs lib_revenue_usd %}
Worldwide revenue in US dollars, from TMDB. Null rather than 0 when unrecorded, for the
same reason as the budget.
{% enddocs %}

{% docs lib_revenue_to_budget %}
Revenue divided by budget. Null unless both are present and the budget is above zero, so
it never reports a ratio against a figure nobody recorded.
{% enddocs %}

{% docs lib_genre %}
One genre name, as Jellyfin holds it -- a French display string on this deployment,
which is why the dimension is keyed by a hash of it rather than by the name.
{% enddocs %}

{% docs lib_genre_key %}
Surrogate key over the genre name. A hash rather than the name itself because the name
is a display string Jellyfin can rewrite when metadata is refreshed, and a join on the
string would break when it does.
{% enddocs %}

{% docs lib_title_key %}
Surrogate key over item id and snapshot date. The grain of the library fact table: one
row per title per day it was seen.
{% enddocs %}

> Public history note: private identifiers, measurements, and operational details were redacted. Code examples use placeholder infrastructure.
