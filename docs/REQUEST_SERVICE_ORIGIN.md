# Which requests came from our services?

`datasets.web_traffic`, `marts.fct_http_request` and the curated
`int_npm_requests` table now carry service-origin columns. The semantic tools,
SQL catalogue, request listings and workbook exports expose the same fields.

| Column | Meaning |
| --- | --- |
| `is_internal` | Existing private-address flag. Its meaning is preserved. |
| `is_internal_service` | `true`: managed/private source plus a recognized service signature. `false`: resolved public source outside the managed inventory. `null`: service attribution remains unknown. |
| `traffic_origin` | `internal_service`, `managed_network`, `private_network`, `unmanaged_network`, or `unresolved`. |
| `origin_edge` | The managed **source** edge; the destination remains `npm_instance`. |
| `origin_service` | `monitoring`, `datalake_query`, `datalake_storage`, or `edge_log_mirror`, when identified. |
| `origin_confidence` | `inferred`, `network_only`, or `unknown`. |
| `origin_evidence` | The inventory evidence and service signature, or why attribution remains unknown. |

An outgoing service request remains internal when it crosses the internet or
lands on a different edge. A browser sharing the managed egress does not become
a service: it remains `managed_network` with a null service flag. A spoofed
DuckDB/monitoring user agent from an unmatched public address cannot identify
an internal service. A CDN-only peer is unresolved, even if its address happens
to match an inventory entry.

Historical HTTP logs do not authenticate a calling process. Positive matches
are explicitly **inferred** from the known source network and a bounded
user-agent/path signature. Do not use this analytics classification to grant
access or suppress security controls. Camera/browser traffic without a process
identity remains unknown rather than being relabeled by its destination path.

## Private inventory

`DATALAKE_MANAGED_ORIGINS_JSON` lives only in the deployment's `.env`. Each
binding has exactly these keys, using documentation addresses here:

```json
[
  {
    "address": "203.0.113.8",
    "origin_edge": "primary",
    "valid_from": "2026-10-01T00:00:00Z",
    "valid_until": "2026-11-01T00:00:00Z",
    "evidence": "Observed egress on managed host; historical attribution inferred"
  },
  {
    "address": "2001:db8::8",
    "origin_edge": "secondary",
    "valid_from": "2026-11-01T00:00:00Z",
    "valid_until": null,
    "evidence": "Observed current egress on secondary host"
  }
]
```

Use canonical exact IPv4/IPv6 addresses and timezone-qualified timestamps.
The interval is inclusive at the start and exclusive at the end. Close an old
binding and add a new dated binding when egress changes; never retroactively
assign today's address to an unrelated historical period. Unsupported networks
remain unmatched. Config validation refuses unknown keys, missing evidence,
invalid addresses/dates and overlapping bindings that could multiply requests.

The inventory contributes to the warehouse build fingerprint, so changing it
rebuilds classifications over the retained raw history. No raw log partition
needs to be re-ingested. Restart the datalake services with the updated `.env`,
then execute `analytics_hourly`; its tested publication updates both the lake
and the MCP serving copy. Do not publish the real inventory to git.

## Inspect and filter

```sql
SELECT requested_at, npm_instance, is_internal, is_internal_service,
       traffic_origin, origin_edge, origin_service, origin_confidence,
       origin_evidence, request_host, path, user_agent
FROM datasets.web_traffic
WHERE partition_date BETWEEN DATE '2026-10-01' AND DATE '2026-10-31'
ORDER BY requested_at DESC;
```

To count identified service calls, filter `is_internal_service = true`.
To inspect cross-edge calls, also require `origin_edge <> npm_instance`.
For a strict outside-inventory audience view, filter
`traffic_origin = 'unmanaged_network' AND client_trust = 'resolved'` and state
that managed-but-unattributed traffic was omitted. To exclude only identified
services while preserving uncertainty, use `is_internal_service IS NOT TRUE`
and show the remaining origin categories separately. `false` does not prove
that no unregistered service exists outside the configured inventory.

The `internal_service_requests` and `unattributed_service_requests` measures
provide the corresponding counts through `query_dataset`.
