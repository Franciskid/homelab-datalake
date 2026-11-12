select request_key
from {{ ref('fct_http_request') }}
where traffic_origin not in ('internal_service', 'managed_network', 'private_network', 'unmanaged_network', 'unresolved')
   or origin_confidence not in ('inferred', 'network_only', 'unknown')
   or traffic_origin is null or origin_confidence is null or origin_evidence is null
   or (is_internal_service is true and (
       traffic_origin <> 'internal_service' or origin_service is null
       or origin_confidence <> 'inferred' or client_trust <> 'resolved'
       or (origin_edge is null and not is_internal)))
   or (traffic_origin = 'internal_service' and is_internal_service is distinct from true)
   or (traffic_origin in ('managed_network', 'private_network', 'unresolved') and is_internal_service is not null)
   or (client_trust <> 'resolved' and traffic_origin <> 'unresolved')
