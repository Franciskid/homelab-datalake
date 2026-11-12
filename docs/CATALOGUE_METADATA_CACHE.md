# Catalogue metadata without full-bucket scans

The November 12 production probe timed out while SQL queries remained healthy.
The old catalogue listed the entire bucket (424,319 keys at diagnosis), compared
each key to every dataset prefix, and read every dataset's Parquet footers again.
Concurrent catalogue requests queued behind that build's lock. A longer client
timeout did not fix the work being repeated.

The catalogue now lists only the 221 registered dataset prefixes, filters Parquet
objects, and reads changed footers with eight bounded workers. Schema and row
counts are cached per object using its namespace, key, size, ETag and modification
time (inode and nanosecond timestamps locally). Removed files disappear from the
next inventory. Unchanged files need no footer reads. Distinct observed schemas
are combined locally using DuckDB's name/type promotion rules; columns never come
from documentation. Manifest descriptions are rejoined on each refresh.

`DATALAKE_CATALOGUE_CACHE_PATH=/audit/catalogue-metadata.json` persists the cache
on the API's existing audit volume. It contains schemas and row counts, not data
rows or credentials, is written atomically with owner-only permissions, and can
be removed to force real footer reads. Invalid or unwritable caches fall back to
real metadata. Failed reads remain explicitly unavailable and are not cached.
The in-memory response still expires on publication identity/TTL; a publication
during its build invalidates the response on the next request.

Run `ruff check src scripts tests`, the complete pytest suite, and the generated
log analytics pipeline. Catalogue tests cover schema evolution, type promotion,
new/overwritten/deleted objects, cache reuse/recovery, prefix scoping and publication
races. Build on the production host, prewarm with a one-off API container sharing
the cache volume, and replace readers only after the cache contains the request
origin columns. Check normal Platform API catalogue and forced-refresh calls
using the standard 45-second timeout. No client timeout increase is required.
