#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
. scripts/demo-env.sh
mkdir -p "$DAGSTER_HOME" .demo/warehouse
printf 'telemetry:\n  enabled: false\n' > "$DAGSTER_HOME/dagster.yaml"
(cd dbt && dbt deps && dbt parse)
python scripts/generate_seed.py --out .demo/seed --days 3
# The second instance is mirrored, not mounted: its host is not reachable from the one
# running this, so infra/secondary-log-sync/ copies its logs into inbox/npm/secondary/
# every hour. The demo makes the same copy once, so both paths land identical rows.
mkdir -p "$DATALAKE_LOCAL_ROOT/inbox/npm/secondary"
cp "$NPM_LOG_DIR_SECONDARY"/* "$DATALAKE_LOCAL_ROOT/inbox/npm/secondary/"

for day in 2026-08-25 2026-08-26 2026-08-27; do
  for host in primary secondary; do
    dagster asset materialize --select npm_access_raw \
      --partition "$day|$host" -m homelab_datalake.definitions
  done
done
python - <<'PY'
import hashlib
import os
from pathlib import Path
root = Path(os.environ['DATALAKE_LOCAL_ROOT'])
files = sorted(root.rglob('*.parquet'))
assert files, 'No partitions were written'
Path('.demo/partition-digests.txt').write_text('\n'.join(
    f'{p.relative_to(root)} {hashlib.sha256(p.read_bytes()).hexdigest()}' for p in files
))
PY
dagster asset materialize --select npm_access_raw \
  --partition '2026-08-27|primary' -m homelab_datalake.definitions
python - <<'PY'
import hashlib
import os
from pathlib import Path
root = Path(os.environ['DATALAKE_LOCAL_ROOT'])
for line in Path('.demo/partition-digests.txt').read_text().splitlines():
    name, digest = line.rsplit(' ', 1)
    assert hashlib.sha256((root / name).read_bytes()).hexdigest() == digest, name
print('Repeat collection preserved every partition byte for byte.')
PY
dagster job execute -j analytics_daily -m homelab_datalake.definitions
python - <<'PY'
import os
from pathlib import Path
import duckdb
with duckdb.connect(os.environ['DUCKDB_SERVING_PATH'], read_only=True) as con:
    schemas = {row[0] for row in con.execute('SELECT DISTINCT schema_name FROM duckdb_tables()').fetchall()}
    assert schemas == {'marts'}, schemas
    rows = con.execute('SELECT count(*) FROM marts.fct_http_request').fetchone()[0]
    assert rows > 0, 'Synthetic traffic did not reach the serving database'
    result = con.execute('SELECT status, count(*) AS requests FROM marts.fct_http_request GROUP BY status ORDER BY requests DESC').fetchall()
    text = 'Synthetic traffic only\n' + '\n'.join(f'HTTP {status}: {count} requests' for status, count in result)
    Path('.demo/summary.txt').write_text(text + '\n')
    print(text)
    print(f'Published {rows} synthetic requests. Serving copy contains marts only.')
PY
