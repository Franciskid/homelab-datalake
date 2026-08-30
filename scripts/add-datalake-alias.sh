#!/usr/bin/env bash
# Adds `datalake` to MCP_SERVER_ALIASES so the platform discovers its tools.
#
# Registering the server with LiteLLM puts the tools on the gateway. Adding them to the
# runtime profile makes them callable. This third step is what makes them *discoverable*
# by the API: agent templates and tool listings only look at the aliases named here.
set -euo pipefail
FRA=ssh.example.test
E=/srv/projects/ai-platform/deployment/.env.production
ssh "$FRA" "python3 - <<'PYEOF'
import pathlib, sys
p = pathlib.Path('$E')
t = p.read_text()
line = [l for l in t.splitlines() if l.startswith('MCP_SERVER_ALIASES=')]
if not line:
    print('  FAIL  MCP_SERVER_ALIASES not found'); sys.exit(1)
old = line[0]
aliases = [a.strip() for a in old.split('=', 1)[1].split(',') if a.strip()]
if 'datalake' in aliases:
    print('  SKIP  datalake already listed'); sys.exit(0)
aliases.append('datalake')
new = 'MCP_SERVER_ALIASES=' + ','.join(aliases)
p.write_text(t.replace(old, new, 1))
print('  OK   ', new)
PYEOF"
