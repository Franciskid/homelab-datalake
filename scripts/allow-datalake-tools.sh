#!/usr/bin/env bash
#
# Adds the seven datalake MCP tools to the agent runtime's read-only profile.
#
#     bash scripts/allow-datalake-tools.sh
#
# WHY THIS IS NEEDED
# The runtime is deny-by-default: a tool absent from a profile cannot be called, and
# cannot even be discovered. Registering the server with LiteLLM makes the tools reach
# the gateway; it does not make them reachable by an agent. Both steps are required.
#
# All seven are read-only against a serving copy that holds marts and nothing else --
# no raw addresses, no prompt payloads, no zone geometry. `export_workbook` writes, but
# only into the datalake's own exports volume, and only the result of a query the same
# profile already permits. It sends nothing anywhere.

set -euo pipefail
FRA=ssh.example.test
AI_ROOT=/srv/projects/ai-platform
F="$AI_ROOT/packages/agent-runtime/src/mcp-profiles.ts"

ssh "$FRA" "python3 - <<'PYEOF'
import pathlib, sys
p = pathlib.Path('$F')
t = p.read_text()
if 'web_traffic' in t:
    print('  SKIP  datalake tools already in the profile'); sys.exit(0)

anchor = \"  { baseName: 'finance_research', disposition: 'allow' },\n\"
if t.count(anchor) != 1:
    print('  FAIL  could not find the finance block'); sys.exit(1)

policy = anchor + '''  // datalake: typed analytics over the homelab warehouse. Every one is a read
  // against a serving copy that holds marts only -- no addresses, no prompt
  // payloads, no zone geometry. export_workbook writes a file into the datalake's
  // own exports volume and sends it nowhere.
  { baseName: 'list_datasets', disposition: 'allow' },
  { baseName: 'describe_dataset', disposition: 'allow' },
  { baseName: 'web_traffic', disposition: 'allow' },
  { baseName: 'llm_spend', disposition: 'allow' },
  { baseName: 'camera_activity', disposition: 'allow' },
  { baseName: 'media_library', disposition: 'allow' },
  { baseName: 'export_workbook', disposition: 'allow' },
'''
t = t.replace(anchor, policy, 1)

meta_anchor = 'const TOOL_METADATA: Readonly<Record<string, McpToolMetadata>> = {\n'
if t.count(meta_anchor) != 1:
    print('  FAIL  could not find TOOL_METADATA'); sys.exit(1)

meta = meta_anchor + '''  list_datasets: LOW_READ('Datalake', 'Lists the datasets that can be queried.'),
  describe_dataset: LOW_READ('Datalake', 'Describes one dataset\\'s dimensions and measures.'),
  web_traffic: LOW_READ('Datalake', 'Reads aggregate reverse-proxy traffic. Visitors are hashed at ingestion.'),
  llm_spend: LOW_READ('Datalake', 'Reads LLM cost and usage. Prompts are excluded at the source.'),
  camera_activity: LOW_READ('Datalake', 'Reads camera event counts. No media and no zone geometry.'),
  media_library: LOW_READ('Datalake', 'Reads the film library snapshot.'),
  export_workbook: {
    category: 'Datalake',
    access: 'read-only',
    risk: 'low',
    riskReason: 'Writes a query result as a workbook into the datalake exports volume. Sends nothing.',
    disposition: 'allow',
  },
'''
t = t.replace(meta_anchor, meta, 1)
p.write_text(t)
print('  OK    seven datalake tools added to the read-only profile')
PYEOF"
