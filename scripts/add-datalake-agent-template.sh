#!/usr/bin/env bash
#
# Adds a "Datalake Analyst" agent template to the platform.
#
#     bash scripts/add-datalake-agent-template.sh
#
# A template is the idiomatic way an agent is created here: the UI lists it, checks its
# prerequisites against what the MCP gateway can actually discover right now, and
# creates the agent in one click. Writing the agent row directly into the database would
# skip the validation the API does and is not how this codebase works.
set -euo pipefail
FRA=ssh.example.test
F=/srv/projects/ai-platform/apps/api/src/runtime/agent-templates.ts

ssh "$FRA" "python3 - <<'PYEOF'
import pathlib, sys
p = pathlib.Path('$F')
t = p.read_text()
if 'datalake-analyst' in t:
    print('  SKIP  the template already exists'); sys.exit(0)

anchor = 'const SPECS: TemplateSpec[] = [\n'
if t.count(anchor) != 1:
    print('  FAIL  could not find SPECS'); sys.exit(1)

spec = anchor + '''  {
    id: 'datalake-analyst',
    name: 'Datalake Analyst',
    category: 'Datalake',
    description:
      'Answers questions about the homelab warehouse: reverse-proxy traffic, LLM spend, camera activity and the film library. Reads through a typed semantic layer, never SQL, and can publish what it finds as a report or a workbook.',
    toolIds: [
      'list_datasets',
      'describe_dataset',
      'web_traffic',
      'llm_spend',
      'camera_activity',
      'media_library',
      'export_workbook',
      'publish_report',
      'create_word_document',
    ],
    approvalPolicy: 'operator',
    systemPrompt:
      'You are the Datalake Analyst for the Homelab homelab. Answer with figures the ' +
      'warehouse actually returns, and never from memory or training data.\\\\n\\\\n' +
      'The tools are a semantic layer, not a database. There is no SQL parameter: you ' +
      'choose a dataset, a date range, a grain, dimensions and measures, and every name ' +
      'you use is looked up in a registry. If you do not know what a dataset offers, ' +
      'call describe_dataset before guessing a name.\\\\n\\\\n' +
      'Two facts about this data change what the numbers mean, and you must respect ' +
      'both. First, `visitors` counts only requests where the real client address was ' +
      'known; before the 27 Aug 2026 real-IP cutover most requests logged a Cloudflare ' +
      'edge instead, so a visitor count spanning that date is not a like-for-like ' +
      'series and you should say so. Second, geography and network operator are derived ' +
      'at ingestion and the address is discarded immediately; only partitions built ' +
      'after 30 Aug 2026 carry them, so a country breakdown over older days will be ' +
      'null rather than wrong.\\\\n\\\\n' +
      'Use export_workbook when somebody wants the rows themselves, and publish_report ' +
      'or create_word_document when they want the reading of them. State the date range ' +
      'you used in every answer: a number without its window is not an answer.',
    monthlyBudgetEur: 5,
    rateLimitRpm: 6,
    requirements: [
      {
        id: 'datalake-mcp',
        label: 'Datalake MCP server',
        tools: ['web_traffic', 'llm_spend'],
        detailReady: 'The datalake tools are discoverable through the MCP gateway.',
        detailMissing:
          'The datalake tools are not discoverable. Check that the datalake entry in litellm/config.yaml carries oauth2_flow: client_credentials, that DATALAKE_MCP_CLIENT_SECRET is set, and that datalake is listed in MCP_SERVER_ALIASES.',
      },
      {
        id: 'datalake-publishing',
        label: 'Reporting',
        tools: ['publish_report'],
        detailReady: 'Findings can be published as a report or a workbook.',
        detailMissing: 'The platform reporting builtin is required.',
      },
    ],
  },
'''
p.write_text(t.replace(anchor, spec, 1))
print('  OK    Datalake Analyst template added')
PYEOF"
