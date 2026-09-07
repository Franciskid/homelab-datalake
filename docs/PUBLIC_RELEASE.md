# What this public version contains

The public history preserves all 93 original commits in order, including their
author names, dates, and development steps. Private email addresses use a GitHub
noreply address. Private infrastructure references, operational measurements,
career notes, and copied fixture details are sanitized in each historical version.
Commit hashes necessarily change when their contents or ancestors change.

The code, models, tests, skills, and relevant design notes remain inspectable.
Historical deployment scripts and configuration now use example infrastructure;
configure your own services before using them. A later commit adds the isolated
synthetic demo, public README, recording, and CI setup.

No live databases, lake files, access logs, camera/media files, workbooks, local
runtime state, credentials, or `.claude` folders are included. The original
private checkout is unchanged. The recording shows a local Dagster instance
running generated web logs; optional live connectors are cleared. It includes
real execution, catalog folders, table metadata, SQL and data checks. Waiting
periods are shortened for readability.

## Data handling is separate from source sharing

Publishing code does not make collected data suitable for publication. LiteLLM
payloads and metadata, camera descriptions, audit logs, identifiers, and coarse
networks can contain sensitive information. Keep real records and generated
reports private. Configure your own storage, identity service, and source access.

## Maintaining the public boundary

Review every commit being published, not only the latest tree. Run lint, unit
tests, and the isolated pipeline before merging. Scan the reachable Git history
for secrets and private infrastructure references. Preserve commit boundaries
when additional historical sanitation is needed.

This release updates the public example and does not change running homelab
services.
