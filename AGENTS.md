# Public repository

Use branches and merge completed work into main after verification. Write in
plain language and preserve the runnable synthetic demo.

Keep the original commit sequence. Sanitize private identifiers and records in
every historical version before publishing history. Do not include production
data, credentials, private hostnames, or `.claude` directories. Use example
infrastructure and generated fixtures. Do not connect the demo to live sources.
Review staged files and the full reachable history before pushing.

Run `ruff check src scripts tests`, `pytest -q`, and `bash scripts/demo.sh` when
changing pipeline behavior. Demo media must record the actual running app with
synthetic data; inspect the footage before publication.
