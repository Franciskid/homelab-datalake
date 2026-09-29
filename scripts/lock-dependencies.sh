#!/usr/bin/env bash
# Rewrites requirements.lock from pyproject.toml.
#
# Resolves in the same base image the Dockerfile uses, so the lock holds the wheels
# that image installs (Linux, Python 3.12), not the ones a Mac would pick. Needs Docker.
set -euo pipefail
cd "$(dirname "$0")/.."

header=$(sed '/^[^#]/,$d' requirements.lock)
pins=$(docker run --rm --platform linux/amd64 -v "$PWD":/src:ro python:3.12-slim sh -c '
  cp -r /src /build && cd /build &&
  pip install --quiet --no-cache-dir --root-user-action=ignore ".[geo]" >/dev/null &&
  pip freeze --exclude-editable | grep -vi "^homelab-datalake"')

printf '%s\n%s\n' "$header" "$(printf '%s\n' "$pins" | LC_ALL=C sort -f)" > requirements.lock
echo "requirements.lock: $(grep -vc '^#' requirements.lock) packages"
