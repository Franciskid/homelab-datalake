FROM rust:1.95-slim-trixie AS firecrawl
RUN apt-get update && apt-get install -y --no-install-recommends pkg-config libssl-dev \
 && cargo install --locked --version 0.1.0 firecrawl-local

FROM python:3.12-slim-trixie

# 3.12 rather than 3.13: dbt-core's dependency tree still has rough edges on 3.13,
# and pinning the image is how the container avoids the problem the README documents
# for local development.

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DAGSTER_HOME=/dagster_home \
    DUCKDB_PATH=/warehouse/datalake.duckdb \
    DBT_PROFILES_DIR=/app/dbt \
    DUCKDB_SERVING_PATH=/warehouse/serving.duckdb \
    DATALAKE_EXPORT_DIR=/exports

WORKDIR /app

# systemd for journalctl only: host_logs_raw reads fraserv's journal files from a
# read-only mount. Nothing here runs systemd.
# poppler-utils: pdftotext, which polls_ingestion reads the poll reports with (PDFs are untrusted
# input and are only ever opened through it).
RUN apt-get update \
 && apt-get install -y --no-install-recommends git systemd poppler-utils chromium \
 && rm -rf /var/lib/apt/lists/*

COPY --from=firecrawl /usr/local/cargo/bin/firecrawl-local /usr/local/bin/firecrawl-local
ENV POLLS_FIRECRAWL_LOCAL=/usr/local/bin/firecrawl-local FIRECRAWL_CHROME_PATH=/usr/bin/chromium

# The lock first, on its own layer, so a code change does not reinstall every package.
# Then the project itself with --no-deps: its dependencies are exactly the locked ones,
# and `pip check` fails the build if pyproject.toml asks for something the lock lacks.
COPY requirements.lock ./
RUN pip install --no-cache-dir -r requirements.lock

COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir --no-deps -e ".[geo]" && pip check

COPY dbt ./dbt
# Resolve dbt packages at build time. Installing them at runtime means every image
# rebuild silently loses them and the next `dbt build` fails on a missing dependency
# rather than on anything to do with the change being deployed.
#
# `dbt parse` writes the manifest dagster-dbt reads to build one asset per model. Doing
# it here rather than at startup means the code location boots without invoking dbt,
# and a parse error fails the build -- where it is legible -- instead of presenting
# later as a code location that will not load.
RUN mkdir -p /dagster_home /warehouse /logs /exports
RUN cd dbt && dbt deps && dbt parse

COPY scripts ./scripts

EXPOSE 3000
CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-m", "homelab_datalake.definitions"]
