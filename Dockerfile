FROM python:3.12-slim

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

RUN apt-get update \
 && apt-get install -y --no-install-recommends git \
 && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir -e ".[geo]"

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
