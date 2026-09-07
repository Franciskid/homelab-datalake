# Source from the repository root after activating your virtual environment.
# Reset every connector so the demo always runs on synthetic, local records.
unset DATALAKE_S3_ENDPOINT DATALAKE_S3_ACCESS_KEY_ID DATALAKE_S3_SECRET_ACCESS_KEY
unset CAMERA_EXPORT_URL CAMERA_OIDC_TOKEN_URL CAMERA_OIDC_CLIENT_ID CAMERA_OIDC_CLIENT_SECRET
unset LITELLM_PG_HOST LITELLM_PG_PASSWORD JELLYFIN_URL JELLYFIN_API_KEY TMDB_API_KEY
unset FAILURE_WEBHOOK_URL GEOIP_CITY_DB GEOIP_ASN_DB MCP_OIDC_ISSUER
unset DATALAKE_API_ALLOW_ANONYMOUS DATALAKE_API_ALLOW_WRITES
export CLIENT_IP_HASH_SALT=synthetic-demo-only
export DATALAKE_LOCAL_ROOT="$PWD/.demo/lake"
export NPM_HOSTS=primary,secondary
export NPM_LOG_DIR_PRIMARY="$PWD/.demo/seed/primary"
export NPM_LOG_DIR_SECONDARY="$PWD/.demo/seed/secondary"
export DUCKDB_PATH="$PWD/.demo/warehouse/datalake.duckdb"
export DUCKDB_SERVING_PATH="$PWD/.demo/warehouse/serving.duckdb"
export DATALAKE_EXPORT_DIR="$PWD/.demo/exports"
export DAGSTER_HOME="$PWD/.demo/dagster"
export DBT_PROFILES_DIR="$PWD/dbt"
