"""Warehouse tables for the sources added on 2026-09-15.

Kept out of warehouse.py so that file changes by one import and one line, and the
sources other branches add to `_SOURCES` there merge without touching these. Same shape
as `_SOURCES`: (table, lake prefix, declared Arrow schema, upstream asset, description).

A ledger's source columns are whatever the application's table holds on the day, so its
declared schema is the three columns every version carries (parsing/ledger.py). When a
ledger has not landed yet, the warehouse still gets a table with those. Lake-only
datasets (layout.Dataset.lake_only) are deliberately not here.
"""

from homelab_datalake import layout
from homelab_datalake.assets import host_logs, keycloak, media_pipeline, status_page
from homelab_datalake.parsing import ledger
from homelab_datalake.parsing.media_pipeline import ALL_TABLES as MEDIA_PIPELINE_TABLES

ADDED_SOURCES = (
    ("status_page_metric_samples", status_page.SAMPLES_PREFIX, status_page.SAMPLE_SCHEMA,
     status_page.status_page_raw, layout.dataset("status_page_metric_samples").description),
    ("status_page_observations", status_page.OBSERVATIONS_PREFIX, status_page.OBSERVATION_SCHEMA,
     status_page.status_page_raw, layout.dataset("status_page_observations").description),
    *(
        (name, layout.prefix(name), ledger.META_SCHEMA, status_page.status_page_raw,
         layout.dataset(name).description)
        for name in status_page.LEDGERS
    ),
    ("host_auth_log", host_logs.AUTH_PREFIX, host_logs.host_logs.AUTH_SCHEMA,
     host_logs.host_logs_raw, layout.dataset("host_auth_log").description),
    ("keycloak_events", keycloak.EVENTS_PREFIX, keycloak.EVENT_SCHEMA,
     keycloak.keycloak_raw, layout.dataset("keycloak_events").description),
    ("keycloak_admin_events", keycloak.ADMIN_EVENTS_PREFIX, keycloak.ADMIN_EVENT_SCHEMA,
     keycloak.keycloak_raw, layout.dataset("keycloak_admin_events").description),
    *(
        (name, layout.prefix(name), ledger.META_SCHEMA, keycloak.keycloak_raw,
         layout.dataset(name).description)
        for name in keycloak.LEDGERS
    ),
    *(
        (spec.table_name, layout.prefix(spec.table_name), ledger.META_SCHEMA,
         getattr(media_pipeline, f"{spec.app}_raw"), spec.description)
        for spec in MEDIA_PIPELINE_TABLES
    ),
)
