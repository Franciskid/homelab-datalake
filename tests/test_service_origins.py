"""Exercise the production SQL, including cross-edge public calls and uncertainty."""

import json
from pathlib import Path

import duckdb
import jinja2
import pytest

from homelab_datalake import rebuild_state
from homelab_datalake.service_origins import ENV_NAME, validated_origins

DBT = Path(__file__).resolve().parents[1] / "dbt"


def binding(address="203.0.113.8", edge="primary", start="2026-10-01T00:00:00Z", end=None):
    return {"address": address, "origin_edge": edge, "valid_from": start,
            "valid_until": end, "evidence": "Direct egress observation; historical attribution inferred"}


def test_inventory_rejects_invalid_or_ambiguous_history():
    invalid = [
        {}, ["not a binding"], [dict(binding(), address="not-an-address")],
        [dict(binding(), valid_from="2026-10-01")],
        [dict(binding(), valid_until="2026-09-01T00:00:00Z")],
        [dict(binding(), evidence="")], [dict(binding(), extra="ignored")],
        [binding(), binding(edge="secondary")],
    ]
    for rows in invalid:
        with pytest.raises(ValueError):
            validated_origins(json.dumps(rows))


def test_adjacent_intervals_and_ipv6_are_supported():
    rows = [binding(end="2026-11-01T00:00:00Z"),
            binding(edge="secondary", start="2026-11-01T00:00:00Z"),
            binding(address="2001:db8::8")]
    assert validated_origins(json.dumps(rows)) == rows


def test_inventory_change_invalidates_historical_builds(monkeypatch):
    monkeypatch.setenv(ENV_NAME, "[]")
    before = rebuild_state.code_version()
    monkeypatch.setenv(ENV_NAME, json.dumps([binding()]))
    assert rebuild_state.code_version() != before


@pytest.fixture
def classified():
    con = duckdb.connect()
    registry = jinja2.Template((DBT / "models/intermediate/int_managed_origin_addresses.sql").read_text()).render(
        env_var=lambda _name, _default: json.dumps([
            binding(end="2026-11-01T00:00:00Z"), binding(address="2001:db8::8"),
        ]),
    )
    con.execute("create view registry as " + registry)
    con.execute("create table requests (npm_instance varchar, requested_at timestamptz, client_address varchar, client_trust varchar, is_internal boolean, user_agent varchar, path varchar, host_is_address boolean)")
    sql = jinja2.Template((DBT / "models/intermediate/int_npm_requests.sql").read_text()).render(
        ref=lambda name: "requests" if name == "stg_npm_access" else "registry",
    )

    def run(address="203.0.113.8", ua="Blackbox-Exporter/1.0", path="/health",
            proxy="secondary", trust="resolved", private=False, at="2026-10-15T12:00:00Z"):
        con.execute("delete from requests")
        con.execute("insert into requests values (?, ?, ?, ?, ?, ?, ?, false)",
                    [proxy, at, address, trust, private, ua, path])
        result = con.execute(sql)
        rows = [dict(zip([d[0] for d in result.description], row, strict=True)) for row in result.fetchall()]
        assert len(rows) == 1, "Attribution must not multiply requests"
        return rows[0]

    yield run
    con.close()


def test_cross_edge_public_monitoring_is_identified_without_changing_legacy_flag(classified):
    row = classified()
    assert row["is_internal"] is False
    assert row["is_internal_service"] is True
    assert row["npm_instance"] == "secondary"
    assert row["origin_edge"] == "primary"
    assert row["origin_service"] == "monitoring"
    assert row["origin_confidence"] == "inferred"
    assert "Direct egress observation" in row["origin_evidence"]
    assert "service signature: monitoring" in row["origin_evidence"]


@pytest.mark.parametrize(("ua", "service"), [("duckdb/v1.0 python/3.12", "datalake_query"), ("Boto3/1.0", "datalake_storage"), ("aws-sdk-js/3.0", "datalake_storage")])
def test_storage_calls_require_managed_source_and_matching_path(classified, ua, service):
    row = classified(ua=ua, path="/datalake/raw/table/data.parquet")
    assert row["is_internal_service"] is True
    assert row["origin_service"] == service
    assert classified(ua=ua, path="/other")["is_internal_service"] is None
    assert classified(address="198.51.100.9", ua=ua, path="/datalake/raw")["is_internal_service"] is False


def test_a_browser_on_the_same_egress_is_not_claimed_to_be_a_service(classified):
    row = classified(ua="Mozilla/5.0 Chrome/1.0", path="/stream/camera")
    assert row["traffic_origin"] == "managed_network"
    assert row["is_internal_service"] is None
    assert row["origin_service"] is None
    assert "browser may share this egress" in row["origin_evidence"]


def test_managed_log_mirror_crosses_the_public_edge(classified):
    row = classified(ua="MinIO (linux; amd64) minio-go/v7.0 mc/RELEASE.2026", path="/datalake/inbox/npm/secondary/access.log")
    assert row["is_internal_service"] is True
    assert row["origin_service"] == "edge_log_mirror"
    assert row["origin_edge"] != row["npm_instance"]


@pytest.mark.parametrize("at", ["2026-09-30T23:59:59Z", "2026-11-01T00:00:00Z"])
def test_an_address_does_not_classify_requests_outside_its_binding(classified, at):
    row = classified(at=at)
    assert row["traffic_origin"] == "unmanaged_network"
    assert row["origin_edge"] is None
    assert row["is_internal_service"] is False


@pytest.mark.parametrize("trust", ["cdn_edge", "unknown", None])
def test_unresolved_clients_are_never_attributed_from_a_proxy_address(classified, trust):
    row = classified(trust=trust)
    assert row["traffic_origin"] == "unresolved"
    assert row["is_internal_service"] is None
    assert row["origin_service"] is None
    assert row["origin_edge"] is None


def test_private_network_and_ipv6_paths(classified):
    private = classified(address="192.168.1.5", private=True)
    assert private["is_internal_service"] is True
    assert private["origin_edge"] is None
    assert classified(address="2001:db8::8")["origin_edge"] == "primary"
    assert classified(address=None)["is_internal_service"] is None
