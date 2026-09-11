"""What the failure sensor sends to the platform.

The sensor itself cannot fail loudly -- it swallows its own exceptions so a delivery
problem never compounds the run failure that triggered it -- so the payload it builds
is the part that has to be checked here instead.
"""

import datetime

from homelab_datalake.alerting import alert_payload

WHEN = datetime.datetime(2026, 9, 11, 4, 15, tzinfo=datetime.UTC)


def _alert(**kwargs):
    base = {
        "job_name": "camera_daily",
        "partition": "2026-09-11",
        "run_id": "run-1",
        "detail": "boom",
        "now": WHEN,
    }
    return alert_payload(**{**base, **kwargs})["alerts"][0]


def test_the_same_job_and_partition_keep_one_fingerprint():
    """Eighteen hourly failures must become one incident, not eighteen."""
    first = _alert(run_id="run-1", detail="boom")
    again = _alert(run_id="run-2", detail="a different message entirely")
    assert first["fingerprint"] == again["fingerprint"]


def test_a_different_partition_is_a_different_incident():
    assert _alert(partition="2026-09-10")["fingerprint"] != _alert()["fingerprint"]


def test_a_different_job_is_a_different_incident():
    assert _alert(job_name="litellm_daily")["fingerprint"] != _alert()["fingerprint"]


def test_every_label_value_is_a_string():
    """The receiver validates labels as Record<string, string> and drops the rest."""
    labels = _alert()["labels"]
    assert all(isinstance(v, str) for v in labels.values())
    assert labels["job"] == "camera_daily"
    assert labels["partition"] == "2026-09-11"
    assert labels["alertname"] == "DatalakeRunFailed"


def test_a_failed_publish_outranks_a_failed_source():
    """analytics_daily failing freezes every reader; one source partition does not."""
    assert _alert(job_name="analytics_daily")["labels"]["severity"] == "critical"
    assert _alert(job_name="camera_daily")["labels"]["severity"] == "warning"


def test_the_summary_names_the_job_and_the_partition():
    annotations = _alert()["annotations"]
    assert "camera_daily" in annotations["summary"]
    assert "2026-09-11" in annotations["summary"]
    assert "run-1" in annotations["description"]
    assert "boom" in annotations["description"]


def test_it_is_firing_and_carries_the_time():
    alert = _alert()
    assert alert["status"] == "firing"
    assert alert["startsAt"] == WHEN.isoformat()


def test_an_unpartitioned_job_still_produces_one_alert():
    alert = _alert(job_name="analytics_daily", partition="-")
    assert alert["labels"]["partition"] == "-"
    assert alert["fingerprint"]
