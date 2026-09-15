"""The three asset checks' pass/fail logic, tested as the pure functions they are
pulled out to be -- see assets/ai_platform.py for where each is actually wired into
an AssetCheckResult.
"""

from homelab_datalake.assets.ai_platform import (
    frame_count_check_passed,
    row_count_check_passed,
)
from homelab_datalake.parsing.ai_platform_registry import registry_coverage

# --------------------------------------------------------------------------------
# (a) per-partition row count reconciliation
# --------------------------------------------------------------------------------


def test_row_count_check_passes_when_extracted_equals_landed():
    assert row_count_check_passed(extracted=100, landed=100) is True


def test_row_count_check_fails_on_a_short_write():
    assert row_count_check_passed(extracted=100, landed=97) is False


def test_row_count_check_fails_on_an_over_write():
    """A write that somehow duplicated rows is just as wrong as one that dropped
    them, and the check does not privilege one direction over the other."""
    assert row_count_check_passed(extracted=100, landed=103) is False


def test_row_count_check_passes_on_an_empty_partition():
    assert row_count_check_passed(extracted=0, landed=0) is True


# --------------------------------------------------------------------------------
# (b) registry coverage against information_schema
# --------------------------------------------------------------------------------


def test_coverage_check_passes_when_nothing_is_missing():
    missing, _stale = registry_coverage(["runs", "agents"])
    assert not missing  # this is exactly the `passed=not missing` the asset checks


def test_coverage_check_fails_when_a_live_table_has_no_spec():
    missing, _stale = registry_coverage(["runs", "agents", "a_new_table_nobody_registered"])
    assert missing == ["a_new_table_nobody_registered"]


def test_coverage_check_does_not_fail_for_a_registry_entry_with_no_live_table():
    """`stale` is reported for visibility but must never fail the check on its own
    -- a table dropped upstream is safe to keep landing zero rows for."""
    missing, stale = registry_coverage(["runs"])  # "agents" and everything else missing from live
    assert missing == []
    assert "agents" in stale  # reported...
    # ...but the pass/fail rule the asset actually uses is `not missing`, which is
    # still true here regardless of how long `stale` is.


# --------------------------------------------------------------------------------
# (c) frame count vs objects written
# --------------------------------------------------------------------------------


def test_frame_count_check_passes_when_every_frame_has_an_object():
    assert frame_count_check_passed(frames_expected=12, objects_present=12) is True


def test_frame_count_check_fails_when_an_object_is_missing():
    assert frame_count_check_passed(frames_expected=12, objects_present=10) is False


def test_frame_count_check_fails_when_a_stale_object_was_not_cleaned_up():
    assert frame_count_check_passed(frames_expected=5, objects_present=8) is False


def test_frame_count_check_passes_when_there_were_no_frames_at_all():
    assert frame_count_check_passed(frames_expected=0, objects_present=0) is True
