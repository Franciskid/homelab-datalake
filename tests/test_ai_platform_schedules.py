"""The AI-platform schedules ask only for partitions that exist when they tick.

Dagster checks a requested partition key when the tick is evaluated, not when the code
loads, and one unknown key fails the whole tick. ai_platform_partitions had no end
offset, so today was not a partition until midnight: the 05:00 snapshot tick asked for
today and launched nothing, and the 05:10 window tick asked for today plus six earlier
days and launched none of the seven.

The keys come from the same functions the schedules in definitions.py call. The
schedules themselves are not imported here, because importing definitions needs the
dbt executable on PATH.
"""

from datetime import UTC, date, datetime, time, timedelta

import pytest

from homelab_datalake.assets import ai_platform
from homelab_datalake.parsing.ai_platform_registry import WINDOW_DAYS

# The tick times as definitions.py schedules them ("0 5 * * *" and "10 5 * * *").
SNAPSHOT_TICK = time(5, 0)
WINDOW_TICK = time(5, 10)

# An ordinary day, a month boundary and a year boundary, all well past
# AI_PLATFORM_START so the trailing window never reaches before it.
DAYS = [date(2026, 9, 16), date(2026, 10, 1), date(2027, 1, 1)]


def _at(day: date, tick: time) -> datetime:
    return datetime.combine(day, tick, tzinfo=UTC)


@pytest.mark.parametrize(
    ("tick", "keys_for"),
    [(SNAPSHOT_TICK, ai_platform.snapshot_partition_keys),
     (WINDOW_TICK, ai_platform.window_partition_keys)],
    ids=["snapshot_0500", "window_0510"],
)
@pytest.mark.parametrize("day", DAYS, ids=str)
def test_every_requested_partition_exists_at_the_tick(tick, keys_for, day):
    when = _at(day, tick)
    requested = keys_for(when)
    valid = set(ai_platform.ai_platform_partitions.get_partition_keys(current_time=when))

    assert requested
    unknown = [key for key in requested if key not in valid]
    assert not unknown, (
        f"at {when.isoformat()} the schedule asks for {unknown}, which "
        f"ai_platform_partitions does not have yet (last valid key: {max(valid)})"
    )


def test_snapshot_asks_for_the_day_it_is_taken_on():
    # Pinned so the fix stays on the partitions definition: dating the snapshot
    # yesterday would make the key valid by recording a state that day never had.
    when = _at(DAYS[0], SNAPSHOT_TICK)
    assert ai_platform.snapshot_partition_keys(when) == ["2026-09-16"]


def test_window_asks_for_today_and_the_days_before_it():
    when = _at(DAYS[0], WINDOW_TICK)
    expected = [(DAYS[0] - timedelta(days=back)).isoformat() for back in range(WINDOW_DAYS)]
    assert ai_platform.window_partition_keys(when) == expected
    assert expected[0] == "2026-09-16"


@pytest.mark.parametrize("day", DAYS, ids=str)
def test_the_sweep_covers_every_day_the_window_no_longer_reaches(day):
    when = _at(day, time(5, 30))
    swept = ai_platform.sweep_partition_keys(when)
    window = ai_platform.window_partition_keys(when)
    valid = ai_platform.ai_platform_partitions.get_partition_keys(current_time=when)
    assert swept[0] == ai_platform.AI_PLATFORM_START
    assert sorted(set(swept) | set(window)) == sorted(valid)
    assert not set(swept) & set(window)


def test_the_sweep_leaves_tables_with_lake_objects_alone():
    swept = set(ai_platform.sweep_asset_keys)
    assert "platform_runs_raw" in swept
    assert "platform_media_items_raw" in swept
    assert not swept & {
        "platform_usage_events_raw", "platform_generated_files_raw",
        "platform_message_attachments_raw",
    }
