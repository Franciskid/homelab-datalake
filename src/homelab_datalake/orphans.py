"""Runs whose process died with the container.

Every run executes inside the daemon container: the queued run coordinator launches
it there, and deploy.sh's `dagster job execute` runs there too. So a run Dagster still
calls STARTED, but which started before this container did, has no process left.
Dagster cannot tell on its own -- the default run launcher has no worker health
check -- and such a run stayed STARTED for five days on 16 Nov 2026, holding the
analytics queue. Each deploy recreates the container, so each deploy used to leave
one. Run monitoring's max runtime would end them eventually; this ends them at once.
"""

import os
from pathlib import Path

# A run created in the second the container came up may carry a start time a hair
# before PID 1's; this margin keeps such a run from being mistaken for an orphan.
GRACE_SECONDS = 30


def container_started_at(proc: str = "/proc") -> float | None:
    """When PID 1 of this container started, as a Unix time. None outside the daemon."""
    try:
        cmdline = Path(proc, "1", "cmdline").read_bytes().replace(b"\0", b" ")
        if b"dagster-daemon" not in cmdline:
            return None
        stat = Path(proc, "1", "stat").read_text()
        # Field 22 (starttime, clock ticks since boot). The process name in field 2
        # may contain spaces, so count from the closing parenthesis.
        start_ticks = int(stat.rsplit(")", 1)[1].split()[19])
        boot = next(
            int(line.split()[1])
            for line in Path(proc, "stat").read_text().splitlines()
            if line.startswith("btime ")
        )
        return boot + start_ticks / os.sysconf("SC_CLK_TCK")
    except (OSError, ValueError, StopIteration, IndexError):
        return None


def orphaned(records, started_at: float) -> list:
    """The STARTED/STARTING run records that began before the container did."""
    return [
        record
        for record in records
        if record.start_time is not None and record.start_time < started_at - GRACE_SECONDS
    ]
