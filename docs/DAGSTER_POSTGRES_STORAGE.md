# Dagster's storage on Postgres

**Applied 2 September 2026, ~20:39 UTC.** Dagster's run, event-log and schedule
storage now live in the shared Postgres (`postgresql_postgres_1`, database
`dagster`) instead of the SQLite files it used by default. This document records
what was done and why, and is the runbook for the one step still outstanding.

**Why:** `ai-platform` adds a second Dagster code location,
`ai_automations`, served over gRPC by its own container. Once that location's jobs
execute, their run and event writes happen from a subprocess inside *that*
container, not from `dagster-daemon`. SQLite is a single-writer store; two
containers writing run data at once is precisely what it does not support. See
[`dagster.yaml`](../dagster.yaml) for the per-setting reasoning.

---

## 1. What was provisioned

On `postgresql_postgres_1`, as the `postgres` superuser:

```sql
CREATE ROLE dagster_rw WITH LOGIN PASSWORD '<generated>' CONNECTION LIMIT 40;
CREATE DATABASE dagster OWNER dagster_rw;
REVOKE CONNECT ON DATABASE dagster FROM PUBLIC;
\c dagster
ALTER SCHEMA public OWNER TO dagster_rw;   -- Dagster autocreates its own tables
```

A **separate role from `datalake_ro`**, deliberately: that one is read-only by
design (`IMPLEMENTATION_PLAN.md` §4/§5) and Dagster's storage must create tables
and write rows.

[Private career context or unrelated service inventory omitted.]

The matching `DAGSTER_PG_*` values are in this repo's `.env` on the server (not
here, and not in `.env.example` beyond placeholders). The password was generated
on the server with `openssl rand -hex 24` and piped straight into `psql` on
stdin, so it never appeared in a process list, a shell history or a transcript.

## 2. Two settings that are load-bearing

**`--db-pool-max-overflow 5` on the webserver.** The webserver — and only the
webserver — calls `optimize_for_webserver()`, which replaces each storage class's
default `NullPool` with a real pool of `pool_size=1 + max_overflow`. Dagster's
stock `max_overflow` is **20**, so the default behaviour is up to `3 × 21 = 63`
connections from this one container, against a server-wide limit of 100 shared
with everything above. 5 caps it at `3 × 6 = 18`, which is ample for a UI with a
single user. The daemon and run subprocesses keep `NullPool` and hold nothing
open.

**`dagster-postgres==0.29.20`, pinned exactly** (`pyproject.toml`). Every
`dagster-postgres` release pins one exact `dagster` version; 0.29.20 pins
`dagster==1.13.20`, which is what this image runs and what `ai_automations` pins
on the other side of the gRPC link. This file otherwise only asks for
`dagster>=1.9`, so a floating `>=0.25` would have let a resolver pick the newest
`dagster-postgres` and drag `dagster` up with it, breaking that match silently on
the next rebuild. Bump this and the pins in `apps/dagster-automations/` together,
or not at all.

## 3. Schedules do NOT survive the move — read this before repeating it

All seven schedules in `definitions.py` are declared
`default_status=DefaultScheduleStatus.STOPPED`. Six of them were **on** only
because someone toggled them on in the UI, and that state lives in schedule
storage. Moving schedule storage to an empty Postgres therefore reverts every
schedule to STOPPED — **silently stopping the entire nightly pipeline**, with
nothing in any log to say so.

The six that were running were captured before the cutover and restarted after
it:

```bash
for s in npm_daily_schedule litellm_daily_schedule jellyfin_daily_schedule \
         camera_daily_schedule camera_late_arrival_schedule analytics_daily_schedule; do
  docker exec datalake-webserver dagster schedule start -m homelab_datalake.definitions "$s"
done
```

`camera_zones_schedule` was off before and was left off. **Do not use
`dagster schedule start --start-all`** — it would turn that one on too.

To capture the list before any future storage move, read the old store directly
rather than trusting memory:

```bash
docker exec datalake-webserver python -c "
import sqlite3, json
con = sqlite3.connect('file:/dagster_home/schedules/schedules.db?mode=ro', uri=True)
for st, ty, body in con.execute('select status, instigator_type, instigator_body from instigators'):
    print(ty, st, json.loads(body)['origin'].get('job_name'))"
```

Sensor state resets too: `notify_on_failure` reinitialised its cursor to
`record_id=-1`, so it watches from the cutover forward and will not replay
earlier failures. Harmless here, but worth expecting.

## 4. Run history was not migrated

The SQLite files stay on the `homelab-datalake_dagster_home` volume, untouched
and unread. The new database started empty.

**Checked against the installed `dagster==1.13.20` CLI, not assumed:** the only
built-in command that touches storage schema is `dagster instance migrate`, which
upgrades an *already-selected* backend's schema. It has no path that reads one
backend and writes another. Dagster 1.13 ships no supported way to copy run or
event history between storage backends. Migrating it would mean a hand-written
script against the Sqlite\* and Postgres\* storage classes — a separate exercise,
against a copy of the files, not a precondition for this change.

Leaving the SQLite files in place is what makes §6 restore the *exact* previous
state, run history included.

## 5. How this was verified

- `dagster_rw` authenticates from `datalake-webserver` over `primary_reseau` and
  can `CREATE TABLE`; the role is non-superuser, cannot create databases.
- Both containers started clean; 22 tables autocreated in `dagster.public`.
- Three real `camera_daily` runs launched through the ordinary run launcher all
  reached **SUCCESS**, with [count withheld] event-log rows in Postgres.
- The webserver's GraphQL reports the `homelab_datalake.definitions` location
  loaded.
- All six schedules read back `[RUNNING]`, `camera_zones_schedule` `[STOPPED]`.
- `dagster_rw` held 1 connection; server-wide usage 27/100, i.e. below where it
  started.

**A trap worth naming, because this document originally walked into it.** The
obvious check — "`runs.db`'s mtime should not change" — is misleading if the
baseline is taken before the restart: both SQLite files' mtimes advance during
the cutover, and `schedules.db` grows by ~[volume withheld]. That is the *old* containers
still working normally right up to shutdown, plus a final WAL checkpoint on
close. Take the baseline **after** the new containers start
(`docker inspect ... .State.StartedAt`), then confirm it is unchanged by runs
that happen later. Here: both files last changed at 20:39:25/20:39:36, the
containers started at 20:39:42, and the three runs at 20:42 left them untouched.

## 6. Rollback

`docker-compose.yml` is the only file that changed behaviour:

```bash
cd ~/projects/homelab-datalake
git checkout -- docker-compose.yml       # or drop the command:/dagster.yaml mounts
docker compose up -d dagster-webserver dagster-daemon
```

Both containers return to the image's own command and stop mounting
`dagster.yaml`, so Dagster falls back to the SQLite files on the volume — with
their run history and, importantly, **their schedule state**, so the six
schedules come back on by themselves. The `dagster` database and `dagster_rw`
role can be left in place; nothing reads them once the mount is gone.

## 7. Still outstanding: the second code location

This rollout was deliberately **split in two**, and only the first half is done.
The storage move stands on its own and is what the multi-writer problem actually
required; loading `ai_automations` is a separate change that depends on a
container in the other repo being deployed first.

[`workspace.yaml`](../workspace.yaml) exists in this repo but is **not mounted
and not referenced** — it is inert. To finish, once `ai-automations` is deployed
and healthy on `primary_reseau`:

1. Mount it into both services: `- ./workspace.yaml:/app/workspace.yaml:ro`.
2. Swap `-m homelab_datalake.definitions` for `-w /app/workspace.yaml` in both
   commands (keep `--db-pool-max-overflow 5` on the webserver).
3. `docker compose up -d dagster-webserver dagster-daemon` — both together, so
   they never disagree about which locations exist.

Doing this before `ai-automations` is up is not fatal, but leaves a permanently
red code location in the UI for no reason.

> Public history note: private identifiers, measurements, and operational details were redacted. Code examples use placeholder infrastructure.
