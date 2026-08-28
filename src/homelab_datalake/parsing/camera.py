"""Reading camera events over HTTPS, authenticated by a Keycloak service account.

The camera runs at another site. It is reached the way anything else would be -- its
public URL, with a token -- not by opening a tunnel between the two. The token comes
from a `client_credentials` grant against the homelab realm and carries an `export:read`
client role, so the pipeline's access is granted and revoked in Keycloak beside every
other account.

The export endpoints do the redaction (no zone geometry, no media, hashed identities);
this module's job is the range contract and the late-arrival handling.

**Late arrival is the interesting part.** Vision analysis is asynchronous: an event
lands with `lifecycle` still `candidate`, no `confidence` and no `summary`, and gets
them minutes or hours later. So re-reading yesterday tomorrow legitimately returns
*different* rows than it did today. Every row carries `_extracted_at`, and the mart
keeps the newest version per event id. "Re-running a partition replaces it exactly"
becomes the more precise "re-running replaces a partition with the current truth for
that date" -- nothing is silently overwritten, and no analysis result is lost to a race.
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from datetime import time as dtime

ENDPOINTS = ("events", "clips", "logs")

# Both hosts sit behind Cloudflare, which rejects urllib's default User-Agent
# ("Python-urllib/3.12") with a 403 before the request ever reaches the origin. That
# looks exactly like an authorisation failure and is not one, so the agent is set
# explicitly and named after this pipeline -- which also makes the traffic identifiable
# in the very access logs this warehouse ingests.
USER_AGENT = "homelab-datalake/1.0 (+https://data.example.test)"

# A token is good for minutes; refresh a little early rather than discovering expiry
# halfway through a backfill.
_TOKEN_SKEW_SECONDS = 30


@dataclass
class CameraExtract:
    events: list[dict] = field(default_factory=list)
    clips: list[dict] = field(default_factory=list)
    logs: list[dict] = field(default_factory=list)
    extracted_at: datetime | None = None

    def rows(self, kind: str) -> list[dict]:
        return getattr(self, kind)


class _TokenCache:
    """One token per process, refreshed shortly before it expires.

    A backfill runs one partition per process, but a sensor re-materialising the
    current day can hit this repeatedly; asking Keycloak for a fresh token on every
    request would be a needless round trip and a needless load on the front door.
    """

    def __init__(self) -> None:
        self._token: str | None = None
        self._expires_at: float = 0.0

    def get(self, token_url: str, client_id: str, client_secret: str) -> str:
        if self._token and time.time() < self._expires_at:
            return self._token

        data = urllib.parse.urlencode(
            {
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
            }
        ).encode()
        if not token_url.startswith("https://"):
            raise ValueError(f"CAMERA_OIDC_TOKEN_URL must be https, got {token_url!r}")
        request = urllib.request.Request(  # noqa: S310 - scheme checked above
            token_url,
            data=data,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "User-Agent": USER_AGENT,
            },
        )
        with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310
            payload = json.loads(response.read().decode("utf-8"))

        self._token = payload["access_token"]
        self._expires_at = time.time() + max(30, int(payload.get("expires_in", 60)) - _TOKEN_SKEW_SECONDS)
        return self._token


_TOKENS = _TokenCache()


def settings() -> dict:
    return {
        "base_url": os.environ.get("CAMERA_EXPORT_URL", ""),
        "token_url": os.environ.get("CAMERA_OIDC_TOKEN_URL", ""),
        "client_id": os.environ.get("CAMERA_OIDC_CLIENT_ID", ""),
        "client_secret": os.environ.get("CAMERA_OIDC_CLIENT_SECRET", ""),
    }


def fetch_ndjson(base_url: str, endpoint: str, token: str, start: datetime, end: datetime) -> list[dict]:
    query = urllib.parse.urlencode(
        {
            "from": start.astimezone(UTC).isoformat().replace("+00:00", "Z"),
            "to": end.astimezone(UTC).isoformat().replace("+00:00", "Z"),
        }
    )
    url = f"{base_url.rstrip('/')}/api/export/{endpoint}?{query}"
    if not url.startswith("https://"):
        raise ValueError(f"CAMERA_EXPORT_URL must be https, got {base_url!r}")

    request = urllib.request.Request(  # noqa: S310 - scheme checked above
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/x-ndjson",
            "User-Agent": USER_AGENT,
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=120) as response:  # noqa: S310
            body = response.read().decode("utf-8")
    except urllib.error.HTTPError as error:
        # 401 and 403 mean the token or the role, not the data. Saying which saves
        # reading the camera's logs to find out.
        if error.code in (401, 403):
            raise PermissionError(
                f"camera export refused the token ({error.code}). Check that the "
                f"datalake-exporter service account still holds export:read."
            ) from error
        raise

    return [json.loads(line) for line in body.splitlines() if line.strip()]


def extract_day(day: date, base_url: str | None = None, token: str | None = None,
                payloads: dict[str, list[dict]] | None = None) -> CameraExtract:
    """One day of camera exports. `payloads` is injectable so tests need no server."""
    start = datetime.combine(day, dtime.min, tzinfo=UTC)
    end = start + timedelta(days=1)
    extracted_at = datetime.now(UTC)

    if payloads is None:
        config = settings()
        base_url = base_url or config["base_url"]
        if not base_url:
            raise ValueError("CAMERA_EXPORT_URL is required")
        token = token or _TOKENS.get(config["token_url"], config["client_id"], config["client_secret"])
        payloads = {
            endpoint: fetch_ndjson(base_url, endpoint, token, start, end) for endpoint in ENDPOINTS
        }

    out = CameraExtract(extracted_at=extracted_at)
    for endpoint in ENDPOINTS:
        rows = []
        for row in payloads.get(endpoint, []):
            enriched = dict(row)
            enriched["dt"] = day
            # Stamped here rather than taken from the payload: it records when *this
            # pipeline* saw the row, which is what lets the mart pick the latest
            # version of an event whose analysis landed after the first read.
            enriched["_extracted_at"] = extracted_at
            rows.append(enriched)
        setattr(out, endpoint, rows)

    return out
