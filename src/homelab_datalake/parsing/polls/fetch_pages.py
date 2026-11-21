# Vendored from the politico repository (scripts/pollslib/fetch_pages.py, 6db2c47). Do not edit here: change it there and run scripts/vendor-pollslib.py.
"""Render public listing pages when ordinary HTTP does not expose their links."""
import json
import os
import re
import subprocess
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen


class BlockedSource(RuntimeError):
    """A publisher declined automated access."""


def check_access(raw, status=200):
    if status in (401, 403, 407, 429, 451) or re.search(
        rb"<title[^>]*>\s*(?:just a moment|access denied|attention required|request blocked|captcha)|"
        rb"<form[^>]+(?:captcha|challenge-form)|<script[^>]+src=[\x22\x27]/cdn-cgi/challenge-platform/", raw, re.I
    ):
        raise BlockedSource("publisher blocked automated access or requested a CAPTCHA")


class PageFetcher:
    def __init__(self, fetch, log):
        self.fetch = fetch
        self.log = log
        self.cloud_calls = self.cloud_credits = 0

    def __call__(self, url, usable):
        try:
            data, _ = self.fetch(url)
            check_access(data)
            if usable(data):
                return data
        except HTTPError as error:
            check_access(b"", error.code)
            if error.code in (404, 410):
                raise
        except BlockedSource:
            raise
        except OSError:
            pass

        try:
            binary = os.environ.get("POLLS_FIRECRAWL_LOCAL", str(Path.home() / ".cargo/bin/firecrawl-local"))
            result = subprocess.run(
                [binary, "scrape", "--public-only", "--browser", "always", "--format", "rawHtml", "--timeout", "20000", url],
                env={**os.environ, "FIRECRAWL_CLOUD": "off"}, capture_output=True, timeout=30,
            )
            if re.search(rb"blocked|captcha|\b(?:401|403|429|451)\b", result.stderr, re.I):
                raise BlockedSource("Firecrawl Local reported a publisher block")
            payload = json.loads(result.stdout)
            page = self.page(payload, usable)
            if page is not None:
                self.log(f"polls: rendered {url} with Firecrawl Local")
                return page
        except (OSError, ValueError, subprocess.TimeoutExpired):
            pass

        key_path = Path(os.environ.get("POLLS_FIRECRAWL_KEY_FILE", Path.home() / ".config/politico/firecrawl.key"))
        if key_path.is_file() and self.cloud_calls < 2:
            key = key_path.read_text().strip()
            if key:
                self.cloud_calls += 1
                request = Request(
                    "https://api.firecrawl.dev/v2/scrape",
                    json.dumps({"url": url, "formats": ["rawHtml"], "proxy": "basic", "timeout": 20000}).encode(),
                    {"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                )
                try:
                    with urlopen(request, timeout=25) as response:
                        payload = json.load(response)
                    if payload.get("success") and payload.get("data", {}).get("rawHtml"):
                        self.cloud_credits += 1
                    self.log(f"polls: Firecrawl cloud request {self.cloud_calls}, {self.cloud_credits} estimated credits this run")
                    page = self.page(payload, usable)
                    if page is not None:
                        return page
                except (OSError, ValueError):
                    self.log(f"polls: Firecrawl cloud request {self.cloud_calls} failed for {url}")
        raise OSError("listing could not be read over HTTP or Firecrawl")

    @staticmethod
    def page(payload, usable):
        data = payload.get("data") or {}
        meta = data.get("metadata") or {}
        raw = (data.get("rawHtml") or "").encode()
        check_access(raw, meta.get("statusCode", 200))
        if meta.get("blocked"):
            raise BlockedSource("Firecrawl reported a publisher block")
        return raw if raw and usable(raw) else None
