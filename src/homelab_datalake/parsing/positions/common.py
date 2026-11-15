# Vendored from the politico repository (scripts/positions/common.py, e83b8f0). Do not edit here: change it there and run scripts/vendor-positions.py.
"""Shared paths, name normalisation and a polite, cached HTTP fetcher.

Everything here is stdlib. Scripts that read downloaded data run with `python3 -I`, so each
entry point puts this directory on sys.path itself (see bootstrap at the top of each script).
"""
from __future__ import annotations

import gzip
import hashlib
import http.client
import http.cookiejar
import json
import os
import re
import ssl
import sys
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
import urllib.robotparser
import zlib
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
# POSITIONS_ROOT makes another directory play the role of the repository root (the server keeps its collected files there;
# paths stored in the database are relative to it, so they are the same on the Mac).
ROOT = Path(os.environ['POSITIONS_ROOT']).resolve() if os.environ.get('POSITIONS_ROOT') else HERE.parent.parent
RESEARCH = ROOT / 'research' / 'positions'
RAW = RESEARCH / 'raw'
# POSITIONS_DB points a run at another database file (e.g. research/positions/positions-r3.sqlite) with the same schema.
DB_PATH = Path(os.environ['POSITIONS_DB']).resolve() if os.environ.get('POSITIONS_DB') else RESEARCH / 'positions.sqlite'

# Generic, honest identification. No personal contact details on purpose.
USER_AGENT = 'Mozilla/5.0 (compatible; observatoire-positions/0.1; non-commercial research crawler; respects robots.txt)'

# Datasets whose licence asks to be cited wherever its figures are shown. The citation text lives here and nowhere else:
# a page, an export or a figure caption that uses one of these datasets takes it from `citation_for` (build-site-data.py
# refuses to publish positions built from a dataset without writing it into the `citations` of the file).
CITATIONS = {
    'ep-votewatch': {
        'text': ('VoteWatch Europe, votes par appel nominal du Parlement européen, juillet 2004 – juin 2019 (législatures 6 à 8), '
                 'données déposées par Simon Hix, European University Institute, Cadmus, http://hdl.handle.net/1814/74918. '
                 'Usage académique, sans but lucratif ; données sources : Parlement européen.'),
        'licence': 'VoteWatch Europe roll-call data, S. Hix (EUI Cadmus, hdl 1814/74918); academic, not-for-profit use, cite the dataset; source data European Parliament',
        # any of these in an item's source (url, title, id) marks it as built from the dataset
        'markers': ('cadmus.eui.eu', 'hdl.handle.net/1814/74918', 'ephix-', 'votewatch'),
    },
    'archelec': {
        'text': ('Archives électorales, Sciences Po / CEVIPOF, collection « Archives électorales du CEVIPOF » (Internet Archive, '
                 'https://archive.org/details/archiveselectoralesducevipof), licence Creative Commons BY-NC-ND 4.0. '
                 'Extraits cités avec attribution, sans usage commercial ; les documents ne sont ni reproduits ni modifiés.'),
        'licence': 'Creative Commons BY-NC-ND 4.0 (Sciences Po, Archives électorales du CEVIPOF)',
        'markers': ('archiveselectoralesducevipof', 'archelec', 'archives électorales', 'archive.org/details/el0', 'archive.org/details/el1'),
    },
}


def citation_for(texts):
    """The citations owed for these source strings (urls, titles, ballot ids), each once, in a stable order."""
    blob = ' '.join(str(t) for t in texts).lower()
    return [c['text'] for c in CITATIONS.values() if any(m in blob for m in c['markers'])]


LICENCES = {
    'an': 'Licence Ouverte / Open Licence 2.0 (Assemblée nationale, data.assemblee-nationale.fr)',
    'senat': 'Licence Ouverte / Open Licence 2.0 (Sénat, data.senat.fr)',
    'ep': 'HowTheyVote.eu open data licence, attribution required (see howtheyvote.eu/about); source data European Parliament',
    'vie-publique-meta': 'Licence Ouverte 2.0 (DILA, métadonnées des discours publics)',
    'vie-publique-text': 'Texte publié par vie-publique.fr (DILA) ; droits des auteurs réservés, usage de citation',
}


def now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def rel(path: Path | str) -> str:
    p = Path(path).resolve()
    try:
        return str(p.relative_to(ROOT))
    except ValueError:
        return str(p)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for block in iter(lambda: f.read(1 << 20), b''):
            h.update(block)
    return h.hexdigest()


def norm_name(s: str | None) -> str:
    """'Marine LE PEN' / 'Dupont-Aignan' / 'Giscard d’Estaing' → 'marine le pen' / 'dupont aignan' / 'giscard d estaing'."""
    if not s:
        return ''
    s = unicodedata.normalize('NFKD', s).encode('ascii', 'ignore').decode().lower()
    return re.sub(r'[^a-z]+', ' ', s).strip()


def slug(s: str) -> str:
    return norm_name(s).replace(' ', '-')


def log(*args) -> None:
    print(*args, file=sys.stderr, flush=True)


class FetchError(Exception):
    pass


class CircuitOpen(FetchError):
    """A host answered 5xx too many times in a row: stop asking it for the rest of this run."""


def iri_to_uri(url: str) -> str:
    """https://fr.wikipedia.org/wiki/Édouard_Philippe → percent-encoded path / query (already-encoded parts are kept)."""
    parts = urllib.parse.urlsplit(url)
    host = parts.netloc.encode('idna').decode('ascii') if not parts.netloc.isascii() else parts.netloc
    return urllib.parse.urlunsplit((parts.scheme, host, urllib.parse.quote(parts.path, safe="/%:@!$&'()*+,;=~-._"),
                                    urllib.parse.quote(parts.query, safe="/%:@!$&'()*+,;=?~-._"), parts.fragment))


def ssl_context() -> ssl.SSLContext:
    """python.org builds on macOS ship without CA certificates until 'Install Certificates' is
    run; fall back to the system bundle rather than disabling verification."""
    ctx = ssl.create_default_context()
    if not ctx.cert_store_stats().get('x509_ca'):
        for bundle in (os.environ.get('SSL_CERT_FILE'), '/etc/ssl/cert.pem', '/etc/ssl/certs/ca-certificates.crt'):
            if bundle and os.path.exists(bundle):
                ctx.load_verify_locations(bundle)
                break
    return ctx


class RobotRules:
    """robots.txt per RFC 9309: the most specific (longest) matching rule wins and Allow wins a
    tie, so "Disallow: /" followed by "Allow: /bitstreams/" allows /bitstreams/…  (Python's
    urllib.robotparser applies the first match and would refuse). Supports * and $."""

    def __init__(self, text: str = '', agent: str = USER_AGENT):
        token = re.split(r'[/ ;(]', agent.lower().replace('mozilla/5.0 (compatible; ', ''))[0]
        groups: list[tuple[list[str], list[tuple[bool, str]]]] = []
        agents, rules, in_rules = [], [], False
        for line in text.splitlines():
            line = line.split('#', 1)[0].strip()
            if ':' not in line:
                continue
            key, _, value = line.partition(':')
            key, value = key.strip().lower(), value.strip()
            if key == 'user-agent':
                if in_rules:
                    groups.append((agents, rules))
                    agents, rules, in_rules = [], [], False
                agents.append(value.lower())
            elif key in ('allow', 'disallow'):
                in_rules = True
                if value or key == 'allow':
                    rules.append((key == 'allow', value))
        groups.append((agents, rules))
        specific = [r for a, r in groups if any(x != '*' and x in token for x in a)]
        generic = [r for a, r in groups if '*' in a]
        self.rules = [r for g in (specific or generic) for r in g]

    @staticmethod
    def _matches(pattern: str, path: str) -> bool:
        if not pattern:
            return False
        rx = re.escape(pattern).replace(r'\*', '.*')
        if rx.endswith(r'\$'):
            rx = rx[:-2] + '$'
        return re.match(rx, path) is not None

    def can_fetch(self, url: str) -> bool:
        parts = urllib.parse.urlsplit(url)
        path = parts.path + ('?' + parts.query if parts.query else '')
        best_len, allowed = -1, True
        for allow, pattern in self.rules:
            if self._matches(pattern, path):
                n = len(pattern)
                if n > best_len or (n == best_len and allow):
                    best_len, allowed = n, allow
        return allowed


class Fetcher:
    """Rate-limited per host, honours robots.txt, caches every response on disk with a sidecar
    `<file>.meta.json` (url, date, sha256). A cached file is reused unless refresh=True, which
    makes runs resumable: an interrupted run restarts where it stopped."""

    BREAKER = 8   # consecutive 5xx answers from one host before it is left alone for the rest of the run

    def __init__(self, min_interval: float = 1.0, per_host: dict[str, float] | None = None, timeout: int = 120,
                 revalidate: bool = False, max_age_days: float = 7.0):
        # revalidate: a cached big download (stream=True) is checked with a HEAD request and fetched again only when the
        # server says it changed (ETag, Last-Modified, then Content-Length); with nothing to compare it is renewed after max_age_days.
        self.revalidate = revalidate
        self.max_age_days = max_age_days
        self.downloaded: list[dict] = []   # what this run really fetched (url, bytes, sha256)
        self.streak: dict[str, int] = {}
        self.min_interval = min_interval
        self.per_host = per_host or {}
        self.timeout = timeout
        self.last: dict[str, float] = {}
        self.robots: dict[str, urllib.robotparser.RobotFileParser | None] = {}
        self.jar = http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(self.jar),
                                                   urllib.request.HTTPSHandler(context=ssl_context()))

    # -- low level -----------------------------------------------------------------------------
    def _wait(self, host: str) -> None:
        gap = self.per_host.get(host, self.min_interval)
        elapsed = time.monotonic() - self.last.get(host, 0)
        if elapsed < gap:
            time.sleep(gap - elapsed)
        self.last[host] = time.monotonic()

    def _open(self, url: str, headers: dict | None = None, method: str = 'GET'):
        url = iri_to_uri(url)
        host = urllib.parse.urlsplit(url).netloc
        req = urllib.request.Request(url, headers={'User-Agent': USER_AGENT, 'Accept-Encoding': 'gzip', **(headers or {})}, method=method)
        for attempt in range(4):
            if self.streak.get(host, 0) >= self.BREAKER:
                raise CircuitOpen(f'circuit open for {host} after {self.BREAKER} consecutive 5xx answers: {url}')
            self._wait(host)
            try:
                resp = self.opener.open(req, timeout=self.timeout)
                self.streak[host] = 0
                return resp
            except urllib.error.HTTPError as e:
                self.streak[host] = self.streak.get(host, 0) + 1 if e.code >= 500 else 0
                if self.streak[host] >= self.BREAKER:
                    raise CircuitOpen(f'circuit open for {host} after {self.BREAKER} consecutive 5xx answers: {url}') from e
                if e.code in (429, 500, 502, 503, 504) and attempt < 3:
                    delay = int(e.headers.get('Retry-After') or 0) or 5 * 2 ** attempt
                    log(f'  {e.code} on {url}, retry in {delay}s')
                    time.sleep(delay)
                    continue
                raise FetchError(f'HTTP {e.code} {url}') from e
            except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
                if attempt < 3:
                    time.sleep(5 * 2 ** attempt)
                    continue
                raise FetchError(f'{type(e).__name__} {url}: {e}') from e
        raise FetchError(f'gave up on {url}')

    @staticmethod
    def _body(resp) -> bytes:
        data = resp.read()
        enc = (resp.headers.get('Content-Encoding') or '').lower()
        ctype = (resp.headers.get('Content-Type') or '').lower()
        # vie-publique.fr gzips text even when not asked to; real .gz files are not text/*.
        if enc == 'gzip' or (data[:2] == b'\x1f\x8b' and ctype.startswith('text/')):
            data = gzip.decompress(data)
        elif enc == 'deflate':
            data = zlib.decompress(data)
        return data

    _CHALLENGE = re.compile(rb"window\.location\.href='(/redirect_[A-Z0-9=]+/[^']*)'")

    def get_bytes(self, url: str) -> tuple[bytes, dict]:
        """GET with the cookie jar. Some sites (vie-publique.fr) answer a first request with a
        one-line page that sets a cookie and redirects to the same path; we follow it once,
        exactly as a browser would, and keep the cookie for later requests."""
        resp = self._open(url)
        data = self._body(resp)
        m = self._CHALLENGE.search(data[:2000]) if len(data) < 2000 else None
        if m:
            parts = urllib.parse.urlsplit(url)
            resp = self._open(f'{parts.scheme}://{parts.netloc}{m.group(1).decode()}')
            data = self._body(resp)
            if self._CHALLENGE.search(data[:2000]):
                raise FetchError(f'cookie redirect loop on {url}')
        return data, {'final_url': resp.geturl(), 'content_type': resp.headers.get('Content-Type'),
                      'last_modified': resp.headers.get('Last-Modified'), 'etag': resp.headers.get('ETag')}

    def _get_with(self, url: str, headers: dict) -> tuple[bytes, dict]:
        resp = self._open(url, headers=headers)
        return self._body(resp), {'final_url': resp.geturl(), 'content_type': resp.headers.get('Content-Type'),
                                  'last_modified': resp.headers.get('Last-Modified'), 'etag': resp.headers.get('ETag')}

    # -- robots ---------------------------------------------------------------------------------
    def allowed(self, url: str) -> bool:
        parts = urllib.parse.urlsplit(url)
        base = f'{parts.scheme}://{parts.netloc}'
        if base not in self.robots:
            try:
                data, _ = self.get_bytes(base + '/robots.txt')
                text = data.decode('utf-8', 'replace')
                # A missing robots.txt served as an HTML error page means no rules.
                self.robots[base] = RobotRules('' if '<html' in text[:500].lower() else text)
            except FetchError:
                self.robots[base] = RobotRules('')
        return self.robots[base].can_fetch(url)

    def unchanged(self, url: str, meta: dict) -> bool:
        """Is the cached copy (described by its sidecar) still what the server has? Any doubt or failure: keep the copy."""
        try:
            resp = self._open(url, headers={'Accept-Encoding': 'identity'}, method='HEAD')
        except FetchError:
            return True
        h = resp.headers
        if meta.get('etag') and h.get('ETag'):
            return meta['etag'] == h['ETag']
        if meta.get('last_modified') and h.get('Last-Modified'):
            return meta['last_modified'] == h['Last-Modified']
        if h.get('Content-Length') and meta.get('bytes'):
            return int(h['Content-Length']) == int(meta['bytes'])
        fetched = meta.get('fetched_at')
        if not fetched:
            return False
        age = (datetime.now(timezone.utc) - datetime.fromisoformat(fetched)).total_seconds() / 86400
        return age < self.max_age_days

    # -- cached download ------------------------------------------------------------------------
    def download(self, url: str, dest: Path, refresh: bool = False, robots: bool = True,
                 stream: bool = False, headers: dict | None = None, canonical_url: str | None = None) -> dict:
        """Download url to dest (atomic). Returns the sidecar metadata. Reuses the cache.
        `headers`: extra request headers; `canonical_url`: the public address recorded as provenance when the bytes
        came from another one (e.g. a release asset fetched through the GitHub API)."""
        dest = Path(dest)
        meta_path = dest.with_name(dest.name + '.meta.json')
        if dest.exists() and meta_path.exists() and not refresh:
            meta = json.loads(meta_path.read_text())
            if not (self.revalidate and stream and not self.unchanged(url, meta)):
                meta['cached'] = True
                return meta
            log(f'  changed upstream, fetching again: {url}')
        if robots and not self.allowed(url):
            raise FetchError(f'robots.txt disallows {url}')
        dest.parent.mkdir(parents=True, exist_ok=True)
        part = dest.with_name(dest.name + '.part')
        if stream:
            resp = self._open(url, headers={'Accept-Encoding': 'identity', **(headers or {})})
            info = {'final_url': resp.geturl(), 'content_type': resp.headers.get('Content-Type'),
                    'last_modified': resp.headers.get('Last-Modified'), 'etag': resp.headers.get('ETag')}
            expected = int(resp.headers.get('Content-Length') or 0)
            got = 0
            # Some servers (data.assemblee-nationale.fr) cut long downloads short without an error:
            # resume with Range requests until the announced length is reached.
            with open(part, 'wb') as f:
                for attempt in range(40):
                    try:
                        while block := resp.read(1 << 20):
                            f.write(block)
                            got += len(block)
                    except (OSError, http.client.HTTPException):
                        pass
                    if not expected or got >= expected:
                        break
                    log(f'  short read on {url}: {got} of {expected} bytes, resuming')
                    resp = self._open(url, headers={'Accept-Encoding': 'identity', 'Range': f'bytes={got}-', **(headers or {})})
                    if resp.status != 206:
                        raise FetchError(f'server ignored Range on {url}')
            if expected and got != expected:
                part.unlink()
                raise FetchError(f'truncated download of {url}: {got} of {expected} bytes')
        else:
            data, info = self.get_bytes(url) if not headers else self._get_with(url, headers)
            part.write_bytes(data)
        os.replace(part, dest)
        meta = {'url': canonical_url or url, **info, 'fetched_at': now(), 'sha256': sha256_file(dest),
                'bytes': dest.stat().st_size, 'path': rel(dest)}
        meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=1))
        meta['cached'] = False
        self.downloaded.append({'url': meta['url'], 'bytes': meta['bytes'], 'sha256': meta['sha256']})
        return meta


def record_raw_file(conn, meta: dict, licence: str | None) -> int:
    conn.execute(
        """INSERT INTO raw_files (url, path, fetched_at, sha256, bytes, content_type, licence)
           VALUES (?,?,?,?,?,?,?)
           ON CONFLICT(url) DO UPDATE SET path=excluded.path, fetched_at=excluded.fetched_at,
             sha256=excluded.sha256, bytes=excluded.bytes, content_type=excluded.content_type,
             licence=excluded.licence""",
        (meta['url'], meta['path'], meta['fetched_at'], meta['sha256'], meta['bytes'], meta.get('content_type'), licence))
    return conn.execute('SELECT id FROM raw_files WHERE url = ?', (meta['url'],)).fetchone()[0]
