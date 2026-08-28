"""Geo and ASN enrichment, derived at the boundary and deliberately optional.

Runs against local MaxMind GeoLite2 database files, not a web API. Two reasons:
no third party gets told which addresses visit the homelab, and there is no
per-request cost or rate limit to design around.

Optional on purpose. Without the database files -- or without the `geoip2`
package -- every column comes back null and the pipeline still runs. That keeps
CI free of a licence key, which is what lets the repo be cloned and built by
someone who is not you.

Enrichment happens *before* homelab_datalake.privacy discards the address. That
ordering is the entire trick: the row keeps country, city and network operator,
and keeps nothing that identifies a household.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass

logger = logging.getLogger(__name__)

try:  # pragma: no cover - exercised by absence, not by tests
    import geoip2.database
    import geoip2.errors

    _GEOIP2_AVAILABLE = True
    # AddressNotFoundError covers "MaxMind has no record"; ValueError covers a
    # malformed address reaching this far. Anything else is a real fault and should
    # surface rather than be swallowed.
    _LOOKUP_MISS: tuple[type[BaseException], ...] = (
        geoip2.errors.AddressNotFoundError,
        ValueError,
    )
except ImportError:  # pragma: no cover
    _GEOIP2_AVAILABLE = False
    _LOOKUP_MISS = (ValueError,)


@dataclass(frozen=True)
class GeoInfo:
    country_code: str | None = None
    country_name: str | None = None
    city: str | None = None
    asn: int | None = None
    as_org: str | None = None


EMPTY = GeoInfo()


class GeoEnricher:
    """Looks up addresses in local MaxMind databases, or does nothing at all.

    `enabled` is False when the files or the library are missing. Callers do not
    branch on it -- `lookup` just returns empty values -- but assets log it once so
    a run whose geo columns are all null says why in its own metadata.
    """

    def __init__(self, city_db: str | None = None, asn_db: str | None = None) -> None:
        self._city_reader = self._open(city_db, "city")
        self._asn_reader = self._open(asn_db, "ASN")

    @classmethod
    def from_env(cls) -> GeoEnricher:
        return cls(os.environ.get("GEOIP_CITY_DB"), os.environ.get("GEOIP_ASN_DB"))

    @staticmethod
    def _open(path: str | None, label: str):
        if not path:
            return None
        if not _GEOIP2_AVAILABLE:
            logger.warning("geoip2 not installed; %s enrichment disabled", label)
            return None
        if not os.path.exists(path):
            logger.warning("MaxMind %s database not found at %s; enrichment disabled", label, path)
            return None
        try:
            return geoip2.database.Reader(path)
        except Exception:  # a corrupt database must degrade, never fail the run
            logger.exception("could not open MaxMind %s database at %s", label, path)
            return None

    @property
    def enabled(self) -> bool:
        return self._city_reader is not None or self._asn_reader is not None

    def lookup(self, address: str | None) -> GeoInfo:
        if not address or not self.enabled:
            return EMPTY
        country_code = country_name = city = as_org = None
        asn = None
        # A miss is the normal case, not an error: private ranges, reserved blocks and
        # addresses MaxMind has no record of all land here. Debug-level, because at
        # [volume withheld] of logs a day a warning per miss would drown the run log.
        if self._city_reader is not None:
            try:
                r = self._city_reader.city(address)
                country_code = r.country.iso_code
                country_name = r.country.name
                city = r.city.name
            except _LOOKUP_MISS:
                logger.debug("no city record for a client address")
        if self._asn_reader is not None:
            try:
                r = self._asn_reader.asn(address)
                asn = r.autonomous_system_number
                as_org = r.autonomous_system_organization
            except _LOOKUP_MISS:
                logger.debug("no ASN record for a client address")
        return GeoInfo(country_code, country_name, city, asn, as_org)

    def close(self) -> None:
        for reader in (self._city_reader, self._asn_reader):
            if reader is not None:
                try:
                    reader.close()
                except OSError:
                    logger.debug("MaxMind reader did not close cleanly")
