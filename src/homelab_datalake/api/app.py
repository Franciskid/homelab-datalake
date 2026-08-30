"""The routes, and the bearer token in front of them.

Starlette rather than FastAPI: there are a handful of endpoints, the request bodies
are validated by hand because the validation *is* the security control, and adding a
web framework to a data repository to save writing four route functions is a poor
trade.

Three things about the shape of this are deliberate.

**Authentication is the same Keycloak realm as everything else.** The verifier is the
one the MCP server uses, unchanged. A second way to authenticate against the same
data is a second thing to revoke, and the one nobody remembers is the one that stays
valid.

**The token names the caller, a header names the person.** This API is reached by the
ai-platform gateway holding a service-account token, on behalf of whichever human is
looking at the workbench. The token cannot say who that is, so the gateway passes it
in `X-Datalake-User` and the audit log records both. The header is trusted exactly as
far as the token is: anyone able to set it already holds a service-account token, and
the audit row keeps the client id next to the claimed user so a disagreement is
visible afterwards.

**The server refuses to start without an issuer.** Same posture as the MCP server: an
endpoint over the lake that answers anyone is worse than one that is down, because
nothing about it looks wrong.
"""

from __future__ import annotations

import logging
import os

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from homelab_datalake.api import query as q
from homelab_datalake.api.audit import AuditLog, Entry
from homelab_datalake.api.catalogue import CatalogueCache
from homelab_datalake.mcp.auth import KeycloakTokenVerifier, verifier_from_env

logger = logging.getLogger(__name__)

# Set by the ai-platform gateway to the identity of the person whose browser caused
# the request. Not authentication -- see the module docstring.
USER_HEADER = "X-Datalake-User"


class Caller:
    """Who made this request: the authenticated client, and the person it names."""

    __slots__ = ("client_id", "user")

    def __init__(self, client_id: str, user: str) -> None:
        self.client_id = client_id
        self.user = user

    def __repr__(self) -> str:  # pragma: no cover - diagnostics only
        return f"Caller(client_id={self.client_id!r}, user={self.user!r})"


def _unauthorised(detail: str) -> JSONResponse:
    # `WWW-Authenticate` so a client that can obtain a token knows it should, rather
    # than reporting "the datalake is broken" to somebody who then goes looking at
    # MinIO.
    return JSONResponse(
        {"error": "unauthorised", "detail": detail},
        status_code=401,
        headers={"WWW-Authenticate": 'Bearer realm="homelab-datalake"'},
    )


async def authenticate(request: Request, verifier: KeycloakTokenVerifier | None) -> Caller | Response:
    if verifier is None:
        # Only reachable when the process was started deliberately without an issuer,
        # which main() refuses to do. Tests construct it this way on purpose.
        return Caller(client_id="anonymous", user=request.headers.get(USER_HEADER, ""))

    header = request.headers.get("Authorization", "")
    scheme, _, token = header.partition(" ")
    if scheme.lower() != "bearer" or not token:
        return _unauthorised("expected an Authorization: Bearer <token> header")

    access = await verifier.verify_token(token)
    if access is None:
        return _unauthorised("the token was rejected, or lacks the datalake:query role")

    return Caller(client_id=access.client_id, user=request.headers.get(USER_HEADER, ""))


def _refused(detail: str) -> JSONResponse:
    # 400, not 500. Every refusal here is a statement of what the caller may ask for,
    # and the workbench shows the message verbatim -- so it has to read as an
    # instruction to the person typing rather than as a fault in the service.
    return JSONResponse({"error": "refused", "detail": detail}, status_code=400)


def create_app(
    catalogue: CatalogueCache | None = None,
    sessions: q.SessionCache | None = None,
    audit: AuditLog | None = None,
    verifier: KeycloakTokenVerifier | None = None,
) -> Starlette:
    """Build the application.

    Every collaborator is an argument rather than a module global so the tests can run
    the real routes against a lake in a temporary directory and no Keycloak.
    """
    cache = catalogue or CatalogueCache()
    pool = sessions or q.SessionCache()
    log = audit or AuditLog()

    async def health(request: Request) -> Response:
        """Liveness only, and deliberately unauthenticated.

        It reports whether the process is up, not whether the lake is reachable --
        building the catalogue to answer a health check would make a slow MinIO
        look like a dead API and restart something that was working.
        """
        return JSONResponse({"status": "ok"})

    async def get_catalogue(request: Request) -> Response:
        caller = await authenticate(request, verifier)
        if isinstance(caller, Response):
            return caller
        return JSONResponse(cache.get().as_dict())

    async def refresh_catalogue(request: Request) -> Response:
        caller = await authenticate(request, verifier)
        if isinstance(caller, Response):
            return caller
        logger.info("catalogue refresh requested by %s", caller)
        return JSONResponse(cache.refresh().as_dict())

    async def run_query(request: Request) -> Response:
        caller = await authenticate(request, verifier)
        if isinstance(caller, Response):
            return caller

        try:
            body = await request.json()
        except ValueError:
            return _refused("the request body must be JSON.")
        if not isinstance(body, dict):
            return _refused("the request body must be a JSON object.")

        sql = body.get("sql")
        if not isinstance(sql, str):
            return _refused("`sql` is required and must be a string.")

        # Read-only unless the caller explicitly says otherwise, and only for the
        # literal `false`. A missing field, a null, or the string "false" all leave
        # the switch on: the direction a mistake falls in is the whole point of a
        # default.
        read_only = body.get("read_only", True) is not False
        max_rows = body.get("max_rows", q.MAX_ROWS)
        if not isinstance(max_rows, int) or isinstance(max_rows, bool) or max_rows < 1:
            return _refused("`max_rows` must be a positive integer.")
        max_rows = min(max_rows, q.MAX_ROWS)

        session = pool.get() if read_only else pool.writable()
        try:
            result = q.run(session, sql, read_only=read_only, max_rows=max_rows)
        except q.QueryRefused as error:
            log.write(Entry(
                client_id=caller.client_id, user=caller.user, sql=sql,
                statement_type=error.statement_type, read_only=read_only,
                outcome="refused", detail=str(error),
            ))
            return _refused(str(error))
        finally:
            # A writable session is built for one statement and closed after it, so
            # a temporary table or a changed setting cannot outlive the statement
            # that made it. The read-only session is shared and stays open.
            if not read_only:
                session.close()

        log.write(Entry(
            client_id=caller.client_id, user=caller.user, sql=result.sql,
            statement_type=result.statement_type, read_only=read_only,
            outcome="ok", rows=result.row_count, truncated=result.truncated,
            elapsed_ms=result.elapsed_ms,
        ))
        return JSONResponse(result.as_dict())

    return Starlette(
        routes=[
            Route("/health", health, methods=["GET"]),
            Route("/catalogue", get_catalogue, methods=["GET"]),
            Route("/catalogue/refresh", refresh_catalogue, methods=["POST"]),
            Route("/query", run_query, methods=["POST"]),
        ]
    )


def main() -> None:
    """Run the API.

    Bound to the compose network inside a container: nothing publishes this port, and
    the ai-platform gateway is the only route in. That is not what makes it safe --
    the bearer token is -- but it means an unauthenticated request has to originate
    inside the network before it can even be refused.
    """
    import uvicorn

    verifier = verifier_from_env()
    if verifier is None and os.environ.get("DATALAKE_API_ALLOW_ANONYMOUS") != "yes":
        raise SystemExit(
            "MCP_OIDC_ISSUER is not set. Refusing to serve the lake over the network "
            "without authentication. Set it to the Keycloak realm URL, or set "
            "DATALAKE_API_ALLOW_ANONYMOUS=yes if this is a local sandbox with no "
            "real data in it."
        )

    uvicorn.run(
        create_app(verifier=verifier),
        host=os.environ.get("DATALAKE_API_HOST", "0.0.0.0"),  # noqa: S104 - see above
        port=int(os.environ.get("DATALAKE_API_PORT", "3444")),
        log_level=os.environ.get("DATALAKE_API_LOG_LEVEL", "info"),
    )


if __name__ == "__main__":
    main()
