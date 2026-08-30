"""The SQL console: one page, behind the login the Dagster UI already uses.

Why this rather than Metabase or Superset. Both are good and both want a database of
their own, a second identity integration and a container an order of magnitude
larger than this project. The thing that was missing was a text box that runs SELECT
against the lake and shows the rows. This is that, mounted under `/sql/` on the host
that already exists, behind the oauth2-proxy that already checks the `datalake-user`
role in Keycloak. No new hostname, no new certificate, no second account to manage.

Authentication is deliberately not implemented here. oauth2-proxy terminates the
login and forwards the user's identity in a header; this app refuses a request that
arrives without one, so a misconfiguration that exposed it directly fails closed
instead of serving the warehouse to the internet.
"""

from __future__ import annotations

import csv
import io
import json
import os
from datetime import UTC, date, datetime
from decimal import Decimal

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import (
    HTMLResponse,
    JSONResponse,
    PlainTextResponse,
    RedirectResponse,
    Response,
)
from starlette.routing import Route

from homelab_datalake.sql import engine
from homelab_datalake.sql.page import PAGE

# The headers oauth2-proxy sets once a session is established. Two of them, because
# they come from two different settings -- `pass-user-headers` writes the first and
# `set-xauthrequest` the second -- and a deployment that has only one of those on
# would otherwise look exactly like a deployment with no login at all. Either is
# proof of a session; both are set in docker-compose.yml.
IDENTITY_HEADERS = tuple(
    h.strip() for h in os.environ.get(
        "SQL_CONSOLE_IDENTITY_HEADERS", "x-forwarded-user,x-auth-request-user"
    ).split(",") if h.strip()
)

# Off only for local development, where there is no proxy in front. Never set in the
# compose file, so forgetting it cannot be what exposes the console.
REQUIRE_IDENTITY = os.environ.get("SQL_CONSOLE_REQUIRE_IDENTITY", "true").lower() != "false"


def viewer(request: Request) -> str | None:
    for header in IDENTITY_HEADERS:
        value = request.headers.get(header)
        if value:
            return value
    return None


def _unauthenticated() -> JSONResponse:
    return JSONResponse(
        {"error": "no authenticated user. This console is reached through "
                  "oauth2-proxy, which sets the identity header."},
        status_code=401,
    )


def encode(value):
    """JSON for the values DuckDB returns.

    Dates and timestamps become ISO strings and decimals become strings rather than
    floats. A spend of 0.000123 rendered as a float in JavaScript is a different
    number from the one in the warehouse, and this console exists to show what is in
    the warehouse.
    """
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        # Fixed point, not str(): str() gives a small decimal back in scientific
        # notation, so a spend of 0.000000123 reaches the page as "1.23E-7".
        return format(value, "f")
    if isinstance(value, (bytes, bytearray)):
        return value.hex()
    return value


async def index(request: Request) -> Response:
    if REQUIRE_IDENTITY and not viewer(request):
        return PlainTextResponse("not authenticated", status_code=401)
    # The page fetches `api/schema` relative to its own URL, so it has to be served
    # from `/sql/`. Reached at `/sql`, the same relative URL would resolve to
    # `/api/schema`, which oauth2-proxy routes to Dagster -- a blank sidebar and no
    # error anywhere. Redirecting is what makes the address a person types work.
    if request.url.path == "/sql":
        return RedirectResponse("/sql/", status_code=308)
    return HTMLResponse(PAGE)


async def schema(request: Request) -> Response:
    if REQUIRE_IDENTITY and not viewer(request):
        return _unauthenticated()
    con = engine.connect()
    try:
        return JSONResponse({"layers": engine.catalogue(con)})
    finally:
        con.close()


async def query(request: Request) -> Response:
    if REQUIRE_IDENTITY and not viewer(request):
        return _unauthenticated()
    try:
        body = json.loads(await request.body() or b"{}")
    except json.JSONDecodeError:
        return JSONResponse({"error": "malformed request"}, status_code=400)

    sql = str(body.get("sql", ""))
    con = engine.connect()
    try:
        result = engine.run(con, sql)
    except engine.QueryRefused as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    finally:
        con.close()

    return JSONResponse({
        "columns": result.columns,
        "rows": [[encode(v) for v in row] for row in result.rows],
        "truncated": result.truncated,
        "elapsed_ms": result.elapsed_ms,
    })


async def download(request: Request) -> Response:
    """The same query, as CSV.

    A browser cannot save a table it only has in the DOM, and the point of a console
    is often to get the rows somewhere else. The row cap still applies -- this is a
    look at the data, not an export pipeline; the MCP layer builds workbooks and the
    lake itself is Parquet for anything larger.
    """
    if REQUIRE_IDENTITY and not viewer(request):
        return _unauthenticated()
    form = await request.form()
    sql = str(form.get("sql", ""))
    con = engine.connect()
    try:
        result = engine.run(con, sql)
    except engine.QueryRefused as exc:
        return PlainTextResponse(str(exc), status_code=400)
    finally:
        con.close()

    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(result.columns)
    writer.writerows([[encode(v) for v in row] for row in result.rows])
    stamp = datetime.now(tz=UTC).strftime("%Y%m%d-%H%M%S")
    return Response(
        buffer.getvalue(),
        media_type="text/csv",
        headers={"content-disposition": f'attachment; filename="query-{stamp}.csv"'},
    )


async def healthz(_: Request) -> Response:
    return PlainTextResponse("ok")


# Mounted under /sql because oauth2-proxy routes to it by path prefix and forwards
# the path unchanged. The trailing-slash pair is there so both /sql and /sql/ work,
# which is what a person types.
app = Starlette(routes=[
    Route("/sql", index),
    Route("/sql/", index),
    Route("/sql/api/schema", schema),
    Route("/sql/api/query", query, methods=["POST"]),
    Route("/sql/api/download", download, methods=["POST"]),
    Route("/healthz", healthz),
])


def main() -> None:
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("SQL_CONSOLE_PORT", "8080")))  # noqa: S104


if __name__ == "__main__":
    main()
