"""One User-Agent, in one place, because getting it wrong has cost time three times.

Several services in this homelab sit behind Cloudflare, which rejects urllib's default
agent (`Python-urllib/3.x`) with a **403 before the request reaches the origin**. That
is a nasty failure to diagnose, because 403 is also what an authorisation problem looks
like -- and it has now appeared three times in this project wearing a different costume
each time:

* the camera export, where it read as a broken Keycloak token;
* Jellyfin, pre-emptively, once the pattern was recognised;
* the JWKS fetch in the MCP server's token verifier, where it read as *every valid
  token being rejected* -- the token was fine and the signing keys could not be
  downloaded.

The third one is the reason this constant exists rather than a fourth copy of the
string. Anything in this codebase that makes an outbound HTTP request identifies
itself with this, which also makes the traffic recognisable in the very access logs
the warehouse ingests.
"""

USER_AGENT = "homelab-datalake/1.0 (+https://data.example.test)"

#: Ready to splat into a urllib/requests call that needs nothing else.
DEFAULT_HEADERS = {"User-Agent": USER_AGENT}
