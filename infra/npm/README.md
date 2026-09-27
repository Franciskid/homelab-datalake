# Nginx Proxy Manager config

Moved to its own private repo, `github.com/Franciskid/homelab-npm`, which is now the
only copy. That covers the data.fraserv.fr vhost (`fraserv/custom/http.conf`), the
Cloudflare real-IP include, the timed log format and NPM's compose files on both
hosts. Edit and deploy from there.

What stays here is the datalake's half of the connection: `docker-compose.yml` puts
`oauth2-proxy` and `datalake-mcp` on NPM's `npm_default` network, and
`scripts/deploy.sh` fails if nginx can't resolve either one. The network used to be
attached from NPM's side by hand, and that attachment was lost when NPM was recreated
on 2026-09-23. data.fraserv.fr answered 502 until 27 Sep.
