# Nginx Proxy Manager config

`server_proxy.conf` is deployed to **primary only**, at
`/srv/npm/data/nginx/custom/server_proxy.conf`. NPM includes that path in every
proxy-host `server` block, so one file covers all 44 hosts.

## Deploy

```bash
scp infra/npm/server_proxy.conf ssh.example.test:/tmp/
ssh ssh.example.test 'sudo cp /tmp/server_proxy.conf /srv/npm/data/nginx/custom/ \
  && docker exec npm-npm-1 nginx -t && docker exec npm-npm-1 nginx -s reload'
```

Always let `nginx -t` gate the reload. A bad file here takes every service offline.

## Not deployed to secondary, deliberately

That host receives traffic directly: 8 of [count withheld] logged client addresses were Cloudflare.
Its `$remote_addr` is already the real visitor. Trusting `CF-Connecting-IP` there would add a
spoofable header for no benefit, so it stays off. Re-check with
`scripts/check-cloudflare-ranges.sh` if that host is ever put behind the proxy.

> Public history note: private identifiers, measurements, and operational details were redacted. Code examples use placeholder infrastructure.
