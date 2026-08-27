# Nginx Proxy Manager config

`server_proxy.conf` is deployed to **fraserv only**, at
`/home/docker/npm/data/nginx/custom/server_proxy.conf`. NPM includes that path in every
proxy-host `server` block, so one file covers all 44 hosts.

## Deploy

```bash
scp infra/npm/server_proxy.conf ssh.fraserv.fr:/tmp/
ssh ssh.fraserv.fr 'sudo cp /tmp/server_proxy.conf /home/docker/npm/data/nginx/custom/ \
  && docker exec npm-npm-1 nginx -t && docker exec npm-npm-1 nginx -s reload'
```

Always let `nginx -t` gate the reload. A bad file here takes every service offline.

## Not deployed to amblevilleserv, deliberately

That host receives traffic directly: 8 of 388,558 logged client addresses were Cloudflare.
Its `$remote_addr` is already the real visitor. Trusting `CF-Connecting-IP` there would add a
spoofable header for no benefit, so it stays off. Re-check with
`scripts/check-cloudflare-ranges.sh` if that host is ever put behind the proxy.
