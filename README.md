# proxy-dash

Live monitor for 9router (`:20129`), served on **`:20130`** by `proxy-dash.service`.

Single-file Python dashboard: it tails the 9router journal, parses request lines,
tracks proxy-pool health and does IP geo lookup, then renders one auto-refreshing page.

Archived here on 2026-10-23 before the page is repurposed into a WARP exit-IP /
rotation log.

## Files

- `proxy-dash.py` — the whole app (HTTP server + parser + HTML renderer, stdlib only)
- `proxy-dash.service` — systemd unit

## What it does

- Tails `journalctl -u 9router-20129` into `logs/9router-20129.log`
- Parses each request line: timestamp, model, provider, status, latency, proxy
- Builds request history and computes per-minute rates
- Reads the `proxyPools` table from the 9router SQLite DB for pool names
- Geo-locates exit IPs (cached to `geo.json`)
- Serves a dark auto-refresh dashboard on port 20130

## Run

```
sudo cp proxy-dash.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now proxy-dash
```

Requires the unit's `WorkingDirectory` (`~/projects/proxy`) to hold `logs/`.

## Notes

- Read-only with respect to 9router: it reads the journal and the SQLite DB, never writes.
- Binds `0.0.0.0:20130` — no auth. Keep it behind the tailnet / LAN, do not expose it.
- Known upstream quirk it works around: 9router sometimes logs `24:xx` instead of `00:xx`
  around midnight; the parser normalises this.
