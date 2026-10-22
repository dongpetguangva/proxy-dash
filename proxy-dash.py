#!/usr/bin/env python3
"""proxy-dash — WARP exit-IP ledger untuk 9router (:20129), disajikan di :20130.

Rombakan 2026-10-23: dulu monitor proxy pool, sekarang pencatat identitas
egress WARP + hasil request OpenCode.

Tiap POLL_SECS detik mencatat satu sampel:
  - IP keluar WARP (IPv4)      via SOCKS5 127.0.0.1:40000
  - IP keluar shell            (IP rumah / Proton, tergantung tunnel)
  - registrasi WARP aktif + umur file reg
  - apakah WARP tersambung
  - satu request nyata ke OpenCode lewat 9router -> kode + jenis error

Data JSON Lines append-only di logs/warp-ip-samples.jsonl — tahan restart,
bisa di-tail manual, dan bisa dibaca ulang saat start.

Stdlib only. Master: ~/projects/proxy/proxy-dash.py
"""
import collections
import json
import os
import re
import sqlite3
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer

PORT = 20130
HERE = os.path.dirname(os.path.abspath(__file__))
SAMPLES = os.path.join(HERE, "logs", "warp-ip-samples.jsonl")
GEO_CACHE = os.path.join(HERE, "geo-cache.json")
DB = "/home/vidh-compaq/apps/9router-20129-data/db/data.sqlite"

POLL_SECS = 30 * 60          # jarak antar sampel
KEEP_DAYS = 30               # sampel lebih tua dibuang saat load
WARP_SOCKS = "127.0.0.1:40000"
PROBE_MODEL = "oc/deepseek-v4.1-flash-free"
PROBE_TIMEOUT = 25
MAX_SAMPLES = 4000

_state = {
    "samples": [],
    "geo": {},
    "geo_loaded": False,
    "next_at": 0.0,
    "last_err": "",
}
_lock = threading.Lock()


# ---------------------------------------------------------------- geo

def load_geo():
    if _state["geo_loaded"]:
        return
    _state["geo_loaded"] = True
    try:
        with open(GEO_CACHE) as f:
            _state["geo"].update(json.load(f))
    except Exception:
        pass


def save_geo():
    try:
        with open(GEO_CACHE, "w") as f:
            json.dump(_state["geo"], f)
    except Exception:
        pass


def geo_lookup(ips):
    """Lookup batch, cache 7 hari untuk entri gagal."""
    load_geo()
    ips = [ip for ip in ips if ip]
    if not ips:
        return
    missing = [ip for ip in dict.fromkeys(ips) if ip not in _state["geo"]]
    try:
        if time.time() - os.path.getmtime(GEO_CACHE) > 7 * 86400:
            missing += [ip for ip in ips if _state["geo"].get(ip) == "?? · ?"]
    except OSError:
        pass
    missing = list(dict.fromkeys(missing))
    for i in range(0, len(missing), 100):
        batch = missing[i:i + 100]
        try:
            req = urllib.request.Request(
                "http://ip-api.com/batch?fields=query,country,countryCode,status",
                data=json.dumps([{"query": ip} for ip in batch]).encode(),
                headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=15) as r:
                for e in json.loads(r.read().decode()):
                    if e.get("status") == "success":
                        _state["geo"][e["query"]] = "%s · %s" % (
                            e.get("countryCode", "??"), e.get("country", "?"))
                    else:
                        _state["geo"][e["query"]] = "?? · ?"
        except Exception:
            for ip in batch:
                _state["geo"].setdefault(ip, "?? · ?")
        time.sleep(1.2)
    save_geo()


# ---------------------------------------------------------------- probes

def sh(cmd, timeout=12):
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True,
                           text=True, timeout=timeout)
        return r.stdout.strip()
    except Exception:
        return ""


IPV4 = re.compile(r"^\d+\.\d+\.\d+\.\d+$")


def warp_ip():
    out = sh("curl -s -4 -m 10 --socks5-hostname %s https://api.ipify.org"
             % WARP_SOCKS)
    return out if IPV4.match(out or "") else ""


def warp_connected():
    return "Connected" in sh("warp-cli --accept-tos status 2>&1", timeout=8)


def warp_registration():
    """ID registrasi aktif (8 char) + umur material registrasi."""
    out = sh("warp-cli --accept-tos registration show 2>&1", timeout=8)
    m = re.search(r"^ID:\s*([0-9a-f-]{36})", out, re.M)
    reg = m.group(1)[:8] if m else ""
    age = None
    for p in ("/var/lib/cloudflare-warp/reg.json",
              "/var/lib/cloudflare-warp/state.json"):
        try:
            age = int(time.time() - os.path.getmtime(p))
            break
        except Exception:
            continue
    return reg, age


def shell_ip():
    out = sh("curl -s -m 8 https://api.ipify.org", timeout=12)
    return out if IPV4.match(out or "") else ""


def api_key():
    try:
        c = sqlite3.connect("file:%s?mode=ro" % DB, uri=True)
        row = c.execute("SELECT key FROM apiKeys LIMIT 1").fetchone()
        c.close()
        return row[0] if row else ""
    except Exception:
        return ""


def probe(model=PROBE_MODEL):
    """Satu request nyata lewat 9router. -> (kode, jenis_error, ms)."""
    key = api_key()
    if not key:
        return 0, "no-api-key", 0
    body = json.dumps({
        "model": model,
        "messages": [{"role": "user", "content": "ok"}],
        "max_tokens": 4,
    }).encode()
    req = urllib.request.Request(
        "http://127.0.0.1:20129/v1/chat/completions", data=body,
        headers={"Content-Type": "application/json",
                 "Authorization": "Bearer " + key})
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=PROBE_TIMEOUT) as r:
            r.read()
            return r.status, "", int((time.time() - t0) * 1000)
    except urllib.error.HTTPError as e:
        try:
            txt = e.read().decode("utf-8", "replace")
        except Exception:
            txt = ""
        low = txt.lower()
        kind = ""
        for marker in ("FreeUsageLimit", "FreeTierError", "abusive",
                       "Rate limit", "rate_limit"):
            if marker.lower() in low:
                kind = marker
                break
        return e.code, kind or ("http-%d" % e.code), int((time.time() - t0) * 1000)
    except Exception as e:
        return 0, type(e).__name__, int((time.time() - t0) * 1000)


# ---------------------------------------------------------------- sampling

def busy():
    """Jangan ambil sampel saat mesin sedang dihajar build (hasil jadi palsu)."""
    try:
        with open("/proc/loadavg") as f:
            return float(f.read().split()[0]) > 2.0
    except Exception:
        return False


def take_sample():
    connected = warp_connected()
    wip = warp_ip() if connected else ""
    reg, reg_age = warp_registration()
    ip = shell_ip()
    code, err, ms = probe()
    return {"t": int(time.time()), "warp_ip": wip, "warp_up": connected,
            "reg": reg, "reg_age": reg_age, "shell_ip": ip,
            "code": code, "err": err, "ms": ms}


def append_sample(s):
    os.makedirs(os.path.dirname(SAMPLES), exist_ok=True)
    with open(SAMPLES, "a") as f:
        f.write(json.dumps(s) + "\n")


def load_samples():
    if not os.path.exists(SAMPLES):
        return []
    cutoff = time.time() - KEEP_DAYS * 86400
    out = []
    for line in open(SAMPLES):
        line = line.strip()
        if not line:
            continue
        try:
            s = json.loads(line)
        except Exception:
            continue
        if s.get("t", 0) >= cutoff:
            out.append(s)
    return out[-MAX_SAMPLES:]


def loop():
    while True:
        time.sleep(5)
        with _lock:
            due = time.time() >= _state["next_at"]
        if not due:
            continue
        if busy():
            time.sleep(45)
            continue
        try:
            s = take_sample()
            with _lock:
                _state["samples"] = (_state["samples"] + [s])[-MAX_SAMPLES:]
                _state["next_at"] = time.time() + POLL_SECS
                _state["last_err"] = ""
            append_sample(s)
        except Exception as e:
            with _lock:
                _state["last_err"] = "%s: %s" % (type(e).__name__, e)
                _state["next_at"] = time.time() + 300


# ---------------------------------------------------------------- aggregate

def build_summary(samples):
    if not samples:
        return {"n": 0, "ok": 0, "bad": 0, "unique_ips": 0, "ip_changes": 0,
                "kinds": [], "next_at": _state["next_at"],
                "last_err": _state["last_err"], "warp_up": False,
                "current_ip": "", "shell_ip": ""}
    ok = sum(1 for s in samples if s.get("code") == 200)
    kinds = collections.Counter(s.get("err") for s in samples if s.get("err"))
    changes, prev = 0, None
    for s in samples:
        ip = s.get("warp_ip") or ""
        if ip:
            if prev is not None and ip != prev:
                changes += 1
            prev = ip
    last = samples[-1]
    return {"n": len(samples), "ok": ok, "bad": len(samples) - ok,
            "unique_ips": len({s.get("warp_ip") for s in samples if s.get("warp_ip")}),
            "ip_changes": changes, "kinds": kinds.most_common(6),
            "next_at": _state["next_at"], "last_err": _state["last_err"],
            "warp_up": last.get("warp_up", False),
            "current_ip": last.get("warp_ip", ""),
            "shell_ip": last.get("shell_ip", "")}


def build_ips(samples):
    by = collections.OrderedDict()
    for s in samples:
        ip = s.get("warp_ip") or ""
        if not ip:
            continue
        e = by.setdefault(ip, {"ip": ip, "n": 0, "ok": 0, "bad": 0,
                               "first": s["t"], "last": s["t"], "kinds": set()})
        e["n"] += 1
        e["last"] = s["t"]
        if s.get("code") == 200:
            e["ok"] += 1
        else:
            e["bad"] += 1
            if s.get("err"):
                e["kinds"].add(s["err"])
    rows = list(by.values())
    for r in rows:
        r["kinds"] = sorted(r["kinds"])
        r["geo"] = _state["geo"].get(r["ip"], "")
    rows.sort(key=lambda r: r["last"], reverse=True)
    return rows


# ---------------------------------------------------------------- page

PAGE = r"""<!doctype html>
<html lang="id"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>egress ledger</title>
<link rel="icon" type="image/svg+xml" href="/favicon.svg">
<style>
:root{
  --paper:#e9e3d5; --ink:#181410; --ink2:#5c554a; --ink3:#918878;
  --rule:#c8c0af; --red:#a83a2c; --green:#31523c;
}
*{box-sizing:border-box}
html,body{margin:0}
body{
  background:var(--paper); color:var(--ink);
  font-family:ui-monospace,"SF Mono","JetBrains Mono","IBM Plex Mono",monospace;
  font-size:13px; line-height:1.55;
  padding:clamp(20px,4.5vw,68px);
  max-width:1260px; margin:0 auto;
  -webkit-font-smoothing:antialiased;
}
/* grain: satu lapis tipis, bukan dekorasi bergambar */
body::after{
  content:"";position:fixed;inset:0;pointer-events:none;z-index:99;
  background-image:repeating-linear-gradient(0deg,rgba(24,20,16,.04) 0 1px,transparent 1px 3px);
  opacity:.5;mix-blend-mode:multiply;
}
header{border-bottom:2px solid var(--ink);padding-bottom:12px;margin-bottom:8px}
.eyebrow{font-size:10px;letter-spacing:.34em;text-transform:uppercase;color:var(--ink3)}
h1{
  font-family:"Times New Roman",Times,Georgia,serif;font-style:italic;
  font-size:clamp(32px,5.8vw,64px);font-weight:400;line-height:.92;
  letter-spacing:-.025em;margin:.18em 0 .12em;
}
.sub{color:var(--ink2);max-width:62ch;margin:0 0 4px}
code{font-size:11.5px;background:rgba(24,20,16,.07);padding:0 3px}

section{margin:36px 0 0}
.hat{display:flex;align-items:baseline;justify-content:space-between;gap:14px;
  border-bottom:1px solid var(--rule);padding-bottom:5px;margin-bottom:12px}
.hat h2{margin:0;font-size:10px;letter-spacing:.3em;text-transform:uppercase;
  font-weight:600;color:var(--ink2)}
.hat .meta{font-size:11px;color:var(--ink3);text-align:right}

/* strip: satu tick per sampel, warna = hasil */
.strip{display:flex;gap:2px;height:74px;align-items:flex-end;padding-bottom:1px;
  border-bottom:1px solid var(--ink);overflow-x:auto;overflow-y:hidden}
.tick{flex:0 0 9px;width:9px;min-width:9px;background:var(--ink3);height:100%}
.tick.ok{background:var(--green)}
.tick.bad{background:var(--red);height:78%}
.tick.down{background:repeating-linear-gradient(45deg,var(--ink3) 0 2px,transparent 2px 4px)}
.tick:hover{outline:2px solid var(--ink);outline-offset:0}
.axis{display:flex;justify-content:space-between;font-size:10px;color:var(--ink3);margin-top:5px}

.stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(138px,1fr));
  border-top:1px solid var(--ink);border-left:1px solid var(--ink)}
.stat{border-right:1px solid var(--ink);border-bottom:1px solid var(--ink);padding:11px 13px}
.stat .k{font-size:9px;letter-spacing:.24em;text-transform:uppercase;color:var(--ink3)}
.stat .v{font-family:"Times New Roman",Georgia,serif;font-size:31px;line-height:1.02;
  margin-top:3px;font-variant-numeric:tabular-nums}
.stat .v small{font-family:ui-monospace,monospace;font-size:11.5px;color:var(--ink2)}
.stat .v.flag{color:var(--red)}

.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(272px,1fr));gap:12px}
.card{border:1px solid var(--ink);padding:12px 14px 13px;position:relative}
.card::before{content:"";position:absolute;left:0;top:0;width:5px;height:100%;background:var(--green)}
.card.dirty::before{background:var(--red)}
.card .ip{font-family:"Times New Roman",Georgia,serif;font-size:20px;letter-spacing:-.012em}
.card .geo{font-size:10px;letter-spacing:.14em;text-transform:uppercase;color:var(--ink3);margin-top:1px}
.card dl{display:grid;grid-template-columns:auto 1fr;gap:2px 10px;margin:10px 0 0;font-size:11.5px}
.card dt{color:var(--ink3)}
.card dd{margin:0;text-align:right;font-variant-numeric:tabular-nums}
.card .tags{margin-top:9px;display:flex;flex-wrap:wrap;gap:4px}
.tag{font-size:9px;letter-spacing:.12em;text-transform:uppercase;
  border:1px solid var(--red);color:var(--red);padding:1px 5px}

.note{border-left:3px solid var(--ink);padding:3px 0 3px 14px;margin:22px 0 0;
  color:var(--ink2);max-width:72ch;font-size:12px}
.note strong{color:var(--ink)}
footer{margin-top:46px;border-top:1px solid var(--rule);padding-top:10px;
  font-size:10px;letter-spacing:.06em;color:var(--ink3);
  display:flex;justify-content:space-between;gap:12px;flex-wrap:wrap}
.empty{padding:26px;border:1px dashed var(--rule);color:var(--ink3);text-align:center}
@media(max-width:520px){.strip{height:56px}.stat .v{font-size:25px}}
</style></head><body>

<header>
  <div class="eyebrow">compaq &middot; egress ledger &middot; :20130</div>
  <h1>WARP exit, on record.</h1>
  <p class="sub">Satu sampel tiap 30 menit: IP keluar WARP, identitas registrasi,
  IP keluar shell, dan satu request nyata ke OpenCode lewat 9router.
  Ditulis append-only ke <code>logs/warp-ip-samples.jsonl</code>.</p>
</header>

<section>
  <div class="hat"><h2>Sample strip</h2><span class="meta" id="stripmeta">&mdash;</span></div>
  <div class="strip" id="strip"></div>
  <div class="axis"><span id="ax0">&mdash;</span><span>sekarang</span></div>
</section>

<section>
  <div class="hat"><h2>Totals</h2><span class="meta" id="nextat">&mdash;</span></div>
  <div class="stats" id="stats"></div>
</section>

<section>
  <div class="hat"><h2>Exit IPs</h2><span class="meta" id="ipmeta">&mdash;</span></div>
  <div class="grid" id="ips"></div>
</section>

<p class="note" id="note"></p>

<footer>
  <span>proxy-dash &middot; python stdlib &middot; data lokal</span>
  <span id="foot">&mdash;</span>
</footer>

<script>
const iso = t => new Date(t*1000).toLocaleString('id-ID',
  {month:'short',day:'2-digit',hour:'2-digit',minute:'2-digit'});

function tick(s){
  let cls = !s.warp_up ? 'down' : (s.code===200 ? 'ok' : 'bad');
  const tip = iso(s.t)+'  '+(s.warp_ip||'tanpa ip')+'  '+
              (s.code||'—')+(s.err?'  '+s.err:'');
  return '<div class="tick '+cls+'" title="'+tip+'"></div>';
}
function stat(k,v,sub,flag){
  return '<div class="stat"><div class="k">'+k+'</div>'+
    '<div class="v'+(flag?' flag':'')+'">'+v+
    (sub?' <small>'+sub+'</small>':'')+'</div></div>';
}

async function poll(){
  try{
    const j = await (await fetch('/api/data')).json();
    const S = j.summary||{}, rows = j.ips||[], sm = j.samples||[];

    document.getElementById('strip').innerHTML = sm.map(tick).join('');
    document.getElementById('stripmeta').textContent =
      sm.length ? sm.length+' sampel · '+iso(sm[0].t)+' → sekarang' : 'belum ada sampel';
    document.getElementById('ax0').textContent = sm.length ? iso(sm[0].t) : '—';

    const rate = S.n ? Math.round(100*S.ok/S.n) : 0;
    document.getElementById('stats').innerHTML =
      stat('Sampel', S.n||0) +
      stat('Lolos', S.ok||0, rate+'%') +
      stat('Gagal', S.bad||0, '', S.bad>0) +
      stat('IP unik', S.unique_ips||0) +
      stat('Ganti IP', S.ip_changes||0) +
      stat('WARP', S.warp_up?'ON':'OFF', '', !S.warp_up);

    document.getElementById('nextat').textContent = S.last_err
      ? 'error: '+S.last_err
      : (S.next_at ? 'sampel berikut '+iso(S.next_at) : 'menunggu');

    document.getElementById('ipmeta').textContent = rows.length+' IP tercatat';
    document.getElementById('ips').innerHTML = rows.length ? rows.map(r=>
      '<div class="card '+(r.bad>0?'dirty':'')+'">'+
      '<div class="ip">'+r.ip+'</div>'+
      '<div class="geo">'+(r.geo||'lokasi belum diketahui')+'</div>'+
      '<dl><dt>sampel</dt><dd>'+r.n+'</dd>'+
      '<dt>lolos</dt><dd>'+r.ok+'</dd>'+
      '<dt>gagal</dt><dd>'+r.bad+'</dd>'+
      '<dt>pertama</dt><dd>'+iso(r.first)+'</dd>'+
      '<dt>terakhir</dt><dd>'+iso(r.last)+'</dd></dl>'+
      (r.kinds.length ? '<div class="tags">'+
        r.kinds.map(k=>'<span class="tag">'+k+'</span>').join('')+'</div>' : '')+
      '</div>').join('')
      : '<div class="empty">Belum ada IP tercatat. Sampel pertama diambil saat service start.</div>';

    document.getElementById('note').innerHTML =
      'IP keluar shell sekarang <strong>'+(S.shell_ip||'tidak diketahui')+
      '</strong>'+(S.current_ip?' · WARP <strong>'+S.current_ip+'</strong>':'')+'. '+
      (S.kinds.length
        ? 'Kegagalan tercatat: '+S.kinds.map(([k,n])=>k+' ×'+n).join(', ')+'.'
        : 'Belum ada kegagalan tercatat.');

    document.getElementById('foot').textContent =
      sm.length ? 'terakhir '+iso(sm[sm.length-1].t) : '—';
  }catch(e){}
}
setInterval(poll, 5000); poll();
</script></body></html>"""


class H(BaseHTTPRequestHandler):
    def _send(self, body, ctype="text/html; charset=utf-8", code=200):
        if isinstance(body, str):
            body = body.encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        u = urllib.parse.urlparse(self.path)
        if u.path == "/api/data":
            with _lock:
                samples = list(_state["samples"])
                summary = build_summary(samples)
            geo_lookup(sorted({s.get("warp_ip", "") for s in samples} - {""}))
            self._send(json.dumps({
                "summary": summary,
                "ips": build_ips(samples),
                "samples": [{"t": s["t"], "warp_ip": s.get("warp_ip", ""),
                             "warp_up": s.get("warp_up", False),
                             "code": s.get("code", 0), "err": s.get("err", "")}
                            for s in samples[-400:]],
            }), "application/json")
            return
        if u.path == "/favicon.svg":
            self._send('<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32">'
                       '<rect width="32" height="32" fill="#e9e3d5"/>'
                       '<rect x="6" y="11" width="3" height="11" fill="#31523c"/>'
                       '<rect x="11" y="8" width="3" height="14" fill="#31523c"/>'
                       '<rect x="16" y="14" width="3" height="8" fill="#a83a2c"/>'
                       '<rect x="21" y="10" width="3" height="12" fill="#31523c"/>'
                       '</svg>', "image/svg+xml")
            return
        self._send(PAGE)

    def do_POST(self):
        self._send(json.dumps({"ok": True}), "application/json")

    def log_message(self, *a):
        pass


if __name__ == "__main__":
    os.makedirs(os.path.dirname(SAMPLES), exist_ok=True)
    load_geo()
    with _lock:
        _state["samples"] = load_samples()
        _state["next_at"] = 0.0
    threading.Thread(target=loop, daemon=True).start()
    httpd = HTTPServer(("0.0.0.0", PORT), H)
    print("proxy-dash :%d — warp egress ledger (%d sampel lama)"
          % (PORT, len(_state["samples"])), flush=True)
    httpd.serve_forever()
