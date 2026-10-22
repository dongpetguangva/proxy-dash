#!/usr/bin/env python3
"""proxy-dash — dashboard monitor proxy 9router TEST (:20129) di :20130.

RECORDER dengan tombol START/STOP:
- OFF (default): backend idle, tabel kosong, nol baca log.
- ON: tail log live (detik), tabel update realtime per detik (polling /api/data).
- AUTO-OFF: tidak ada yang buka dashboard 3 jam -> recorder mati + state reset.

Geo negara via ip-api.com batch (cache di geo-cache.json).
Stdlib only. Master: ~/projects/proxy/proxy-dash.py
"""
import json
import os
import re
import sqlite3
import threading
import time
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer

PORT = 20130
HERE = os.path.dirname(os.path.abspath(__file__))
LOG = os.path.join(HERE, "logs", "9router-20129.log")
GEO_CACHE = os.path.join(HERE, "geo-cache.json")
DB = "/home/vidh-compaq/apps/9router-20129-data/db/data.sqlite"
IDLE_OFF = 3 * 3600  # auto-off kalau 3 jam tidak ada yang akses (non-aktif bila ALWAYS_ON)
ALWAYS_ON = True  # 30-09: standby 24/7 — recorder ON permanen, tanpa auto-off

RE_PROXY = re.compile(
    r"\[(.*?)\].*\[PROXY\]\s+(\S+)\s*\|\s*(\S+).*?"
    r"pool=([0-9a-f-]+)\s*\|\s*url=(\S+)")
RE_ERROR = re.compile(r"ERROR (\d+)")
RE_DONE = re.compile(r"DONE (\d+)ms")

_state = {
    "recording": False,
    "last_access": 0.0,
    "stats": {},       # url -> stats
    "pending": None,
    "pending_age": 0,
    "log_pos": 0,
    "geo": {},
    "pool_names": {},
    "geo_loaded": False,
    "today": "",       # tanggal WIB saat parse (log cuma ada jam) — lihat keterbatasan di README
}
_lock = threading.Lock()


def load_geo():
    if _state["geo_loaded"]:
        return
    _state["geo_loaded"] = True
    try:
        _state["geo"].update(json.load(open(GEO_CACHE)))
    except Exception:
        pass


def save_geo():
    try:
        json.dump(_state["geo"], open(GEO_CACHE, "w"))
    except Exception:
        pass


def geo_lookup(ips):
    load_geo()
    missing = [ip for ip in ips if ip not in _state["geo"]]
    # entry "? (?)" yg lebih tua dari 7 hari dianggap missing (retry, dulu rate-limit)
    try:
        mtime = os.path.getmtime(GEO_CACHE)
        if time.time() - mtime > 7 * 86400:
            missing += [ip for ip in ips
                        if _state["geo"].get(ip) == "? (?)"]
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
            with urllib.request.urlopen(req, timeout=20) as r:
                for e in json.loads(r.read().decode()):
                    if e.get("status") == "success":
                        _state["geo"][e["query"]] = "%s (%s)" % (
                            e.get("country", "?"), e.get("countryCode", "?"))
                    else:
                        _state["geo"][e["query"]] = "? (?)"
        except Exception:
            for ip in batch:
                _state["geo"].setdefault(ip, "? (?)")
        time.sleep(1.5)
    save_geo()


def pool_names():
    if _state["pool_names"]:
        return _state["pool_names"]
    try:
        c = sqlite3.connect("file:%s?mode=ro" % DB, uri=True)
        for pid, data in c.execute("SELECT id, data FROM proxyPools"):
            try:
                _state["pool_names"][pid] = json.loads(data).get("name", pid[:8])
            except Exception:
                _state["pool_names"][pid] = pid[:8]
        c.close()
    except Exception:
        pass
    return _state["pool_names"]


def _split_ts(ts):
    """Pecah 'HH:MM:SS' -> (detik, tampilan, bump_hari).

    Bug sumber di 9router: lewat tengah malam dia nulis 24:xx (harusnya 00:xx).
    Normalisasi: jam >=24 dikurangi 24, bump_hari=+1 per 24 jam kelebihan.
    """
    try:
        h, m, s = ts.strip().split(":")
        h, m, s = int(h), int(m), int(s)
        bump = 0
        while h >= 24:
            h -= 24
            bump += 1
        return h * 3600 + m * 60 + s, "%02d:%02d:%02d" % (h, m, s), bump
    except Exception:
        return -1, ts, 0


def _clock_raw_secs(ts):
    """'HH:MM:SS' -> detik MENTAH (tanpa normalisasi 24:xx).
    Untuk pass-1 _init_history: jam 24:xx = kelanjutan hari yang sama,
    BUKAN mundur. Return -1 kalau parse gagal."""
    try:
        h, m, s = ts.strip().split(":")
        return int(h) * 3600 + int(m) * 60 + int(s)
    except Exception:
        return -1


def _clock_secs(ts):
    """'HH:MM:SS' -> detik (ternormalisasi, 24:xx dianggap 00:xx). -1 kalau gagal."""
    sec, _, _ = _split_ts(ts)
    return sec


def _init_history():
    """Two-pass init: hitung midnight-crossing dari urutan jam, lalu feed
    berurutan dengan tanggal maju yang benar. Keterbatasan jujur: log 9router
    cuma ada jam (tanpa tanggal); tanggal direkonstruksi dari urutan +
    mtime file. Akurat selama log kronologis dan tidak ada gap >24 jam."""
    import datetime
    try:
        lines = open(LOG, errors="replace").readlines()
    except Exception:
        return
    clocks = []
    for ln in lines:
        m = RE_PROXY.search(ln)
        if m:
            # nilai MENTAH: 24:xx > 23:xx = maju (bukan crossing)
            sec = _clock_raw_secs(m.group(1))
            if sec >= 0:
                clocks.append(sec)
    # crossing = mundur beneran pada jam TERNORMALISASI (24:xx dianggap 00:xx).
    # Catatan bug lama: hitung mundur pada jam MENTAH + klausul enter-24h
    # terpisah menghitung dobel transisi 24:xx->01:xx (lanjutan hari yang sama,
    # mis. 24:58 -> 01:03) sebagai crossing. Versi ini normalisasi dulu,
    # sehingga 23:59->24:00 (=00:00) tetap kehitung sekali, dan 24:58->01:03
    # benar TIDAK kehitung. Konsisten dengan _stamp() live.
    norm = []
    for sec in clocks:
        while sec >= 24 * 3600:
            sec -= 24 * 3600
        norm.append(sec)
    crossings = sum(1 for i in range(1, len(norm))
                    if norm[i] < norm[i - 1] - 3600)
    try:
        mt_day = datetime.datetime.fromtimestamp(
            os.path.getmtime(LOG)).date()
    except Exception:
        mt_day = datetime.date.today()
    _state["day_base"] = (mt_day - datetime.timedelta(days=crossings)).isoformat()
    _state["last_clock"] = -1
    for ln in lines:
        feed_line(ln)
    try:
        _state["log_pos"] = os.path.getsize(LOG)
    except Exception:
        pass


def _stamp(ts):
    """Tempel tanggal ke jam. day_base/last_clock diset _init_history
    (two-pass); tail live tinggal maju: mundur drastis = tengah malam.
    Jam 24:xx (bug 9router) dinormalisasi ke 00:xx — deteksi midnight-crossing
    yang ada otomatis naik 1 hari."""
    import datetime
    sec, shown, _ = _split_ts(ts)
    raw = _clock_raw_secs(ts)
    if sec < 0:
        return ts
    if not _state.get("day_base"):
        _state["day_base"] = datetime.date.today().isoformat()
        _state["last_clock"] = sec
        _state["last_raw"] = raw
    elif raw >= 24 * 3600 > _state.get("last_raw", -1):
        # masuk segmen 24:xx+ (bug 9router) = sebenarnya sudah ganti hari
        d = datetime.date.fromisoformat(_state["day_base"]) + datetime.timedelta(days=1)
        _state["day_base"] = d.isoformat()
    elif sec < _state.get("last_clock", -1) - 3600:
        d = datetime.date.fromisoformat(_state["day_base"]) + datetime.timedelta(days=1)
        _state["day_base"] = d.isoformat()
    _state["last_clock"] = sec
    _state["last_raw"] = raw
    return "%s %s" % (_state["day_base"], shown)


def feed_line(line):
    """Makan 1 baris log ke state (dipanggil tailer, bukan full parse)."""
    st = _state["stats"]
    m = RE_PROXY.search(line)
    if m:
        ts, prov, model, pool, url = m.groups()
        _state["pending"] = {"ts": ts, "provider": prov, "model": model,
                             "pool": pool, "url": url, "fallback": False}
        _state["pending_age"] = 0
        s = st.setdefault(url, {
            "url": url, "pool": pool, "provider": prov, "model": model,
            "ok": 0, "e403": 0, "d403": 0, "err": 0, "fallback": 0,
            "last": "?", "last_ts": _stamp(ts)})
        s["pool"] = pool
        s["last_ts"] = _stamp(ts)
        return
    p = _state["pending"]
    if p is None:
        return
    _state["pending_age"] += 1
    if _state["pending_age"] > 8:
        if p.get("saw403"):
            s = st.get(p["url"])
            if s:
                if p.get("fallback"):
                    s["d403"] = s.get("d403", 0) + 1
                    s["last"] = "403-DIRECT"
                else:
                    s["err"] += 1
                    s["last"] = "ERROR-403"
                s["last_ts"] = _stamp(p["ts"])
        _state["pending"] = None
        return
    s = st.get(p["url"])
    if s is None:
        return
    if "Proxy failed, falling back to direct" in line:
        p["fallback"] = True
        return
    if "RegionError" in line:
        if p.get("fallback"):
            # proxy mati -> request jalan direct -> 403 BUKAN salah proxy
            s["d403"] = s.get("d403", 0) + 1
            s["last"] = "403-DIRECT"
        else:
            s["e403"] += 1
            s["last"] = "403-REGION"
        s["last_ts"] = _stamp(p["ts"])
        _state["pending"] = None
        return
    me = RE_ERROR.search(line)
    if me:
        if me.group(1) == "403":
            p["saw403"] = True
            return
        s["err"] += 1
        s["last"] = "ERROR-" + me.group(1)
        s["last_ts"] = _stamp(p["ts"])
        _state["pending"] = None
        return
    if RE_DONE.search(line):
        s["ok"] += 1
        if p["fallback"]:
            s["fallback"] += 1
            s["last"] = "OK-VIA-DIRECT"
        else:
            s["last"] = "OK"
        s["last_ts"] = _stamp(p["ts"])
        _state["pending"] = None
        return


def tailer():
    """Loop 1 detik: baca baris baru log hanya saat recording."""
    while True:
        time.sleep(1)
        with _lock:
            if not _state["recording"]:
                continue
            if not ALWAYS_ON and time.time() - _state["last_access"] > IDLE_OFF:
                _state["recording"] = False
                _state["stats"] = {}
                _state["pending"] = None
                continue
            try:
                size = os.path.getsize(LOG)
                if size < _state["log_pos"]:
                    _state["log_pos"] = 0  # log rotate/truncate
                if size == _state["log_pos"]:
                    continue
                with open(LOG, errors="replace") as f:
                    f.seek(_state["log_pos"])
                    for line in f:
                        try:
                            feed_line(line)
                        except Exception:
                            continue  # satu baris rusak tidak boleh macetkan tail
                    _state["log_pos"] = f.tell()
                _state["tail_ok_ts"] = time.time()
            except Exception:
                pass


PAGE = """<!DOCTYPE html><html><head><meta charset="utf-8">
<title>PROXY DASH :20129</title>
<link rel="icon" type="image/svg+xml" href="/favicon.svg">
<style>
body{background:#020c02;color:#00FF41;font-family:monospace;font-size:15px;margin:16px}
h1{font-size:20px} .dim{color:#008F11} table{border-collapse:collapse;width:100%}
th,td{border:1px solid #008F11;padding:4px 8px;text-align:left}
th{background:#013d01} .bad{color:#FF5555} .warn{color:#FFB000}
button{background:#00FF41;color:#020c02;border:0;padding:6px 16px;font-family:inherit;font-size:15px;cursor:pointer}
button.off{background:#FFB000} .big{font-size:18px;padding:10px 28px}
</style></head><body>
<h1>PROXY DASH — 9router TEST :20129</h1>
<p class="dim"><span id="sum"></span> &middot; live 1s &middot; always-on</p>
<table><thead><tr><th>#</th><th>proxy</th><th>negara</th><th>OK</th><th>403</th><th>403-direct</th><th>ERR</th><th>via-direct</th><th>last</th><th>last ts</th></tr></thead>
<tbody id="rows"><tr><td colspan=10>loading…</td></tr></tbody></table>
<script>
async function poll(){
 try{
   let r=await fetch('/api/data'); let j=await r.json();
   document.getElementById('sum').textContent=j.summary.replace(/&middot;/g,'·');
   let tb='';
   j.rows.forEach((x,i)=>{
     let cls=x.e403?'bad':((x.err||x.d403)?'warn':'');
     tb+="<tr class='"+cls+"'><td>"+(i+1)+"</td><td>"+x.url+"</td><td>"+x.geo+"</td><td>"+x.ok+"</td><td>"+x.e403+"</td><td>"+x.d403+"</td><td>"+x.err+"</td><td>"+x.fallback+"</td><td>"+x.last+"</td><td>"+x.last_ts+"</td></tr>";
   });
   if(tb) document.getElementById('rows').innerHTML=tb;
 }catch(e){}
}
setInterval(poll,1000); poll();
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

    def _touch(self):
        with _lock:
            _state["last_access"] = time.time()

    def do_GET(self):
        u = urllib.parse.urlparse(self.path)
        if u.path == "/favicon.svg":
            self._touch()
            try:
                with open(os.path.join(HERE, "favicon.svg"), "rb") as f:
                    self._send(f.read(), "image/svg+xml")
            except Exception:
                self._send(b"", "image/svg+xml", 404)
            return
        if u.path == "/api/data":
            self._touch()
            with _lock:
                rec = _state["recording"]
                items = sorted(_state["stats"].values(),
                               key=lambda x: x["last_ts"], reverse=True)
            ips = set()
            for s in items:
                try:
                    h = urllib.parse.urlparse(s["url"]).hostname
                    if h:
                        ips.add(h)
                except Exception:
                    pass
            if rec and ips:
                geo_lookup(sorted(ips))
            rows = [{"url": s["url"],
                     "geo": _state["geo"].get(
                         urllib.parse.urlparse(s["url"]).hostname or "", "? (?)"),
                     "ok": s["ok"], "e403": s["e403"], "d403": s.get("d403", 0),
                     "err": s["err"],
                     "fallback": s["fallback"], "last": s["last"],
                     "last_ts": s["last_ts"]} for s in items] if rec else []
            n403 = sum(1 for s in items if s["e403"])
            ndirect = sum(1 for s in items if s.get("d403", 0))
            with _lock:
                rec = _state["recording"]
            self._send(json.dumps({
                "recording": rec,
                "tail_ok_ts": _state.get("tail_ok_ts", 0),
                "summary": (("%d proxy &middot; %d kena 403 &middot; %d direct-403"
                             % (len(items), n403, ndirect))) if rec else "recorder OFF",
                "rows": rows}), "application/json")
            return
        self._touch()
        self._send(PAGE)

    def do_POST(self):
        u = urllib.parse.urlparse(self.path)
        self._touch()
        # recorder always-on: /api/stop sengaja no-op (matiin = kill proses di Compaq)
        with _lock:
            if u.path == "/api/start":
                load_geo()
                _state["recording"] = True
                try:
                    _state["log_pos"] = os.path.getsize(LOG)
                except Exception:
                    _state["log_pos"] = 0
            rec = _state["recording"]
        self._send(json.dumps({"recording": rec}), "application/json")

    def log_message(self, *a):
        pass


if __name__ == "__main__":
    load_geo()
    if ALWAYS_ON:
        _state["recording"] = True
        _state["last_access"] = time.time()
        _init_history()  # proses history lama sekali (dengan tanggal), lalu incremental
    threading.Thread(target=tailer, daemon=True).start()
    httpd = HTTPServer(("0.0.0.0", PORT), H)
    print("proxy-dash :%d recorder (log :20129)" % PORT, flush=True)
    httpd.serve_forever()
