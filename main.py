import os
import time
import json
import logging
import threading
from datetime import datetime, timezone
from urllib.parse import urlparse
from flask import Flask, jsonify, render_template_string, request
import pg8000.native
import requests

ODDS_API_KEY = os.environ.get("ODDS_API_KEY", "")
FOOTBALL_API_KEY = os.environ.get("FOOTBALL_API_KEY", "")
DATABASE_URL = os.environ.get("DATABASE_URL", "")
PORT = int(os.environ.get("PORT", 8080))
POLL_INTERVAL = 30

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("papagoal")

app = Flask(__name__)

def parse_db_url(url):
    p = urlparse(url)
    return {"host": p.hostname, "port": p.port or 5432, "database": p.path.lstrip("/"), "user": p.username, "password": p.password, "ssl_context": True}

def get_db():
    return pg8000.native.Connection(**parse_db_url(DATABASE_URL))

def init_db():
    conn = get_db()
    try:
        conn.run("CREATE TABLE IF NOT EXISTS odds_snapshots (id SERIAL PRIMARY KEY, captured_at TIMESTAMPTZ DEFAULT NOW(), match_id TEXT, home_team TEXT, away_team TEXT, sport TEXT, bookmaker TEXT, market TEXT, outcome TEXT, price FLOAT, prev_price FLOAT, price_held_seconds INT DEFAULT 0)")
        conn.run("CREATE INDEX IF NOT EXISTS idx_match_id ON odds_snapshots(match_id)")
        conn.run("CREATE INDEX IF NOT EXISTS idx_captured_at ON odds_snapshots(captured_at)")
        conn.run("CREATE TABLE IF NOT EXISTS goals (id SERIAL PRIMARY KEY, recorded_at TIMESTAMPTZ DEFAULT NOW(), match_id TEXT, home_team TEXT, away_team TEXT, match_minute INT, score TEXT, over_price_30s FLOAT, over_price_60s FLOAT, notes TEXT)")
        conn.run("CREATE TABLE IF NOT EXISTS signals (id SERIAL PRIMARY KEY, detected_at TIMESTAMPTZ DEFAULT NOW(), match_id TEXT, home_team TEXT, away_team TEXT, rule_name TEXT, rule_number INT, confidence INT, verdict TEXT, over_price FLOAT, draw_price FLOAT)")
        log.info("✅ Database initialized")
    except Exception as e:
        log.error(f"DB init error: {e}")
    finally:
        conn.close()

def run_engine(match_id, home, away, over, draw, home_win, minute, duration_seconds):
    signals = []
    o = over or 0
    d = draw or 0
    hw = home_win or 0
    m = minute or 0
    dur = duration_seconds or 0
    if 21 <= m <= 25 and 1.57 <= d <= 1.66 and 1.83 <= o <= 2.10:
        signals.append({"rule": 1, "name": "Early Draw Signal", "confidence": 75, "verdict": "DRAW or UNDER"})
    if 26 <= m <= 30 and 1.80 <= o <= 1.86 and 1.58 <= d <= 1.64:
        signals.append({"rule": 2, "name": "Frozen Over", "confidence": 70, "verdict": "NO ENTRY"})
    if 1.66 <= o <= 1.75:
        signals.append({"rule": 3, "name": "Two Early Goals Trap", "confidence": 72, "verdict": "UNDER"})
    if 30 <= m <= 34 and o >= 2.10:
        signals.append({"rule": 4, "name": "Over 2.10 = Value", "confidence": 78, "verdict": "GOAL ENTRY"})
    if 1.63 <= o <= 1.69:
        signals.append({"rule": 5, "name": "1.66 Trap", "confidence": 80, "verdict": "DO NOT ENTER"})
    if 1.58 <= d <= 1.64 and 1.87 <= o <= 1.93:
        signals.append({"rule": 6, "name": "Pair Signal 1.61+1.90", "confidence": 83, "verdict": "GOAL"})
    if 65 <= m <= 70 and o >= 2.15:
        signals.append({"rule": 7, "name": "3rd Goal Moment", "confidence": 76, "verdict": "GOAL ENTRY"})
    if m >= 82 and o >= 2.80:
        signals.append({"rule": 8, "name": "Market Shut", "confidence": 88, "verdict": "NO GOAL"})
    if 17 <= m <= 20 and o <= 1.55:
        signals.append({"rule": 11, "name": "Early Drop Signal", "confidence": 86, "verdict": "GOAL VERY SOON"})
    if m <= 15 and hw <= 1.32:
        signals.append({"rule": 12, "name": "Opening 1.30 Rule", "confidence": 88, "verdict": "EARLY GOAL"})
    if 1.54 <= o <= 1.60:
        signals.append({"rule": 14, "name": "1.57 Entry Point", "confidence": 79, "verdict": "ENTRY"})
    if 2.30 <= o <= 2.70:
        if dur >= 120:
            signals.append({"rule": 15, "name": "Duration HELD 2min+", "confidence": 82, "verdict": "POSSIBLE GOAL"})
        elif 0 < dur <= 30:
            signals.append({"rule": 15, "name": "Duration REJECTED 30s", "confidence": 80, "verdict": "NO GOAL"})
    return signals

live_match_minutes = {}  # match_id -> {minute, score, home, away}

def fetch_live_minutes():
    """Fetch live match minutes from API-Football"""
    if not FOOTBALL_API_KEY:
        return
    try:
        headers = {"x-apisports-key": FOOTBALL_API_KEY}
        resp = requests.get("https://v3.football.api-sports.io/fixtures?live=all", headers=headers, timeout=10)
        if resp.status_code != 200:
            return
        data = resp.json()
        for fixture in data.get("response", []):
            teams = fixture.get("teams", {})
            status = fixture.get("fixture", {}).get("status", {})
            goals = fixture.get("goals", {})
            home = teams.get("home", {}).get("name", "")
            away = teams.get("away", {}).get("name", "")
            minute = status.get("elapsed", 0) or 0
            score = f"{goals.get('home', 0)}-{goals.get('away', 0)}"
            fid = str(fixture.get("fixture", {}).get("id", ""))
            live_match_minutes[home + "_" + away] = {"minute": minute, "score": score, "fid": fid}
        log.info(f"⏱ Got {len(data.get('response', []))} live fixtures")
    except Exception as e:
        log.error(f"Football API error: {e}")

def get_match_minute(home, away):
    """Try to find minute for a match"""
    key = home + "_" + away
    if key in live_match_minutes:
        return live_match_minutes[key]["minute"]
    # Try partial match
    for k, v in live_match_minutes.items():
        if home.split()[0].lower() in k.lower() or away.split()[0].lower() in k.lower():
            return v["minute"]
    return 0



def collect_odds():
    try:
        url = "https://api.the-odds-api.com/v4/sports/soccer/odds/"
        params = {"apiKey": ODDS_API_KEY, "regions": "eu", "markets": "h2h,totals", "oddsFormat": "decimal", "dateFormat": "iso"}
        resp = requests.get(url, params=params, timeout=15)
        if resp.status_code != 200:
            log.warning(f"API error: {resp.status_code}")
            return
        games = resp.json()
        log.info(f"📡 Fetched {len(games)} games")
        conn = get_db()
        try:
            for game in games:
                match_id = game["id"]
                home = game["home_team"]
                away = game["away_team"]
                sport = game["sport_key"]
                over_price = None
                draw_price = None
                home_win = None
                for bookmaker in game.get("bookmakers", [])[:1]:
                    bname = bookmaker["key"]
                    for market in bookmaker.get("markets", []):
                        mkey = market["key"]
                        for outcome in market.get("outcomes", []):
                            oname = outcome["name"]
                            price = float(outcome["price"])
                            key = f"{match_id}_{mkey}_{oname}"
                            now = time.time()
                            prev_price = None
                            held_seconds = 0
                            if key in last_prices:
                                lp = last_prices[key]
                                prev_price = lp["price"]
                                if abs(price - lp["price"]) < 0.01:
                                    held_seconds = int(now - lp["since"])
                                else:
                                    last_prices[key] = {"price": price, "since": now}
                            else:
                                last_prices[key] = {"price": price, "since": now}
                            held_seconds = int(now - last_prices[key]["since"])
                            if mkey == "totals" and oname == "Over":
                                over_price = price
                            if mkey == "h2h":
                                if oname == "Draw":
                                    draw_price = price
                                elif oname == home:
                                    home_win = price
                            conn.run("INSERT INTO odds_snapshots (match_id, home_team, away_team, sport, bookmaker, market, outcome, price, prev_price, price_held_seconds) VALUES (:a, :b, :c, :d, :e, :f, :g, :h, :i, :j)", a=match_id, b=home, c=away, d=sport, e=bname, f=mkey, g=oname, h=price, i=prev_price, j=held_seconds)
                if over_price:
                    dur = 0
                    key_over = f"{match_id}_totals_Over"
                    if key_over in last_prices:
                        dur = int(time.time() - last_prices[key_over]["since"])
                    minute = get_match_minute(home, away)
                    sigs = run_engine(match_id, home, away, over_price, draw_price, home_win, minute, dur)
                    for s in sigs:
                        conn.run("INSERT INTO signals (match_id, home_team, away_team, rule_name, rule_number, confidence, verdict, over_price, draw_price) VALUES (:a, :b, :c, :d, :e, :f, :g, :h, :i)", a=match_id, b=home, c=away, d=s["name"], e=s["rule"], f=s["confidence"], g=s["verdict"], h=over_price, i=draw_price)
            log.info("✅ Data saved")
        finally:
            conn.close()
    except Exception as e:
        log.error(f"Collect error: {e}")

def collector_loop():
    time.sleep(5)
    fetch_live_minutes()
    while True:
        collect_odds()
        fetch_live_minutes()
        time.sleep(POLL_INTERVAL)

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="he" dir="rtl">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"><title>PapaGoal</title>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;700&family=Heebo:wght@400;700;900&display=swap" rel="stylesheet">
<style>
:root{--bg:#04040f;--card:#0a0a1e;--border:#1a1a3a;--green:#00ff88;--red:#ff3355;--yellow:#ffcc00;--orange:#ff6b35;--blue:#00cfff;--text:#e0e0ff;--muted:#555577}
*{box-sizing:border-box;margin:0;padding:0}body{background:var(--bg);color:var(--text);font-family:'Heebo',sans-serif}
header{background:linear-gradient(90deg,#000010,#0a0a2e);border-bottom:1px solid var(--border);padding:16px 24px;display:flex;align-items:center;gap:16px;position:sticky;top:0;z-index:100}
.logo{font-size:24px;font-family:'IBM Plex Mono',monospace;font-weight:700;color:#fff;letter-spacing:3px}.logo span{color:var(--green)}
.dot{width:10px;height:10px;border-radius:50%;background:var(--green);animation:blink 1s infinite}
.live{display:flex;align-items:center;gap:6px;font-size:11px;color:var(--green);letter-spacing:2px;margin-right:auto}
.upd{font-size:11px;color:var(--muted);font-family:'IBM Plex Mono',monospace}
.wrap{max-width:1200px;margin:0 auto;padding:24px 16px}
.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:24px}
.sc{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:16px;text-align:center}
.sn{font-size:32px;font-weight:900;font-family:'IBM Plex Mono',monospace}.sl{font-size:11px;color:var(--muted);margin-top:4px}
.st{font-size:12px;letter-spacing:3px;color:var(--muted);text-transform:uppercase;margin-bottom:12px;padding-bottom:8px;border-bottom:1px solid var(--border)}
.sg{display:grid;gap:12px;margin-bottom:32px}
.scard{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:16px;display:grid;grid-template-columns:auto 1fr auto;gap:16px;align-items:center}
.scard.green{border-color:var(--green)44}.scard.red{border-color:var(--red)44}.scard.yellow{border-color:var(--yellow)44}.scard.orange{border-color:var(--orange)44}
.rb{width:44px;height:44px;border-radius:10px;display:flex;align-items:center;justify-content:center;font-family:'IBM Plex Mono',monospace;font-weight:700;font-size:13px;flex-shrink:0}
.rb.green{background:var(--green)22;color:var(--green)}.rb.red{background:var(--red)22;color:var(--red)}.rb.yellow{background:var(--yellow)22;color:var(--yellow)}.rb.orange{background:var(--orange)22;color:var(--orange)}
.sm{font-size:15px;font-weight:700}.srn{font-size:12px;color:var(--muted);margin-top:2px}
.or{display:flex;gap:12px;margin-top:6px;font-family:'IBM Plex Mono',monospace;font-size:12px}.ot{background:#ffffff0a;border-radius:4px;padding:2px 8px}
.vb{padding:6px 14px;border-radius:8px;font-size:12px;font-weight:700;letter-spacing:1px;white-space:nowrap}
.vb.green{background:var(--green)22;color:var(--green);border:1px solid var(--green)44}.vb.red{background:var(--red)22;color:var(--red);border:1px solid var(--red)44}
.vb.yellow{background:var(--yellow)22;color:var(--yellow);border:1px solid var(--yellow)44}.vb.orange{background:var(--orange)22;color:var(--orange);border:1px solid var(--orange)44}
.tw{background:var(--card);border:1px solid var(--border);border-radius:12px;overflow:hidden;margin-bottom:32px}
table{width:100%;border-collapse:collapse;font-size:13px}th{background:#0f0f2a;padding:12px 16px;text-align:right;font-size:11px;color:var(--muted);font-weight:400}
td{padding:12px 16px;border-top:1px solid var(--border)88}.empty{text-align:center;padding:48px;color:var(--muted)}
.pu{color:var(--red)}.pd{color:var(--green)}
@keyframes blink{0%,100%{opacity:1}50%{opacity:0.3}}
@media(max-width:600px){.stats{grid-template-columns:repeat(2,1fr)}.vb{display:none}}
</style></head><body>
<header><div class="logo">PAPA<span>GOAL</span></div><div class="live"><div class="dot"></div>LIVE</div><div class="upd" id="upd">מתעדכן...</div></header>
<div class="wrap">
<div class="stats">
<div class="sc"><div class="sn" style="color:var(--blue)" id="g">—</div><div class="sl">משחקים פעילים</div></div>
<div class="sc"><div class="sn" style="color:var(--green)" id="s">—</div><div class="sl">אותות היום</div></div>
<div class="sc"><div class="sn" style="color:var(--yellow)" id="d">—</div><div class="sl">דגימות נשמרו</div></div>
<div class="sc"><div class="sn" style="color:var(--orange)" id="gl">—</div><div class="sl">גולים מוקלטים</div></div>
</div>
<div class="st">🔥 אותות פעילים – PapaGoal Engine</div>
<div class="sg" id="sg"><div class="empty">📡 אוסף נתונים... חזור בעוד 30 שניות</div></div>
<div class="st">📊 יחסים אחרונים</div>
<div class="tw"><table><thead><tr><th>משחק</th><th>שוק</th><th>תוצאה</th><th>יחס</th><th>שינוי</th><th>החזיק</th><th>זמן</th></tr></thead><tbody id="ob"><tr><td colspan="7" class="empty">טוען...</td></tr></tbody></table></div>
</div>
<script>
const cm={1:'yellow',2:'orange',3:'red',4:'green',5:'red',6:'green',7:'green',8:'red',11:'green',12:'green',14:'green',15:'green'};
async function load(){
try{
const[st,si,od]=await Promise.all([fetch('/api/stats').then(r=>r.json()),fetch('/api/signals').then(r=>r.json()),fetch('/api/odds').then(r=>r.json())]);
document.getElementById('g').textContent=st.games||0;
document.getElementById('s').textContent=st.signals_today||0;
document.getElementById('d').textContent=st.snapshots||0;
document.getElementById('gl').textContent=st.goals||0;
document.getElementById('upd').textContent='עדכון: '+new Date().toLocaleTimeString('he-IL');
const sg=document.getElementById('sg');
if(!si.length){sg.innerHTML='<div class="empty">✅ אין אותות פעילים כרגע</div>';}
else{sg.innerHTML=si.map(s=>{const c=cm[s.rule_number]||'yellow';return`<div class="scard ${c}"><div class="rb ${c}">R${s.rule_number}</div><div><div class="sm">${s.home_team} vs ${s.away_team}</div><div class="srn">${s.rule_name}</div><div class="or"><span class="ot">Over: ${s.over_price||'—'}</span>${s.draw_price?'<span class="ot">Draw: '+s.draw_price+'</span>':''}</div></div><div class="vb ${c}">${s.verdict}</div></div>`;}).join('');}
const ob=document.getElementById('ob');
if(!od.length){ob.innerHTML='<tr><td colspan="7" class="empty">אין נתונים עדיין</td></tr>';}
else{ob.innerHTML=od.map(o=>{const diff=o.prev_price?(o.price-o.prev_price).toFixed(2):null;const dc=!diff?'':(parseFloat(diff)>0?'pu':'pd');const dt=!diff?'—':(parseFloat(diff)>0?'▲ '+diff:'▼ '+Math.abs(diff));const h=o.price_held_seconds>0?Math.floor(o.price_held_seconds/60)+'m '+o.price_held_seconds%60+'s':'—';return`<tr><td>${o.home_team} vs ${o.away_team}</td><td>${o.market}</td><td>${o.outcome}</td><td><b>${o.price}</b></td><td class="${dc}">${dt}</td><td>${h}</td><td>${new Date(o.captured_at).toLocaleTimeString('he-IL')}</td></tr>`;}).join('');}
}catch(e){console.error(e);}
}
load();setInterval(load,15000);
</script></body></html>"""

@app.route("/")
def dashboard():
    return render_template_string(DASHBOARD_HTML)

@app.route("/api/stats")
def api_stats():
    try:
        conn = get_db()
        try:
            r1 = conn.run("SELECT COUNT(DISTINCT match_id) FROM odds_snapshots WHERE captured_at > NOW() - INTERVAL '1 hour'")
            r2 = conn.run("SELECT COUNT(*) FROM signals WHERE detected_at > NOW() - INTERVAL '24 hours'")
            r3 = conn.run("SELECT COUNT(*) FROM odds_snapshots")
            r4 = conn.run("SELECT COUNT(*) FROM goals")
            return jsonify({"games": r1[0][0], "signals_today": r2[0][0], "snapshots": r3[0][0], "goals": r4[0][0]})
        finally:
            conn.close()
    except Exception as e:
        log.error(f"Stats error: {e}")
        return jsonify({"games": 0, "signals_today": 0, "snapshots": 0, "goals": 0})

@app.route("/api/signals")
def api_signals():
    try:
        conn = get_db()
        try:
            rows = conn.run("SELECT id, detected_at, match_id, home_team, away_team, rule_name, rule_number, confidence, verdict, over_price, draw_price FROM signals WHERE detected_at > NOW() - INTERVAL '30 minutes' ORDER BY detected_at DESC LIMIT 20")
            cols = ["id","detected_at","match_id","home_team","away_team","rule_name","rule_number","confidence","verdict","over_price","draw_price"]
            result = [dict(zip(cols, r)) for r in rows]
            for r in result:
                r["detected_at"] = str(r["detected_at"])
                r["color"] = {1:"yellow",2:"orange",3:"red",4:"green",5:"red",6:"green",7:"green",8:"red",11:"green",12:"green",14:"green",15:"green"}.get(r["rule_number"],"yellow")
            return jsonify(result)
        finally:
            conn.close()
    except Exception as e:
        log.error(f"Signals error: {e}")
        return jsonify([])

@app.route("/api/odds")
def api_odds():
    try:
        conn = get_db()
        try:
            rows = conn.run("SELECT DISTINCT ON (match_id, market, outcome) match_id, home_team, away_team, market, outcome, price, prev_price, price_held_seconds, captured_at FROM odds_snapshots WHERE captured_at > NOW() - INTERVAL '2 minutes' ORDER BY match_id, market, outcome, captured_at DESC LIMIT 100")
            cols = ["match_id","home_team","away_team","market","outcome","price","prev_price","price_held_seconds","captured_at"]
            result = [dict(zip(cols, r)) for r in rows]
            for r in result:
                r["captured_at"] = str(r["captured_at"])
            return jsonify(result)
        finally:
            conn.close()
    except Exception as e:
        log.error(f"Odds error: {e}")
        return jsonify([])

@app.route("/health")
def health():
    return jsonify({"status": "ok", "time": datetime.now(timezone.utc).isoformat()})

# ─── Auto-start collector when loaded by gunicorn ────────────────────────────
init_db()
_collector_thread = threading.Thread(target=collector_loop, daemon=True)
_collector_thread.start()
log.info("📡 Collector started")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, debug=False)
