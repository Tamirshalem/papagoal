import os
import time
import logging
import threading
from datetime import datetime, timezone
from urllib.parse import urlparse
from flask import Flask, jsonify, render_template_string, request
import pg8000.native
import requests

ODDS_API_KEY = os.environ.get("ODDS_API_KEY", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
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
        conn.run("CREATE TABLE IF NOT EXISTS odds_snapshots (id SERIAL PRIMARY KEY, captured_at TIMESTAMPTZ DEFAULT NOW(), match_id TEXT, home_team TEXT, away_team TEXT, sport TEXT, bookmaker TEXT, market TEXT, outcome TEXT, price FLOAT, prev_price FLOAT, price_held_seconds INT DEFAULT 0, match_minute INT DEFAULT 0, match_score TEXT DEFAULT '0-0')")
        conn.run("CREATE INDEX IF NOT EXISTS idx_match_id ON odds_snapshots(match_id)")
        conn.run("CREATE INDEX IF NOT EXISTS idx_captured_at ON odds_snapshots(captured_at)")
        try:
            conn.run("ALTER TABLE odds_snapshots ADD COLUMN IF NOT EXISTS match_minute INT DEFAULT 0")
        except: pass
        try:
            conn.run("ALTER TABLE odds_snapshots ADD COLUMN IF NOT EXISTS match_score TEXT DEFAULT '0-0'")
        except: pass
        conn.run("CREATE TABLE IF NOT EXISTS goals (id SERIAL PRIMARY KEY, recorded_at TIMESTAMPTZ DEFAULT NOW(), match_id TEXT, home_team TEXT, away_team TEXT, match_minute INT, match_score TEXT, over_price_30s FLOAT, over_price_60s FLOAT, notes TEXT)")
        conn.run("CREATE TABLE IF NOT EXISTS signals (id SERIAL PRIMARY KEY, detected_at TIMESTAMPTZ DEFAULT NOW(), match_id TEXT, home_team TEXT, away_team TEXT, rule_name TEXT, rule_number INT, confidence INT, verdict TEXT, over_price FLOAT, draw_price FLOAT, match_minute INT DEFAULT 0)")
        try:
            conn.run("ALTER TABLE signals ADD COLUMN IF NOT EXISTS match_minute INT DEFAULT 0")
        except: pass
        conn.run("CREATE TABLE IF NOT EXISTS ai_analyses (id SERIAL PRIMARY KEY, analyzed_at TIMESTAMPTZ DEFAULT NOW(), match_id TEXT, home_team TEXT, away_team TEXT, over_price FLOAT, draw_price FLOAT, match_minute INT, analysis TEXT)")
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

def get_ai_analysis(home, away, over, draw, home_win, away_win, minute, signals):
    if not ANTHROPIC_API_KEY:
        return None
    try:
        sig_text = ", ".join([s["name"] for s in signals]) if signals else "אין אותות"
        prompt = f"""אתה PapaGoal AI – מומחה לניתוח שוק הימורים בכדורגל.

משחק: {home} vs {away}
דקה: {minute}
Over: {over} | Draw: {draw} | {home}: {home_win} | {away}: {away_win}
אותות שזוהו: {sig_text}

הפילוסופיה שלנו: אתה לא מנתח משחק – אתה קורא את השוק.
יחסים זזים = כסף חכם נכנס.
Duration Rule: יחס שמחזיק 2+ דקות = שוק מאמין. יחס שקופץ ב-30 שניות = דחייה.

תן המלצה קצרה ב-3 משפטים בעברית:
1. מה השוק אומר?
2. האם כדאי להיכנס?
3. מה הסיכון?"""

        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
            json={"model": "claude-sonnet-4-20250514", "max_tokens": 300, "messages": [{"role": "user", "content": prompt}]},
            timeout=15
        )
        if resp.status_code == 200:
            return resp.json()["content"][0]["text"]
    except Exception as e:
        log.error(f"AI error: {e}")
    return None

last_prices = {}
match_minutes = {}  # manual minutes override
live_match_data = {}  # from Football API

def fetch_live_minutes():
    """Fetch live match minutes from API-Football Pro"""
    if not FOOTBALL_API_KEY:
        return
    try:
        headers = {"x-apisports-key": FOOTBALL_API_KEY}
        resp = requests.get(
            "https://v3.football.api-sports.io/fixtures",
            headers=headers,
            params={"live": "all"},
            timeout=10
        )
        if resp.status_code != 200:
            log.warning(f"Football API: {resp.status_code}")
            return
        fixtures = resp.json().get("response", [])
        log.info(f"⏱ Football API: {len(fixtures)} live fixtures")
        for f in fixtures:
            try:
                home = f["teams"]["home"]["name"]
                away = f["teams"]["away"]["name"]
                minute = f["fixture"]["status"]["elapsed"] or 0
                hg = f["goals"]["home"] or 0
                ag = f["goals"]["away"] or 0
                score = f"{hg}-{ag}"
                key = f"{home}_{away}"
                live_match_data[key] = {"minute": minute, "score": score}
                # Fuzzy keys by first word
                live_match_data[home.split()[0].lower()] = {"minute": minute, "score": score}
                live_match_data[away.split()[0].lower()] = {"minute": minute, "score": score}
            except:
                continue
    except Exception as e:
        log.error(f"Football API error: {e}")

def get_live_data(home, away):
    """Get minute and score for a match"""
    # Manual override first
    for mid, m in match_minutes.items():
        if home in mid or away in mid:
            return m, "0-0"
    # Try exact match
    key = f"{home}_{away}"
    if key in live_match_data:
        d = live_match_data[key]
        return d["minute"], d["score"]
    # Try fuzzy match
    h1 = home.split()[0].lower()
    a1 = away.split()[0].lower()
    if h1 in live_match_data:
        d = live_match_data[h1]
        return d["minute"], d["score"]
    if a1 in live_match_data:
        d = live_match_data[a1]
        return d["minute"], d["score"]
    return 0, "0-0"

def collect_odds():
    try:
        url = "https://api.the-odds-api.com/v4/sports/soccer/odds/"
        params = {"apiKey": ODDS_API_KEY, "regions": "eu", "markets": "h2h,totals", "oddsFormat": "decimal", "dateFormat": "iso"}
        resp = requests.get(url, params=params, timeout=15)
        if resp.status_code != 200:
            log.warning(f"API error: {resp.status_code}")
            return
        games = resp.json()

        # Get scores
        scores_resp = requests.get(f"https://api.the-odds-api.com/v4/sports/soccer/scores/?apiKey={ODDS_API_KEY}&daysFrom=1", timeout=10)
        live_scores = {}
        if scores_resp.status_code == 200:
            for s in scores_resp.json():
                if not s.get("completed") and s.get("scores"):
                    h = next((x["score"] for x in s["scores"] if x["name"] == s["home_team"]), "0")
                    a = next((x["score"] for x in s["scores"] if x["name"] == s["away_team"]), "0")
                    live_scores[s["id"]] = f"{h}-{a}"

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
                away_win = None
                current_score = live_scores.get(match_id, "0-0")
                minute, live_score = get_live_data(home, away)
                if live_score != "0-0":
                    current_score = live_score
                # Manual override
                if match_id in match_minutes:
                    minute = match_minutes[match_id]

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
                                else:
                                    away_win = price
                            conn.run("INSERT INTO odds_snapshots (match_id, home_team, away_team, sport, bookmaker, market, outcome, price, prev_price, price_held_seconds, match_minute, match_score) VALUES (:a, :b, :c, :d, :e, :f, :g, :h, :i, :j, :k, :l)", a=match_id, b=home, c=away, d=sport, e=bname, f=mkey, g=oname, h=price, i=prev_price, j=held_seconds, k=minute, l=current_score)

                if over_price:
                    dur = 0
                    key_over = f"{match_id}_totals_Over"
                    if key_over in last_prices:
                        dur = int(time.time() - last_prices[key_over]["since"])
                    sigs = run_engine(match_id, home, away, over_price, draw_price, home_win, minute, dur)
                    for s in sigs:
                        conn.run("INSERT INTO signals (match_id, home_team, away_team, rule_name, rule_number, confidence, verdict, over_price, draw_price, match_minute) VALUES (:a, :b, :c, :d, :e, :f, :g, :h, :i, :j)", a=match_id, b=home, c=away, d=s["name"], e=s["rule"], f=s["confidence"], g=s["verdict"], h=over_price, i=draw_price, j=minute)

                    # AI analysis for significant signals
                    if sigs and ANTHROPIC_API_KEY:
                        analysis = get_ai_analysis(home, away, over_price, draw_price, home_win, away_win, minute, sigs)
                        if analysis:
                            conn.run("INSERT INTO ai_analyses (match_id, home_team, away_team, over_price, draw_price, match_minute, analysis) VALUES (:a, :b, :c, :d, :e, :f, :g)", a=match_id, b=home, c=away, d=over_price, e=draw_price, f=minute, g=analysis)
                            log.info(f"🤖 AI analysis saved for {home} vs {away}")

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
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"><title>PapaGoal ⚽</title>
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
.scard{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:16px;}
.scard.green{border-color:var(--green)44}.scard.red{border-color:var(--red)44}.scard.yellow{border-color:var(--yellow)44}.scard.orange{border-color:var(--orange)44}
.scard-top{display:flex;align-items:center;gap:12px}
.rb{width:44px;height:44px;border-radius:10px;display:flex;align-items:center;justify-content:center;font-family:'IBM Plex Mono',monospace;font-weight:700;font-size:13px;flex-shrink:0}
.rb.green{background:var(--green)22;color:var(--green)}.rb.red{background:var(--red)22;color:var(--red)}.rb.yellow{background:var(--yellow)22;color:var(--yellow)}.rb.orange{background:var(--orange)22;color:var(--orange)}
.sm{font-size:15px;font-weight:700}.srn{font-size:12px;color:var(--muted);margin-top:2px}
.or{display:flex;gap:8px;margin-top:6px;font-family:'IBM Plex Mono',monospace;font-size:12px;flex-wrap:wrap}.ot{background:#ffffff0a;border-radius:4px;padding:2px 8px}
.vb{padding:6px 14px;border-radius:8px;font-size:12px;font-weight:700;letter-spacing:1px;white-space:nowrap;margin-right:auto}
.vb.green{background:var(--green)22;color:var(--green);border:1px solid var(--green)44}.vb.red{background:var(--red)22;color:var(--red);border:1px solid var(--red)44}
.vb.yellow{background:var(--yellow)22;color:var(--yellow);border:1px solid var(--yellow)44}.vb.orange{background:var(--orange)22;color:var(--orange);border:1px solid var(--orange)44}
.ai-box{margin-top:12px;padding:12px;background:#ffffff05;border-radius:8px;border:1px solid #ffffff11;font-size:13px;line-height:1.6;color:#aaa}
.ai-label{font-size:10px;letter-spacing:2px;color:var(--blue);margin-bottom:6px}
.minute-form{display:flex;gap:8px;margin-top:8px;align-items:center}
.minute-input{background:#ffffff0a;border:1px solid var(--border);border-radius:6px;color:var(--text);padding:4px 8px;width:70px;font-size:13px;text-align:center}
.minute-btn{background:var(--blue)22;border:1px solid var(--blue)44;color:var(--blue);border-radius:6px;padding:4px 12px;cursor:pointer;font-size:12px}
.tw{background:var(--card);border:1px solid var(--border);border-radius:12px;overflow:hidden;margin-bottom:32px}
table{width:100%;border-collapse:collapse;font-size:13px}th{background:#0f0f2a;padding:10px 12px;text-align:right;font-size:11px;color:var(--muted);font-weight:400}
td{padding:10px 12px;border-top:1px solid var(--border)88}.empty{text-align:center;padding:40px;color:var(--muted)}
.pu{color:var(--red)}.pd{color:var(--green)}
.goal-section{background:var(--card);border:1px solid #ff335544;border-radius:12px;padding:20px;margin-bottom:32px}
.goal-form{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-top:12px}
.finput{background:#ffffff0a;border:1px solid var(--border);border-radius:8px;color:var(--text);padding:8px 12px;font-size:14px;width:100%}
.goal-btn{grid-column:1/-1;background:var(--red)22;border:1px solid var(--red)44;color:var(--red);border-radius:8px;padding:12px;cursor:pointer;font-size:15px;font-weight:700;letter-spacing:1px}
.goal-btn:hover{background:var(--red)44}
@keyframes blink{0%,100%{opacity:1}50%{opacity:0.3}}
@media(max-width:600px){.stats{grid-template-columns:repeat(2,1fr)}.goal-form{grid-template-columns:1fr}}
</style></head><body>
<header><div class="logo">PAPA<span>GOAL</span></div><div class="live"><div class="dot"></div>LIVE</div><div class="upd" id="upd">מתעדכן...</div></header>
<div class="wrap">
<div class="stats">
<div class="sc"><div class="sn" style="color:var(--blue)" id="g">—</div><div class="sl">משחקים פעילים</div></div>
<div class="sc"><div class="sn" style="color:var(--green)" id="s">—</div><div class="sl">אותות היום</div></div>
<div class="sc"><div class="sn" style="color:var(--yellow)" id="d">—</div><div class="sl">דגימות נשמרו</div></div>
<div class="sc"><div class="sn" style="color:var(--orange)" id="gl">—</div><div class="sl">גולים מוקלטים</div></div>
</div>

<div class="st">⚽ רישום גול ידני</div>
<div class="goal-section">
  <div style="font-size:13px;color:var(--muted)">כשנכנס גול – רשום אותו כאן לניתוח עתידי</div>
  <div class="goal-form">
    <input class="finput" id="gMatch" placeholder="משחק (לדוגמה: Al-Shabab vs Al-Fateh)">
    <input class="finput" id="gMinute" type="number" placeholder="דקה">
    <input class="finput" id="gScore" placeholder="תוצאה (לדוגמה: 1-0)">
    <input class="finput" id="gNotes" placeholder="הערות (אופציונלי)">
    <button class="goal-btn" onclick="recordGoal()">⚽ רשום גול!</button>
  </div>
</div>

<div class="st">🔥 אותות פעילים – PapaGoal Engine</div>
<div class="sg" id="sg"><div class="empty">📡 אוסף נתונים...</div></div>

<div class="st">📊 יחסים אחרונים</div>
<div class="tw"><table>
<thead><tr><th>משחק</th><th>שוק</th><th>תוצאה</th><th>יחס</th><th>שינוי</th><th>החזיק</th><th>דקה</th><th>תוצאה</th></tr></thead>
<tbody id="ob"><tr><td colspan="8" class="empty">טוען...</td></tr></tbody>
</table></div>
</div>

<script>
const cm={1:'yellow',2:'orange',3:'red',4:'green',5:'red',6:'green',7:'green',8:'red',11:'green',12:'green',14:'green',15:'green'};
let matchData = {};

async function load(){
try{
const[st,si,od,ai]=await Promise.all([
  fetch('/api/stats').then(r=>r.json()),
  fetch('/api/signals').then(r=>r.json()),
  fetch('/api/odds').then(r=>r.json()),
  fetch('/api/ai').then(r=>r.json())
]);
document.getElementById('g').textContent=st.games||0;
document.getElementById('s').textContent=st.signals_today||0;
document.getElementById('d').textContent=st.snapshots||0;
document.getElementById('gl').textContent=st.goals||0;
document.getElementById('upd').textContent='עדכון: '+new Date().toLocaleTimeString('he-IL');

// Build AI lookup
const aiMap = {};
ai.forEach(a => aiMap[a.match_id] = a.analysis);

const sg=document.getElementById('sg');
if(!si.length){sg.innerHTML='<div class="empty">✅ אין אותות פעילים כרגע</div>';}
else{
  sg.innerHTML=si.map(s=>{
    const c=cm[s.rule_number]||'yellow';
    const aiText = aiMap[s.match_id] ? `<div class="ai-box"><div class="ai-label">🤖 CLAUDE AI</div>${aiMap[s.match_id]}</div>` : '';
    return`<div class="scard ${c}">
      <div class="scard-top">
        <div class="rb ${c}">R${s.rule_number}</div>
        <div style="flex:1">
          <div class="sm">${s.home_team} vs ${s.away_team}</div>
          <div class="srn">${s.rule_name}</div>
          <div class="or">
            <span class="ot">Over: ${s.over_price||'—'}</span>
            ${s.draw_price?'<span class="ot">Draw: '+s.draw_price+'</span>':''}
            ${s.match_minute>0?'<span class="ot">⏱ '+s.match_minute+"'</span>":''}
          </div>
        </div>
        <div class="vb ${c}">${s.verdict}</div>
      </div>
      <div class="minute-form">
        <span style="font-size:12px;color:var(--muted)">עדכן דקה:</span>
        <input class="minute-input" type="number" id="min_${s.match_id}" placeholder="דקה" value="${s.match_minute||''}">
        <button class="minute-btn" onclick="setMinute('${s.match_id}', document.getElementById('min_${s.match_id}').value)">✓</button>
      </div>
      ${aiText}
    </div>`;
  }).join('');}

const ob=document.getElementById('ob');
if(!od.length){ob.innerHTML='<tr><td colspan="8" class="empty">אין נתונים עדיין</td></tr>';}
else{ob.innerHTML=od.map(o=>{
  const diff=o.prev_price?(o.price-o.prev_price).toFixed(2):null;
  const dc=!diff?'':(parseFloat(diff)>0?'pu':'pd');
  const dt=!diff?'—':(parseFloat(diff)>0?'▲ '+diff:'▼ '+Math.abs(diff));
  const h=o.price_held_seconds>0?Math.floor(o.price_held_seconds/60)+'m '+o.price_held_seconds%60+'s':'—';
  return`<tr><td>${o.home_team} vs ${o.away_team}</td><td>${o.market}</td><td>${o.outcome}</td><td><b>${o.price}</b></td><td class="${dc}">${dt}</td><td>${h}</td><td>${o.match_minute>0?o.match_minute+"'":'-'}</td><td>${o.match_score||'-'}</td></tr>`;
}).join('');}
}catch(e){console.error(e);}
}

async function setMinute(matchId, minute) {
  await fetch('/api/set_minute', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({match_id: matchId, minute: parseInt(minute)||0})});
  load();
}

async function recordGoal() {
  const match = document.getElementById('gMatch').value;
  const minute = document.getElementById('gMinute').value;
  const score = document.getElementById('gScore').value;
  const notes = document.getElementById('gNotes').value;
  if (!match || !minute) { alert('נא למלא משחק ודקה'); return; }
  await fetch('/api/goal', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({match, minute: parseInt(minute), score, notes})});
  document.getElementById('gMatch').value='';
  document.getElementById('gMinute').value='';
  document.getElementById('gScore').value='';
  document.getElementById('gNotes').value='';
  alert('✅ גול נרשם!');
  load();
}

load();
setInterval(load, 15000);
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
        return jsonify({"games": 0, "signals_today": 0, "snapshots": 0, "goals": 0})

@app.route("/api/signals")
def api_signals():
    try:
        conn = get_db()
        try:
            rows = conn.run("SELECT id, detected_at, match_id, home_team, away_team, rule_name, rule_number, confidence, verdict, over_price, draw_price, match_minute FROM signals WHERE detected_at > NOW() - INTERVAL '30 minutes' ORDER BY detected_at DESC LIMIT 20")
            cols = ["id","detected_at","match_id","home_team","away_team","rule_name","rule_number","confidence","verdict","over_price","draw_price","match_minute"]
            result = [dict(zip(cols, r)) for r in rows]
            for r in result:
                r["detected_at"] = str(r["detected_at"])
                r["color"] = {1:"yellow",2:"orange",3:"red",4:"green",5:"red",6:"green",7:"green",8:"red",11:"green",12:"green",14:"green",15:"green"}.get(r["rule_number"],"yellow")
            return jsonify(result)
        finally:
            conn.close()
    except Exception as e:
        return jsonify([])

@app.route("/api/odds")
def api_odds():
    try:
        conn = get_db()
        try:
            rows = conn.run("SELECT DISTINCT ON (match_id, market, outcome) match_id, home_team, away_team, market, outcome, price, prev_price, price_held_seconds, captured_at, match_minute, match_score FROM odds_snapshots WHERE captured_at > NOW() - INTERVAL '2 minutes' ORDER BY match_id, market, outcome, captured_at DESC LIMIT 100")
            cols = ["match_id","home_team","away_team","market","outcome","price","prev_price","price_held_seconds","captured_at","match_minute","match_score"]
            result = [dict(zip(cols, r)) for r in rows]
            for r in result:
                r["captured_at"] = str(r["captured_at"])
            return jsonify(result)
        finally:
            conn.close()
    except Exception as e:
        return jsonify([])

@app.route("/api/ai")
def api_ai():
    try:
        conn = get_db()
        try:
            rows = conn.run("SELECT match_id, home_team, away_team, over_price, draw_price, match_minute, analysis FROM ai_analyses WHERE analyzed_at > NOW() - INTERVAL '30 minutes' ORDER BY analyzed_at DESC LIMIT 20")
            cols = ["match_id","home_team","away_team","over_price","draw_price","match_minute","analysis"]
            return jsonify([dict(zip(cols, r)) for r in rows])
        finally:
            conn.close()
    except Exception as e:
        return jsonify([])

@app.route("/api/set_minute", methods=["POST"])
def api_set_minute():
    data = request.json or {}
    match_id = data.get("match_id")
    minute = int(data.get("minute", 0))
    if match_id:
        match_minutes[match_id] = minute
        log.info(f"⏱ Manual minute set: {match_id} = {minute}'")
    return jsonify({"status": "ok"})

@app.route("/api/goal", methods=["POST"])
def api_goal():
    data = request.json or {}
    try:
        conn = get_db()
        try:
            # Get last odds
            match_text = data.get("match", "")
            parts = match_text.split(" vs ")
            home = parts[0] if parts else match_text
            away = parts[1] if len(parts) > 1 else ""
            r30 = conn.run("SELECT price FROM odds_snapshots WHERE home_team=:a AND market='totals' AND outcome='Over' AND captured_at < NOW() - INTERVAL '30 seconds' ORDER BY captured_at DESC LIMIT 1", a=home)
            r60 = conn.run("SELECT price FROM odds_snapshots WHERE home_team=:a AND market='totals' AND outcome='Over' AND captured_at < NOW() - INTERVAL '60 seconds' ORDER BY captured_at DESC LIMIT 1", a=home)
            conn.run("INSERT INTO goals (home_team, away_team, match_minute, match_score, over_price_30s, over_price_60s, notes) VALUES (:a, :b, :c, :d, :e, :f, :g)",
                a=home, b=away, c=data.get("minute", 0), d=data.get("score", ""),
                e=r30[0][0] if r30 else None, f=r60[0][0] if r60 else None, g=data.get("notes", ""))
        finally:
            conn.close()
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/health")
def health():
    return jsonify({"status": "ok", "time": datetime.now(timezone.utc).isoformat()})

# Auto-start
init_db()
_t = threading.Thread(target=collector_loop, daemon=True)
_t.start()
log.info("📡 Collector started")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, debug=False)
