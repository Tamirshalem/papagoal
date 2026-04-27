import os
import time
import json
import logging
import threading
from datetime import datetime, timezone
from flask import Flask, jsonify, render_template_string
import pg8000.native
import requests

# ─── Config ───────────────────────────────────────────────────────────────────
ODDS_API_KEY = os.environ.get("ODDS_API_KEY", "")
DATABASE_URL = os.environ.get("DATABASE_URL", "")
PORT = int(os.environ.get("PORT", 8080))
POLL_INTERVAL = 30  # seconds

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("papagoal")

app = Flask(__name__)

# ─── Database ─────────────────────────────────────────────────────────────────
def get_db():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)

def init_db():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS odds_snapshots (
                    id SERIAL PRIMARY KEY,
                    captured_at TIMESTAMPTZ DEFAULT NOW(),
                    match_id TEXT,
                    home_team TEXT,
                    away_team TEXT,
                    sport TEXT,
                    commence_time TIMESTAMPTZ,
                    bookmaker TEXT,
                    market TEXT,
                    outcome TEXT,
                    price FLOAT,
                    prev_price FLOAT,
                    price_held_seconds INT DEFAULT 0
                );
                CREATE INDEX IF NOT EXISTS idx_match_id ON odds_snapshots(match_id);
                CREATE INDEX IF NOT EXISTS idx_captured_at ON odds_snapshots(captured_at);

                CREATE TABLE IF NOT EXISTS goals (
                    id SERIAL PRIMARY KEY,
                    recorded_at TIMESTAMPTZ DEFAULT NOW(),
                    match_id TEXT,
                    home_team TEXT,
                    away_team TEXT,
                    match_minute INT,
                    score TEXT,
                    over_price_30s FLOAT,
                    over_price_60s FLOAT,
                    over_market TEXT,
                    notes TEXT
                );

                CREATE TABLE IF NOT EXISTS signals (
                    id SERIAL PRIMARY KEY,
                    detected_at TIMESTAMPTZ DEFAULT NOW(),
                    match_id TEXT,
                    home_team TEXT,
                    away_team TEXT,
                    rule_name TEXT,
                    rule_number INT,
                    confidence INT,
                    verdict TEXT,
                    over_price FLOAT,
                    draw_price FLOAT,
                    details TEXT
                );
            """)
        conn.commit()
    log.info("✅ Database initialized")

# ─── PapaGoal Engine ──────────────────────────────────────────────────────────
def run_engine(match_id, home, away, over, draw, home_win, away_win, minute, duration_seconds):
    signals = []
    o = over or 0
    d = draw or 0
    hw = home_win or 0
    aw = away_win or 0
    m = minute or 0
    dur = duration_seconds or 0

    # Rule 1: Early Draw Signal (21-25)
    if 21 <= m <= 25 and 1.57 <= d <= 1.66 and 1.83 <= o <= 2.10:
        signals.append({"rule": 1, "name": "Early Draw Signal", "confidence": 75, "verdict": "DRAW or UNDER", "color": "yellow"})

    # Rule 2: Frozen Over (26-30)
    if 26 <= m <= 30 and 1.80 <= o <= 1.86 and 1.58 <= d <= 1.64:
        signals.append({"rule": 2, "name": "Frozen Over", "confidence": 70, "verdict": "NO ENTRY / UNDER", "color": "orange"})

    # Rule 3: Two Early Goals Trap
    if 1.66 <= o <= 1.75:
        signals.append({"rule": 3, "name": "Two Early Goals Trap", "confidence": 72, "verdict": "UNDER / NO MORE GOALS", "color": "red"})

    # Rule 4: Over 2.10 Value (30-34)
    if 30 <= m <= 34 and o >= 2.10:
        signals.append({"rule": 4, "name": "Over 2.10 = Value", "confidence": 78, "verdict": "GOAL ENTRY", "color": "green"})

    # Rule 5: 1.66 Trap
    if 1.63 <= o <= 1.69:
        signals.append({"rule": 5, "name": "1.66 Trap", "confidence": 80, "verdict": "DO NOT ENTER", "color": "red"})

    # Rule 6: Pair Signal 1.61+1.90
    if 1.58 <= d <= 1.64 and 1.87 <= o <= 1.93:
        signals.append({"rule": 6, "name": "Pair Signal 1.61+1.90", "confidence": 83, "verdict": "OVER / GOAL", "color": "green"})

    # Rule 7: 3rd Goal Moment (65-70)
    if 65 <= m <= 70 and o >= 2.15:
        signals.append({"rule": 7, "name": "3rd Goal Moment", "confidence": 76, "verdict": "GOAL ENTRY", "color": "green"})

    # Rule 8: Market Shut (82+)
    if m >= 82 and o >= 2.80:
        signals.append({"rule": 8, "name": "Market Shut", "confidence": 88, "verdict": "NO GOAL", "color": "red"})

    # Rule 9: 1.36 Safe Exit (82+)
    if m >= 82 and 1.30 <= o <= 1.40:
        signals.append({"rule": 9, "name": "Safe Exit 1.36", "confidence": 85, "verdict": "LOCK NO GOAL", "color": "red"})

    # Rule 10: 1.40 Win Odds (74+)
    if m >= 74 and 1.35 <= hw <= 1.45:
        signals.append({"rule": 10, "name": "1.40 Win = Game Open", "confidence": 72, "verdict": "ANOTHER GOAL", "color": "yellow"})

    # Rule 11: Early Drop Signal (17-20)
    if 17 <= m <= 20 and o <= 1.55:
        signals.append({"rule": 11, "name": "Early Drop Signal", "confidence": 86, "verdict": "GOAL VERY SOON", "color": "green"})

    # Rule 12: Opening 1.30 Rule
    if m <= 15 and (hw <= 1.32 or aw <= 1.32):
        signals.append({"rule": 12, "name": "Opening 1.30 Rule", "confidence": 88, "verdict": "EARLY GOAL", "color": "green"})

    # Rule 14: 1.57 Entry Point
    if 1.54 <= o <= 1.60:
        signals.append({"rule": 14, "name": "1.57 Entry Point", "confidence": 79, "verdict": "ENTRY", "color": "green"})

    # Rule 15: Duration Rule
    if dur > 0:
        if 2.30 <= o <= 2.70 and dur >= 120:
            signals.append({"rule": 15, "name": "Duration Rule – HELD 2min+", "confidence": 82, "verdict": "POSSIBLE GOAL", "color": "green"})
        elif 2.30 <= o <= 2.70 and dur <= 30:
            signals.append({"rule": 15, "name": "Duration Rule – REJECTED 30s", "confidence": 80, "verdict": "NO GOAL", "color": "red"})

    return signals

# ─── Odds Collector ───────────────────────────────────────────────────────────
last_prices = {}  # match_id+market+outcome -> {price, since}

def collect_odds():
    try:
        url = "https://api.the-odds-api.com/v4/sports/soccer/odds/"
        params = {
            "apiKey": ODDS_API_KEY,
            "regions": "eu",
            "markets": "h2h,totals",
            "oddsFormat": "decimal",
            "dateFormat": "iso"
        }
        resp = requests.get(url, params=params, timeout=15)
        if resp.status_code != 200:
            log.warning(f"API error: {resp.status_code}")
            return

        games = resp.json()
        log.info(f"📡 Fetched {len(games)} games")

        with get_db() as conn:
            with conn.cursor() as cur:
                for game in games:
                    match_id = game["id"]
                    home = game["home_team"]
                    away = game["away_team"]
                    sport = game["sport_key"]
                    commence = game["commence_time"]

                    over_price = None
                    draw_price = None
                    home_win = None
                    away_win = None

                    for bookmaker in game.get("bookmakers", [])[:1]:
                        bname = bookmaker["key"]
                        for market in bookmaker.get("markets", []):
                            mkey = market["key"]
                            for outcome in market.get("outcomes", []):
                                oname = outcome["name"]
                                price = outcome["price"]
                                key = f"{match_id}_{mkey}_{oname}"
                                now = time.time()

                                # Duration tracking
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

                                if held_seconds == 0 and key in last_prices:
                                    held_seconds = int(now - last_prices[key]["since"])

                                # Capture key prices
                                if mkey == "totals" and oname == "Over":
                                    over_price = price
                                if mkey == "h2h":
                                    if oname == "Draw":
                                        draw_price = price
                                    elif oname == home:
                                        home_win = price
                                    else:
                                        away_win = price

                                cur.execute("""
                                    INSERT INTO odds_snapshots
                                    (match_id, home_team, away_team, sport, commence_time,
                                     bookmaker, market, outcome, price, prev_price, price_held_seconds)
                                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                                """, (match_id, home, away, sport, commence,
                                      bname, mkey, oname, price, prev_price, held_seconds))

                    # Run engine
                    if over_price:
                        dur = 0
                        key_over = f"{match_id}_totals_Over"
                        if key_over in last_prices:
                            dur = int(time.time() - last_prices[key_over]["since"])

                        sigs = run_engine(match_id, home, away,
                                          over_price, draw_price, home_win, away_win,
                                          0, dur)
                        for s in sigs:
                            cur.execute("""
                                INSERT INTO signals
                                (match_id, home_team, away_team, rule_name, rule_number,
                                 confidence, verdict, over_price, draw_price, details)
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            """, (match_id, home, away, s["name"], s["rule"],
                                  s["confidence"], s["verdict"], over_price, draw_price,
                                  json.dumps(s)))

            conn.commit()

    except Exception as e:
        log.error(f"Collect error: {e}")

def collector_loop():
    while True:
        collect_odds()
        time.sleep(POLL_INTERVAL)

# ─── Dashboard HTML ───────────────────────────────────────────────────────────
DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="he" dir="rtl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>PapaGoal ⚽</title>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600;700&family=Heebo:wght@300;400;700;900&display=swap" rel="stylesheet">
<style>
  :root {
    --bg: #04040f;
    --card: #0a0a1e;
    --border: #1a1a3a;
    --green: #00ff88;
    --red: #ff3355;
    --yellow: #ffcc00;
    --orange: #ff6b35;
    --blue: #00cfff;
    --text: #e0e0ff;
    --muted: #555577;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: var(--bg); color: var(--text); font-family: 'Heebo', sans-serif; min-height: 100vh; }
  
  header {
    background: linear-gradient(90deg, #000010, #0a0a2e);
    border-bottom: 1px solid var(--border);
    padding: 16px 24px;
    display: flex;
    align-items: center;
    gap: 16px;
    position: sticky;
    top: 0;
    z-index: 100;
  }
  .logo { font-size: 28px; font-family: 'IBM Plex Mono', monospace; font-weight: 700; color: #fff; letter-spacing: 3px; }
  .logo span { color: var(--green); }
  .status-dot { width: 10px; height: 10px; border-radius: 50%; background: var(--green); animation: blink 1s infinite; margin-right: 6px; }
  .live-badge { display: flex; align-items: center; font-size: 11px; color: var(--green); letter-spacing: 2px; margin-right: auto; }
  .last-update { font-size: 11px; color: var(--muted); font-family: 'IBM Plex Mono', monospace; }

  .container { max-width: 1200px; margin: 0 auto; padding: 24px 16px; }
  
  .stats-row { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 24px; }
  .stat-card { background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 16px; text-align: center; }
  .stat-num { font-size: 32px; font-weight: 900; font-family: 'IBM Plex Mono', monospace; }
  .stat-label { font-size: 11px; color: var(--muted); letter-spacing: 1px; margin-top: 4px; }

  .section-title { font-size: 12px; letter-spacing: 3px; color: var(--muted); text-transform: uppercase; margin-bottom: 12px; padding-bottom: 8px; border-bottom: 1px solid var(--border); }

  .signals-grid { display: grid; gap: 12px; margin-bottom: 32px; }
  .signal-card {
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 16px;
    display: grid;
    grid-template-columns: auto 1fr auto;
    gap: 16px;
    align-items: center;
    transition: border-color 0.3s;
  }
  .signal-card.green { border-color: var(--green)44; }
  .signal-card.red { border-color: var(--red)44; }
  .signal-card.yellow { border-color: var(--yellow)44; }
  .signal-card.orange { border-color: var(--orange)44; }

  .rule-badge { width: 44px; height: 44px; border-radius: 10px; display: flex; align-items: center; justify-content: center; font-family: 'IBM Plex Mono', monospace; font-weight: 700; font-size: 13px; flex-shrink: 0; }
  .rule-badge.green { background: var(--green)22; color: var(--green); }
  .rule-badge.red { background: var(--red)22; color: var(--red); }
  .rule-badge.yellow { background: var(--yellow)22; color: var(--yellow); }
  .rule-badge.orange { background: var(--orange)22; color: var(--orange); }

  .signal-info .match { font-size: 15px; font-weight: 700; }
  .signal-info .rule-name { font-size: 12px; color: var(--muted); margin-top: 2px; }
  .signal-info .odds-row { display: flex; gap: 12px; margin-top: 6px; font-family: 'IBM Plex Mono', monospace; font-size: 12px; }
  .odds-tag { background: #ffffff0a; border-radius: 4px; padding: 2px 8px; }

  .verdict-badge { padding: 6px 14px; border-radius: 8px; font-size: 12px; font-weight: 700; letter-spacing: 1px; white-space: nowrap; }
  .verdict-badge.green { background: var(--green)22; color: var(--green); border: 1px solid var(--green)44; }
  .verdict-badge.red { background: var(--red)22; color: var(--red); border: 1px solid var(--red)44; }
  .verdict-badge.yellow { background: var(--yellow)22; color: var(--yellow); border: 1px solid var(--yellow)44; }
  .verdict-badge.orange { background: var(--orange)22; color: var(--orange); border: 1px solid var(--orange)44; }

  .conf-bar { width: 80px; height: 4px; background: #1a1a3a; border-radius: 2px; margin-top: 4px; }
  .conf-fill { height: 100%; border-radius: 2px; }

  .odds-table-wrap { background: var(--card); border: 1px solid var(--border); border-radius: 12px; overflow: hidden; margin-bottom: 32px; }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th { background: #0f0f2a; padding: 12px 16px; text-align: right; font-size: 11px; letter-spacing: 1px; color: var(--muted); font-weight: 400; }
  td { padding: 12px 16px; border-top: 1px solid var(--border)88; }
  tr:hover td { background: #ffffff03; }
  .price-up { color: var(--red); }
  .price-down { color: var(--green); }
  .price-same { color: var(--muted); }

  .empty { text-align: center; padding: 48px; color: var(--muted); font-size: 14px; }
  .empty .icon { font-size: 48px; margin-bottom: 12px; }

  @keyframes blink { 0%,100%{opacity:1} 50%{opacity:0.3} }
  @media (max-width: 600px) {
    .stats-row { grid-template-columns: repeat(2, 1fr); }
    .signal-card { grid-template-columns: auto 1fr; }
    .verdict-badge { display: none; }
  }
</style>
</head>
<body>

<header>
  <div class="logo">PAPA<span>GOAL</span></div>
  <div class="live-badge"><div class="status-dot"></div>LIVE</div>
  <div class="last-update" id="lastUpdate">מתעדכן...</div>
</header>

<div class="container">

  <div class="stats-row">
    <div class="stat-card">
      <div class="stat-num" style="color:var(--blue)" id="statGames">—</div>
      <div class="stat-label">משחקים פעילים</div>
    </div>
    <div class="stat-card">
      <div class="stat-num" style="color:var(--green)" id="statSignals">—</div>
      <div class="stat-label">אותות היום</div>
    </div>
    <div class="stat-card">
      <div class="stat-num" style="color:var(--yellow)" id="statSnapshots">—</div>
      <div class="stat-label">דגימות נשמרו</div>
    </div>
    <div class="stat-card">
      <div class="stat-num" style="color:var(--orange)" id="statGoals">—</div>
      <div class="stat-label">גולים מוקלטים</div>
    </div>
  </div>

  <div class="section-title">🔥 אותות פעילים – PapaGoal Engine</div>
  <div class="signals-grid" id="signalsGrid">
    <div class="empty"><div class="icon">📡</div>אוסף נתונים... חזור בעוד 30 שניות</div>
  </div>

  <div class="section-title">📊 יחסים אחרונים</div>
  <div class="odds-table-wrap">
    <table>
      <thead>
        <tr>
          <th>משחק</th>
          <th>שוק</th>
          <th>תוצאה</th>
          <th>יחס</th>
          <th>שינוי</th>
          <th>החזיק</th>
          <th>זמן</th>
        </tr>
      </thead>
      <tbody id="oddsBody">
        <tr><td colspan="7" class="empty">טוען...</td></tr>
      </tbody>
    </table>
  </div>

</div>

<script>
async function load() {
  try {
    const [stats, signals, odds] = await Promise.all([
      fetch('/api/stats').then(r=>r.json()),
      fetch('/api/signals').then(r=>r.json()),
      fetch('/api/odds').then(r=>r.json())
    ]);

    document.getElementById('statGames').textContent = stats.games || 0;
    document.getElementById('statSignals').textContent = stats.signals_today || 0;
    document.getElementById('statSnapshots').textContent = stats.snapshots || 0;
    document.getElementById('statGoals').textContent = stats.goals || 0;
    document.getElementById('lastUpdate').textContent = 'עדכון: ' + new Date().toLocaleTimeString('he-IL');

    // Signals
    const sg = document.getElementById('signalsGrid');
    if (signals.length === 0) {
      sg.innerHTML = '<div class="empty"><div class="icon">✅</div>אין אותות פעילים כרגע</div>';
    } else {
      sg.innerHTML = signals.map(s => `
        <div class="signal-card ${s.color}">
          <div class="rule-badge ${s.color}">R${s.rule_number}</div>
          <div class="signal-info">
            <div class="match">${s.home_team} vs ${s.away_team}</div>
            <div class="rule-name">${s.rule_name}</div>
            <div class="odds-row">
              <span class="odds-tag">Over: ${s.over_price || '—'}</span>
              ${s.draw_price ? `<span class="odds-tag">Draw: ${s.draw_price}</span>` : ''}
              <span class="odds-tag">${new Date(s.detected_at).toLocaleTimeString('he-IL')}</span>
            </div>
            <div class="conf-bar"><div class="conf-fill" style="width:${s.confidence}%;background:var(--${s.color})"></div></div>
          </div>
          <div class="verdict-badge ${s.color}">${s.verdict}</div>
        </div>
      `).join('');
    }

    // Odds table
    const ob = document.getElementById('oddsBody');
    if (odds.length === 0) {
      ob.innerHTML = '<tr><td colspan="7" class="empty">אין נתונים עדיין</td></tr>';
    } else {
      ob.innerHTML = odds.map(o => {
        const diff = o.prev_price ? (o.price - o.prev_price).toFixed(2) : null;
        const diffClass = !diff ? 'price-same' : diff > 0 ? 'price-up' : 'price-down';
        const diffText = !diff ? '—' : (diff > 0 ? '▲ '+diff : '▼ '+Math.abs(diff));
        const held = o.price_held_seconds > 0 ? Math.floor(o.price_held_seconds/60)+'m '+o.price_held_seconds%60+'s' : '—';
        return `<tr>
          <td>${o.home_team} vs ${o.away_team}</td>
          <td>${o.market}</td>
          <td>${o.outcome}</td>
          <td><b>${o.price}</b></td>
          <td class="${diffClass}">${diffText}</td>
          <td>${held}</td>
          <td>${new Date(o.captured_at).toLocaleTimeString('he-IL')}</td>
        </tr>`;
      }).join('');
    }
  } catch(e) { console.error(e); }
}

load();
setInterval(load, 15000);
</script>
</body>
</html>
"""

# ─── API Routes ───────────────────────────────────────────────────────────────
@app.route("/")
def dashboard():
    return render_template_string(DASHBOARD_HTML)

@app.route("/api/stats")
def api_stats():
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(DISTINCT match_id) FROM odds_snapshots WHERE captured_at > NOW() - INTERVAL '1 hour'")
                games = cur.fetchone()["count"]
                cur.execute("SELECT COUNT(*) FROM signals WHERE detected_at > NOW() - INTERVAL '24 hours'")
                signals_today = cur.fetchone()["count"]
                cur.execute("SELECT COUNT(*) FROM odds_snapshots")
                snapshots = cur.fetchone()["count"]
                cur.execute("SELECT COUNT(*) FROM goals")
                goals = cur.fetchone()["count"]
        return jsonify({"games": games, "signals_today": signals_today, "snapshots": snapshots, "goals": goals})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/signals")
def api_signals():
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT s.*, 
                           CASE rule_number
                             WHEN 1 THEN 'yellow' WHEN 2 THEN 'orange' WHEN 3 THEN 'red'
                             WHEN 4 THEN 'green' WHEN 5 THEN 'red' WHEN 6 THEN 'green'
                             WHEN 7 THEN 'green' WHEN 8 THEN 'red' WHEN 9 THEN 'red'
                             WHEN 10 THEN 'yellow' WHEN 11 THEN 'green' WHEN 12 THEN 'green'
                             WHEN 14 THEN 'green' WHEN 15 THEN 'green'
                             ELSE 'yellow'
                           END as color
                    FROM signals s
                    WHERE detected_at > NOW() - INTERVAL '30 minutes'
                    ORDER BY detected_at DESC LIMIT 20
                """)
                return jsonify([dict(r) for r in cur.fetchall()])
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/odds")
def api_odds():
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT ON (match_id, market, outcome)
                        match_id, home_team, away_team, market, outcome,
                        price, prev_price, price_held_seconds, captured_at
                    FROM odds_snapshots
                    WHERE captured_at > NOW() - INTERVAL '2 minutes'
                    ORDER BY match_id, market, outcome, captured_at DESC
                    LIMIT 100
                """)
                return jsonify([dict(r) for r in cur.fetchall()])
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/goal", methods=["POST"])
def api_record_goal():
    """Record a goal manually"""
    from flask import request
    data = request.json or {}
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                # Get last odds before this moment
                cur.execute("""
                    SELECT price FROM odds_snapshots
                    WHERE match_id = %s AND market = 'totals' AND outcome = 'Over'
                    AND captured_at < NOW() - INTERVAL '30 seconds'
                    ORDER BY captured_at DESC LIMIT 1
                """, (data.get("match_id"),))
                r30 = cur.fetchone()

                cur.execute("""
                    SELECT price FROM odds_snapshots
                    WHERE match_id = %s AND market = 'totals' AND outcome = 'Over'
                    AND captured_at < NOW() - INTERVAL '60 seconds'
                    ORDER BY captured_at DESC LIMIT 1
                """, (data.get("match_id"),))
                r60 = cur.fetchone()

                cur.execute("""
                    INSERT INTO goals (match_id, home_team, away_team, match_minute, score,
                                       over_price_30s, over_price_60s, over_market, notes)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (data.get("match_id"), data.get("home"), data.get("away"),
                      data.get("minute"), data.get("score"),
                      r30["price"] if r30 else None,
                      r60["price"] if r60 else None,
                      data.get("market", "totals"),
                      data.get("notes", "")))
            conn.commit()
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/health")
def health():
    return jsonify({"status": "ok", "time": datetime.now(timezone.utc).isoformat()})

# ─── Start ────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("🚀 PapaGoal starting...")
    init_db()
    t = threading.Thread(target=collector_loop, daemon=True)
    t.start()
    log.info(f"📡 Collector started – polling every {POLL_INTERVAL}s")
    app.run(host="0.0.0.0", port=PORT, debug=False)
