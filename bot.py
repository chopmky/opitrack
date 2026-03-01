import os
import time
import json
import sqlite3
import threading
import requests
from datetime import datetime, date
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# CONFIG
# ============================================================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ADMIN_ID           = int(os.getenv("ADMIN_ID", "0"))

OPINION_TRADE_URL     = "https://openapi.opinion.trade/openapi/trade/user/{wallet}"
OPINION_POSITIONS_URL = "https://openapi.opinion.trade/openapi/positions/user/{wallet}"
TG_BASE               = "https://api.telegram.org/bot{token}/{method}"

POLL_SECONDS      = 5
HEARTBEAT_SECONDS = 3600
DB_PATH           = "opitrack.db"
MAX_WALLETS       = 5


# ============================================================
# DATABASE
# ============================================================

def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                chat_id     INTEGER PRIMARY KEY,
                api_key     TEXT,
                joined_date TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS wallets (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id      INTEGER,
                eoa          TEXT,
                nickname     TEXT,
                last_seen_id TEXT,
                created_at   TEXT,
                UNIQUE(chat_id, eoa)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS daily (
                wallet_id INTEGER,
                date      TEXT,
                total     INTEGER DEFAULT 0,
                volume    REAL    DEFAULT 0,
                markets   TEXT    DEFAULT '[]',
                PRIMARY KEY (wallet_id, date)
            )
        """)
        # Migrate v1 users if needed
        try:
            rows = conn.execute("SELECT chat_id, eoa, last_seen_id FROM users WHERE eoa IS NOT NULL").fetchall()
            for row in rows:
                try:
                    conn.execute("""
                        INSERT OR IGNORE INTO wallets (chat_id, eoa, last_seen_id, created_at)
                        VALUES (?, ?, ?, ?)
                    """, (row["chat_id"], row["eoa"], row["last_seen_id"], str(date.today())))
                except Exception:
                    pass
        except Exception:
            pass

DB_LOCK = threading.Lock()

def db_get_user(chat_id):
    with DB_LOCK:
        with get_db() as conn:
            row = conn.execute("SELECT * FROM users WHERE chat_id=?", (chat_id,)).fetchone()
            return dict(row) if row else None

def db_upsert_user(chat_id, **kwargs):
    with DB_LOCK:
        with get_db() as conn:
            existing = conn.execute("SELECT chat_id FROM users WHERE chat_id=?", (chat_id,)).fetchone()
            if existing:
                if kwargs:
                    sets = ", ".join(f"{k}=?" for k in kwargs)
                    conn.execute(f"UPDATE users SET {sets} WHERE chat_id=?", (*kwargs.values(), chat_id))
            else:
                kwargs["chat_id"] = chat_id
                kwargs.setdefault("joined_date", str(date.today()))
                cols = ", ".join(kwargs.keys())
                placeholders = ", ".join("?" * len(kwargs))
                conn.execute(f"INSERT INTO users ({cols}) VALUES ({placeholders})", list(kwargs.values()))

def db_get_wallets(chat_id):
    with DB_LOCK:
        with get_db() as conn:
            rows = conn.execute(
                "SELECT * FROM wallets WHERE chat_id=? ORDER BY created_at ASC", (chat_id,)
            ).fetchall()
            return [dict(r) for r in rows]

def db_get_wallet(wallet_id):
    with DB_LOCK:
        with get_db() as conn:
            row = conn.execute("SELECT * FROM wallets WHERE id=?", (wallet_id,)).fetchone()
            return dict(row) if row else None

def db_add_wallet(chat_id, eoa, nickname=None):
    with DB_LOCK:
        with get_db() as conn:
            conn.execute("""
                INSERT OR IGNORE INTO wallets (chat_id, eoa, nickname, last_seen_id, created_at)
                VALUES (?, ?, ?, NULL, ?)
            """, (chat_id, eoa, nickname, str(date.today())))
            row = conn.execute("SELECT * FROM wallets WHERE chat_id=? AND eoa=?", (chat_id, eoa)).fetchone()
            return dict(row) if row else None

def db_update_wallet(wallet_id, **kwargs):
    with DB_LOCK:
        with get_db() as conn:
            sets = ", ".join(f"{k}=?" for k in kwargs)
            conn.execute(f"UPDATE wallets SET {sets} WHERE id=?", (*kwargs.values(), wallet_id))

def db_remove_wallet(wallet_id):
    with DB_LOCK:
        with get_db() as conn:
            conn.execute("DELETE FROM wallets WHERE id=?", (wallet_id,))
            conn.execute("DELETE FROM daily WHERE wallet_id=?", (wallet_id,))

def db_count_wallets(chat_id):
    with DB_LOCK:
        with get_db() as conn:
            return conn.execute("SELECT COUNT(*) FROM wallets WHERE chat_id=?", (chat_id,)).fetchone()[0]

def db_get_all_active_wallets():
    with DB_LOCK:
        with get_db() as conn:
            rows = conn.execute("""
                SELECT w.*, u.api_key FROM wallets w
                JOIN users u ON w.chat_id = u.chat_id
                WHERE u.api_key IS NOT NULL
            """).fetchall()
            return [dict(r) for r in rows]

def db_get_all_users_with_wallets():
    with DB_LOCK:
        with get_db() as conn:
            rows = conn.execute("""
                SELECT DISTINCT u.chat_id FROM users u
                JOIN wallets w ON w.chat_id = u.chat_id
                WHERE u.api_key IS NOT NULL
            """).fetchall()
            return [r["chat_id"] for r in rows]

def db_count_users():
    with DB_LOCK:
        with get_db() as conn:
            total   = conn.execute("SELECT COUNT(*) FROM users WHERE api_key IS NOT NULL").fetchone()[0]
            active  = conn.execute("SELECT COUNT(DISTINCT chat_id) FROM wallets").fetchone()[0]
            today   = conn.execute("SELECT COUNT(*) FROM users WHERE joined_date=?", (str(date.today()),)).fetchone()[0]
            return total, active, today

def db_get_daily(wallet_id):
    today = str(date.today())
    with DB_LOCK:
        with get_db() as conn:
            row = conn.execute(
                "SELECT * FROM daily WHERE wallet_id=? AND date=?", (wallet_id, today)
            ).fetchone()
            if row:
                d = dict(row)
                d["markets"] = json.loads(d["markets"])
                return d
            return {"wallet_id": wallet_id, "date": today, "total": 0, "volume": 0.0, "markets": []}

def db_get_daily_by_date(wallet_id, target_date):
    with DB_LOCK:
        with get_db() as conn:
            row = conn.execute(
                "SELECT * FROM daily WHERE wallet_id=? AND date=?", (wallet_id, target_date)
            ).fetchone()
            if row:
                d = dict(row)
                d["markets"] = json.loads(d["markets"])
                return d
            return {"wallet_id": wallet_id, "date": target_date, "total": 0, "volume": 0.0, "markets": []}

def db_add_trade_to_daily(wallet_id, trade):
    daily = db_get_daily(wallet_id)
    daily["total"] += 1
    try:
        daily["volume"] += float(trade.get("amount") or 0)
    except Exception:
        pass
    market = trade.get("rootMarketTitle") or trade.get("marketTitle") or "Unknown"
    if market not in daily["markets"]:
        daily["markets"].append(market)
    with DB_LOCK:
        with get_db() as conn:
            conn.execute("""
                INSERT INTO daily (wallet_id, date, total, volume, markets)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(wallet_id, date) DO UPDATE SET
                    total=excluded.total,
                    volume=excluded.volume,
                    markets=excluded.markets
            """, (wallet_id, daily["date"], daily["total"], daily["volume"], json.dumps(daily["markets"])))
    return daily


# ============================================================
# TELEGRAM HELPERS
# ============================================================

def tg(method, **kwargs):
    url = TG_BASE.format(token=TELEGRAM_BOT_TOKEN, method=method)
    try:
        resp = requests.post(url, json=kwargs, timeout=30)
        return resp.json()
    except Exception as e:
        print("Telegram error:", repr(e))
        return {}

def send_message(chat_id, text, reply_markup=None, parse_mode="Markdown"):
    kwargs = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode}
    if reply_markup:
        kwargs["reply_markup"] = reply_markup
    return tg("sendMessage", **kwargs)

def edit_message(chat_id, message_id, text, reply_markup=None, parse_mode="Markdown"):
    kwargs = {"chat_id": chat_id, "message_id": message_id, "text": text, "parse_mode": parse_mode}
    if reply_markup:
        kwargs["reply_markup"] = reply_markup
    tg("editMessageText", **kwargs)

def answer_callback(callback_query_id):
    tg("answerCallbackQuery", callback_query_id=callback_query_id)


# ============================================================
# MENU HELPERS
# ============================================================

def back_btn():
    return {"inline_keyboard": [[{"text": "Main Menu", "callback_data": "main_menu"}]]}

def wallet_display(w):
    if w.get("nickname"):
        return w["nickname"]
    eoa = w.get("eoa", "")
    return eoa[:6] + "..." + eoa[-4:] if len(eoa) >= 10 else eoa

def short_addr(eoa):
    return eoa[:6] + "..." + eoa[-4:] if len(eoa) >= 10 else eoa

def main_menu_text(user_name, chat_id):
    wallets = db_get_wallets(chat_id)
    name    = user_name or "there"
    count   = len(wallets)
    if count > 0:
        def fmt_wallet(w):
            if w.get("nickname"):
                return f"{w['nickname']} (`{short_addr(w['eoa'])}`)"
            return f"`{w['eoa']}`"
        wallet_lines = "\n".join(f"• {fmt_wallet(w)}" for w in wallets)
        return (
            f"Welcome *{name}* to Opitrack Bot\n\n"
            f"Monitoring: {count}/{MAX_WALLETS} wallets\n"
            f"{wallet_lines}\n"
            f"Status: 🟢 Active"
        )
    else:
        return (
            f"Welcome *{name}* to Opitrack Bot\n\n"
            f"No wallets monitored yet."
        )

def main_menu_markup(chat_id):
    wallets = db_get_wallets(chat_id)
    if wallets:
        return {"inline_keyboard": [
            [{"text": "My Wallets",     "callback_data": "my_wallets"},
             {"text": "Add Wallet",     "callback_data": "add_wallet"}],
            [{"text": "Change API Key", "callback_data": "change_api_key"}],
        ]}
    else:
        return {"inline_keyboard": [
            [{"text": "Add Wallet",     "callback_data": "add_wallet"}],
            [{"text": "Change API Key", "callback_data": "change_api_key"}],
        ]}

def send_main_menu(chat_id, user_name):
    send_message(chat_id, main_menu_text(user_name, chat_id), reply_markup=main_menu_markup(chat_id))

def edit_main_menu(chat_id, message_id, user_name):
    edit_message(chat_id, message_id, main_menu_text(user_name, chat_id), reply_markup=main_menu_markup(chat_id))

def my_wallets_text(chat_id):
    wallets = db_get_wallets(chat_id)
    count   = len(wallets)
    if not wallets:
        return f"📋 *My Wallets (0/{MAX_WALLETS})*\n\nNo wallets added yet."
    lines = [f"📋 *My Wallets ({count}/{MAX_WALLETS})*\n"]
    for w in wallets:
        name = wallet_display(w)
        eoa  = w["eoa"]
        if w.get("nickname"):
            lines.append(f"• {name} (`{short_addr(eoa)}`)")
        else:
            lines.append(f"• `{eoa}`")
    return "\n".join(lines)

def my_wallets_markup(chat_id):
    wallets = db_get_wallets(chat_id)
    buttons = []
    for w in wallets:
        buttons.append([{"text": wallet_display(w), "callback_data": f"wallet~{w['id']}"}])
    row = []
    if len(wallets) < MAX_WALLETS:
        row.append({"text": "+ Add Wallet", "callback_data": "add_wallet"})
    buttons.append(row) if row else None
    buttons.append([{"text": "Main Menu", "callback_data": "main_menu"}])
    return {"inline_keyboard": buttons}

def wallet_detail_text(w):
    name = wallet_display(w)
    return f"*{name}*\n`{w['eoa']}`"

def wallet_detail_markup(wallet_id):
    return {"inline_keyboard": [
        [{"text": "Positions",     "callback_data": f"positions~{wallet_id}"},
         {"text": "Trade History", "callback_data": f"history~{wallet_id}"}],
        [{"text": "Rename",        "callback_data": f"rename~{wallet_id}"},
         {"text": "Remove",        "callback_data": f"remove~{wallet_id}"}],
        [{"text": "Main Menu",     "callback_data": "main_menu"}],
    ]}


# ============================================================
# OPINION API
# ============================================================

def validate_api_key(api_key):
    url = OPINION_TRADE_URL.format(wallet="0x0000000000000000000000000000000000000000")
    try:
        resp = requests.get(url, headers={"apikey": api_key}, timeout=15)
        return resp.status_code != 401
    except Exception:
        return False

def fetch_trades(api_key, wallet):
    url = OPINION_TRADE_URL.format(wallet=wallet)
    last_err = None
    for _ in range(3):
        try:
            resp = requests.get(url, headers={"apikey": api_key}, timeout=45)
            resp.raise_for_status()
            return resp.json().get("result", {}).get("list") or []
        except Exception as e:
            last_err = e
            time.sleep(2)
    raise last_err

def fetch_all_positions(api_key, eoa):
    """Fetch all pages of positions, return list of active positions with shares >= 1."""
    all_positions = []
    page = 1
    while True:
        url = f"{OPINION_POSITIONS_URL.format(wallet=eoa)}?limit=20&page={page}"
        try:
            resp = requests.get(url, headers={"apikey": api_key}, timeout=30)
            resp.raise_for_status()
            result = resp.json().get("result", {})
            items = result.get("list") or []
            if not items:
                break
            all_positions.extend(items)
            # If less than 20 returned, we've reached the last page
            if len(items) < 20:
                break
            page += 1
        except Exception:
            break
    # Filter active with shares
    active = [p for p in all_positions
              if p.get("marketStatusEnum") == "Activated"
              and float(p.get("sharesOwned") or 0) >= 1]
    return active

def fetch_positions_summary(api_key, eoa):
    """Return (total_positions, total_value) for daily summary."""
    try:
        positions = fetch_all_positions(api_key, eoa)
        total = len(positions)
        value = sum(float(p.get("currentValueInQuoteToken") or 0) for p in positions)
        return total, value
    except Exception:
        return None, None

def fetch_positions_text(api_key, eoa, wallet_name):
    url = OPINION_POSITIONS_URL.format(wallet=eoa)
    try:
        resp = requests.get(url, headers={"apikey": api_key}, timeout=30)
        resp.raise_for_status()
        positions = resp.json().get("result", {}).get("list", [])
        if not positions:
            return f"*{wallet_name}*\n\nNo open positions."
        lines = [f"*Open Positions ({len(positions)}) — {wallet_name}*\n"]
        for i, p in enumerate(positions, 1):
            root    = p.get("rootMarketTitle") or p.get("marketTitle") or f"Market {p.get('marketId','?')}"
            sub     = p.get("marketTitle") or ""
            outcome = "YES" if p.get("outcomeSide") == 1 else "NO"
            try:    shares = f"{float(p.get('sharesOwned') or 0):,.4f}"
            except: shares = "?"
            try:    value  = f"${float(p.get('currentValueInQuoteToken') or 0):,.4f}"
            except: value  = "?"
            try:    avg    = f"{float(p.get('avgEntryPrice') or 0):,.4f}c"
            except: avg    = "?"
            try:
                pnl     = float(p.get("unrealizedPnl") or 0)
                pnl_pct = float(p.get("unrealizedPnlPercent") or 0) * 100
                pnl_s   = f"+${pnl:,.4f}" if pnl >= 0 else f"-${abs(pnl):,.4f}"
                pnl_p   = f"+{pnl_pct:.1f}%" if pnl_pct >= 0 else f"{pnl_pct:.1f}%"
            except:
                pnl_s = pnl_p = "?"
            lines.append(f"{i}. *{root}*")
            if sub and sub != root:
                lines.append(f"   {sub}")
            lines.append(f"   {outcome} | Shares: {shares} | Value: {value}")
            lines.append(f"   Avg Cost: {avg} | PnL: {pnl_s} ({pnl_p})\n")
        return "\n".join(lines)
    except Exception:
        return "Could not fetch positions. Please try again later."

def fetch_history_text(api_key, eoa, wallet_name):
    url = OPINION_TRADE_URL.format(wallet=eoa)
    try:
        resp = requests.get(url, headers={"apikey": api_key}, timeout=30)
        resp.raise_for_status()
        trades = resp.json().get("result", {}).get("list") or []
        if not trades:
            return f"*{wallet_name}*\n\nNo trades found."
        trades = trades[:10]
        lines = [f"*Last 10 Trades — {wallet_name}*\n"]
        for i, t in enumerate(trades, 1):
            side    = str(t.get("side", "")).upper()
            outcome = "YES" if str(t.get("outcomeSide", "")) == "1" else "NO"
            root    = t.get("rootMarketTitle") or t.get("marketTitle") or "?"
            root_id = t.get("rootMarketId") or t.get("marketId") or ""
            mkt_id  = t.get("marketId") or ""
            sub     = t.get("marketTitle") or ""
            is_multi = root_id and mkt_id and str(root_id) != str(mkt_id)
            try:    price = f"{float(t.get('price') or 0)*100:.1f}c"
            except: price = "?"
            try:    usd   = f"${float(t.get('amount') or 0):,.2f}"
            except: usd   = "?"
            try:    ts    = datetime.utcfromtimestamp(int(t.get("createdAt") or 0)).strftime("%d/%m %H:%M (UTC)")
            except: ts    = "?"
            if is_multi and sub and sub != root:
                action = f"*{side} {outcome} ({sub})* for {usd} at {price}"
            else:
                action = f"*{side} {outcome}* for {usd} at {price}"
            lines.append(f"{i}. {action}\n   {root[:50]}\n   {ts}\n")
        return "\n".join(lines)
    except Exception:
        return "Could not fetch trade history. Please try again later."


# ============================================================
# TRADE FORMAT
# ============================================================

def pick_id(trade):
    for k in ["txHash", "tradeNo", "createdAt", "id"]:
        v = trade.get(k)
        if v:
            return str(v)
    return str(trade)

def format_trade_alert(wallet_row, t):
    eoa     = wallet_row["eoa"]
    name    = wallet_display(wallet_row)
    side    = str(t.get("side") or "").upper()
    outcome = "YES" if str(t.get("outcomeSide")) == "1" else "NO"
    root    = t.get("rootMarketTitle") or t.get("marketTitle") or "?"
    root_id = t.get("rootMarketId") or t.get("marketId") or ""
    mkt_id  = t.get("marketId") or ""
    sub     = t.get("marketTitle") or ""
    is_multi = root_id and mkt_id and str(root_id) != str(mkt_id)

    if root_id:
        suffix = "&type=multi" if is_multi else ""
        url    = f"https://app.opinion.trade/detail?topicId={root_id}{suffix}"
        link   = f"[{root}]({url})"
    else:
        link = root

    if is_multi and sub and sub != root:
        action = f"*{side} {outcome} ({sub})*"
    else:
        action = f"*{side} {outcome}*"

    try:    price = f"{float(t.get('price') or 0)*100:.1f} c"
    except: price = "?"
    try:    usd   = f"${float(t.get('amount') or 0):,.2f}"
    except: usd   = "?"

    if wallet_row.get("nickname"):
        wallet_line = f"{name} (`{eoa}`)"
    else:
        wallet_line = f"`{eoa}`"

    return "\n".join([
        "✅ *TRADE EXECUTED*",
        "",
        f"Market: {link}",
        "",
        f"Target Wallet: {wallet_line}",
        f"• Action: {action} for {usd}",
        f"• Order Price: {price}",
    ])


# ============================================================
# DAILY SUMMARY
# ============================================================

def build_daily_summary(chat_id, target_date=None):
    wallets = db_get_wallets(chat_id)
    if not wallets:
        return None

    user = db_get_user(chat_id)
    api_key = user["api_key"] if user else None

    if target_date is None:
        target_date = str(date.today())

    try:
        d_fmt = datetime.strptime(target_date, "%Y-%m-%d").strftime("%b %d, %Y")
    except Exception:
        d_fmt = target_date

    lines = [f"📊 *Daily Report — {d_fmt}*\n"]

    # Fetch positions in parallel
    pos_results = {}
    def fetch_pos(w):
        if api_key:
            total, value = fetch_positions_summary(api_key, w["eoa"])
            pos_results[w["id"]] = (total, value)
        else:
            pos_results[w["id"]] = (None, None)

    threads = [threading.Thread(target=fetch_pos, args=(w,)) for w in wallets]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    for w in wallets:
        daily  = db_get_daily_by_date(w["id"], target_date)
        name   = wallet_display(w)
        eoa    = w["eoa"]
        total  = daily.get("total", 0)
        volume = daily.get("volume", 0.0)
        markets = daily.get("markets", [])
        pos_total, pos_value = pos_results.get(w["id"], (None, None))

        lines.append(f"*{name}*")
        lines.append(f"`{short_addr(eoa)}`")

        if total == 0:
            lines.append("No trades today.")
        else:
            lines.append(f"Trades: {total} | Volume: ${volume:,.2f}")
            lines.append(f"Markets: {len(markets)} traded")

        if pos_total is not None:
            lines.append(f"Positions: {pos_total} | Portfolio Value: ${pos_value:,.2f}")
        else:
            lines.append("Positions: N/A")

        lines.append("")

    return "\n".join(lines)


# ============================================================
# MONITOR THREAD
# ============================================================

class MonitorThread(threading.Thread):
    def __init__(self, chat_id, api_key, wallet_row):
        super().__init__(daemon=True)
        self.chat_id      = chat_id
        self.api_key      = api_key
        self.wallet_row   = wallet_row
        self.wallet_id    = wallet_row["id"]
        self.eoa          = wallet_row["eoa"]
        self.last_seen_id = wallet_row.get("last_seen_id") or None
        self.stop_event   = threading.Event()
        self.last_heartbeat = 0

    def stop(self):
        self.stop_event.set()

    def run(self):
        print(f"[Monitor] START chat={self.chat_id} wallet={self.eoa}")
        consecutive_errors = 0

        while not self.stop_event.is_set():
            try:
                now_ts = time.time()
                if now_ts - self.last_heartbeat >= HEARTBEAT_SECONDS:
                    print(f"[Monitor] ALIVE chat={self.chat_id} wallet={self.eoa}")
                    self.last_heartbeat = now_ts

                trades = fetch_trades(self.api_key, self.eoa)
                consecutive_errors = 0

                if isinstance(trades, list) and trades:
                    if self.last_seen_id is None:
                        self.last_seen_id = pick_id(trades[0])
                        db_update_wallet(self.wallet_id, last_seen_id=self.last_seen_id)
                    else:
                        new_trades = []
                        for tr in trades:
                            if pick_id(tr) == self.last_seen_id:
                                break
                            new_trades.append(tr)

                        self.wallet_row = db_get_wallet(self.wallet_id) or self.wallet_row

                        for tr in reversed(new_trades):
                            send_message(self.chat_id, format_trade_alert(self.wallet_row, tr))
                            db_add_trade_to_daily(self.wallet_id, tr)

                        if new_trades:
                            self.last_seen_id = pick_id(trades[0])
                            db_update_wallet(self.wallet_id, last_seen_id=self.last_seen_id)

            except Exception as e:
                print(f"[Monitor] POLL ERROR chat={self.chat_id} wallet={self.eoa}: {repr(e)}")
                consecutive_errors += 1
                if consecutive_errors == 10:
                    send_message(self.chat_id,
                        f"⚠️ Monitor error for `{self.eoa}`.\n"
                        f"Last error: `{repr(e)}`\n\n"
                        f"Please verify your API key with /start.")

            self.stop_event.wait(POLL_SECONDS)

        print(f"[Monitor] STOP chat={self.chat_id} wallet={self.eoa}")


# ============================================================
# MONITOR REGISTRY
# ============================================================

MONITORS      = {}
MONITORS_LOCK = threading.Lock()

def start_monitor(chat_id, api_key, wallet_row):
    key = (chat_id, wallet_row["id"])
    with MONITORS_LOCK:
        old = MONITORS.get(key)
        if old and old.is_alive():
            old.stop()
            old.join(timeout=10)
        t = MonitorThread(chat_id, api_key, wallet_row)
        t.start()
        MONITORS[key] = t

def stop_monitor(chat_id, wallet_id):
    key = (chat_id, wallet_id)
    with MONITORS_LOCK:
        t = MONITORS.pop(key, None)
        if t and t.is_alive():
            t.stop()
            t.join(timeout=10)

def stop_all_monitors(chat_id):
    with MONITORS_LOCK:
        keys = [k for k in MONITORS if k[0] == chat_id]
        for key in keys:
            t = MONITORS.pop(key, None)
            if t and t.is_alive():
                t.stop()


# ============================================================
# CONVERSATION STATE
# ============================================================

CHAT_STATE = {}

def get_step(chat_id):    return CHAT_STATE.get(chat_id, {}).get("step")
def get_state(chat_id):   return CHAT_STATE.get(chat_id, {})
def set_step(chat_id, step, **data): CHAT_STATE[chat_id] = {"step": step, **data}
def clear_step(chat_id):  CHAT_STATE.pop(chat_id, None)

def get_user_name(from_obj):
    first = (from_obj or {}).get("first_name") or ""
    last  = (from_obj or {}).get("last_name")  or ""
    return (first + " " + last).strip() or "there"


# ============================================================
# HANDLE MESSAGES
# ============================================================

def handle_message(message):
    chat_id   = message["chat"]["id"]
    text      = message.get("text", "").strip()
    user_name = get_user_name(message.get("from") or message.get("chat"))

    if text in ("/start", "/menu"):
        clear_step(chat_id)
        user = db_get_user(chat_id)
        if not user or not user.get("api_key"):
            send_message(chat_id,
                "👋 *Welcome to Opitrack!*\n\n"
                "Track any wallet on opinion.trade - get instant alerts, daily summaries and last trade history in one place.\n\n"
                "To get started, you need to enter your Opinion API key.\n"
                "Get your key at: https://docs.opinion.trade/developer-guide/opinion-open-api/overview\n\n"
                "⚠️ *Note:*\n"
                "• DO NOT use the API key of your main EOA wallet, use a brand new wallet instead\n"
                "• Your API key will be sent to the email you registered with Opinion\n\n"
                "🔑 Please enter your API key:"
            )
            set_step(chat_id, "waiting_api_key")
        else:
            send_main_menu(chat_id, user_name)
        return

    if text == "/stats" and chat_id == ADMIN_ID:
        total, active, today = db_count_users()
        send_message(chat_id,
            f"📈 *Opitrack Stats*\n\n"
            f"Total users: {total}\n"
            f"Active monitors: {active}\n"
            f"Joined today: {today}"
        )
        return

    step  = get_step(chat_id)
    state = get_state(chat_id)

    if step == "waiting_api_key":
        api_key = text.strip()
        send_message(chat_id, "🔄 Validating API key...")
        if validate_api_key(api_key):
            db_upsert_user(chat_id, api_key=api_key)
            clear_step(chat_id)
            send_message(chat_id,
                "✅ *API key is valid!*\n\n"
                "Please enter the EOA wallet address you want to monitor:"
            )
            set_step(chat_id, "waiting_eoa")
        else:
            send_message(chat_id, "❌ Invalid API key. Please try again.\n\n🔑 Enter your API key:")
        return

    if step == "waiting_new_api_key":
        api_key = text.strip()
        send_message(chat_id, "🔄 Validating API key...")
        if validate_api_key(api_key):
            db_upsert_user(chat_id, api_key=api_key)
            wallets = db_get_wallets(chat_id)
            stop_all_monitors(chat_id)
            for w in wallets:
                start_monitor(chat_id, api_key, w)
            clear_step(chat_id)
            send_message(chat_id, "✅ *API key updated successfully!*", reply_markup=back_btn())
        else:
            send_message(chat_id, "❌ Invalid API key. Please try again.\n\n🔑 Enter your new API key:")
        return

    if step == "waiting_eoa":
        eoa = text.strip()
        if not (eoa.startswith("0x") and len(eoa) == 42):
            send_message(chat_id,
                "❌ Invalid wallet address. Must start with `0x` and be 42 characters long.\n\n"
                "Please enter a valid EOA address:")
            return
        existing = db_get_wallets(chat_id)
        if any(w["eoa"].lower() == eoa.lower() for w in existing):
            send_message(chat_id,
                "⚠️ This wallet is already being monitored.\n\nPlease enter a different address:",
                reply_markup=back_btn())
            return
        set_step(chat_id, "waiting_nickname", pending_eoa=eoa)
        send_message(chat_id,
            f"Wallet: `{eoa}`\n\n"
            "(Optional) Give this wallet a nickname:",
            reply_markup={"inline_keyboard": [[{"text": "Skip", "callback_data": "skip_nickname"}]]}
        )
        return

    if step == "waiting_nickname":
        nickname    = text.strip() or None
        pending_eoa = state.get("pending_eoa")
        _finish_add_wallet(chat_id, pending_eoa, nickname, user_name)
        return

    if step == "waiting_rename":
        wallet_id = state.get("wallet_id")
        new_name  = text.strip()
        if wallet_id and new_name:
            db_update_wallet(wallet_id, nickname=new_name)
            clear_step(chat_id)
            w = db_get_wallet(wallet_id)
            send_message(chat_id,
                f"✅ *Wallet renamed to \"{new_name}\"*\n\n"
                + wallet_detail_text(w),
                reply_markup=wallet_detail_markup(wallet_id)
            )
        return

    send_message(chat_id, "Use /start to open the menu.")


def _finish_add_wallet(chat_id, eoa, nickname, user_name):
    user    = db_get_user(chat_id)
    api_key = user["api_key"] if user else None
    w       = db_add_wallet(chat_id, eoa, nickname)
    if w:
        start_monitor(chat_id, api_key, w)
    clear_step(chat_id)
    name = nickname or eoa
    send_message(chat_id,
        f"✅ *Now monitoring:* {name}\n`{eoa}`\n\n"
        f"You will be notified when a new trade is executed.",
        reply_markup={"inline_keyboard": [
            [{"text": "My Wallets", "callback_data": "my_wallets"}],
            [{"text": "Main Menu",  "callback_data": "main_menu"}],
        ]}
    )


# ============================================================
# HANDLE CALLBACKS
# ============================================================

def handle_callback(callback_query):
    chat_id    = callback_query["message"]["chat"]["id"]
    message_id = callback_query["message"]["message_id"]
    data       = callback_query.get("data", "")
    cq_id      = callback_query["id"]
    user_name  = get_user_name(callback_query.get("from"))

    answer_callback(cq_id)

    if data == "main_menu":
        clear_step(chat_id)
        edit_main_menu(chat_id, message_id, user_name)
        return

    if data == "my_wallets":
        clear_step(chat_id)
        edit_message(chat_id, message_id,
            my_wallets_text(chat_id),
            reply_markup=my_wallets_markup(chat_id)
        )
        return

    if data == "add_wallet":
        count = db_count_wallets(chat_id)
        if count >= MAX_WALLETS:
            edit_message(chat_id, message_id,
                f"⚠️ You've reached the maximum of {MAX_WALLETS} wallets.\n"
                f"Please remove one before adding a new one.",
                reply_markup={"inline_keyboard": [
                    [{"text": "My Wallets", "callback_data": "my_wallets"}],
                    [{"text": "Main Menu",  "callback_data": "main_menu"}],
                ]}
            )
            return
        set_step(chat_id, "waiting_eoa")
        edit_message(chat_id, message_id,
            "Please enter the EOA wallet address you want to monitor:",
            reply_markup={"inline_keyboard": [[{"text": "Cancel", "callback_data": "main_menu"}]]}
        )
        return

    if data == "skip_nickname":
        state       = get_state(chat_id)
        pending_eoa = state.get("pending_eoa")
        if pending_eoa:
            _finish_add_wallet(chat_id, pending_eoa, None, user_name)
            try:
                edit_message(chat_id, message_id, f"Wallet: `{pending_eoa}`\n\nNickname: _(none)_")
            except Exception:
                pass
        return

    if data.startswith("wallet~"):
        wallet_id = int(data.split("~")[1])
        w = db_get_wallet(wallet_id)
        if w and w["chat_id"] == chat_id:
            edit_message(chat_id, message_id,
                wallet_detail_text(w),
                reply_markup=wallet_detail_markup(wallet_id)
            )
        return

    if data.startswith("positions~"):
        wallet_id = int(data.split("~")[1])
        w         = db_get_wallet(wallet_id)
        user      = db_get_user(chat_id)
        if w and w["chat_id"] == chat_id:
            edit_message(chat_id, message_id, "🔄 Fetching positions...")
            msg = fetch_positions_text(user["api_key"], w["eoa"], wallet_display(w))
            edit_message(chat_id, message_id, msg,
                reply_markup={"inline_keyboard": [
                    [{"text": "Back", "callback_data": f"wallet~{wallet_id}"}],
                    [{"text": "Main Menu", "callback_data": "main_menu"}],
                ]}
            )
        return

    if data.startswith("history~"):
        wallet_id = int(data.split("~")[1])
        w         = db_get_wallet(wallet_id)
        user      = db_get_user(chat_id)
        if w and w["chat_id"] == chat_id:
            edit_message(chat_id, message_id, "🔄 Fetching trade history...")
            msg = fetch_history_text(user["api_key"], w["eoa"], wallet_display(w))
            edit_message(chat_id, message_id, msg,
                reply_markup={"inline_keyboard": [
                    [{"text": "Back", "callback_data": f"wallet~{wallet_id}"}],
                    [{"text": "Main Menu", "callback_data": "main_menu"}],
                ]}
            )
        return

    if data.startswith("rename~"):
        wallet_id = int(data.split("~")[1])
        w         = db_get_wallet(wallet_id)
        if w and w["chat_id"] == chat_id:
            set_step(chat_id, "waiting_rename", wallet_id=wallet_id)
            edit_message(chat_id, message_id,
                f"Enter new nickname for:\n`{w['eoa']}`",
                reply_markup={"inline_keyboard": [
                    [{"text": "Cancel", "callback_data": f"wallet~{wallet_id}"}],
                    [{"text": "Main Menu", "callback_data": "main_menu"}],
                ]}
            )
        return

    if data.startswith("remove~"):
        wallet_id = int(data.split("~")[1])
        w         = db_get_wallet(wallet_id)
        if w and w["chat_id"] == chat_id:
            name = wallet_display(w)
            edit_message(chat_id, message_id,
                f"Are you sure you want to remove this wallet?\n\n*{name}*\n`{w['eoa']}`",
                reply_markup={"inline_keyboard": [
                    [{"text": "Yes, remove", "callback_data": f"confirm_remove~{wallet_id}"},
                     {"text": "Cancel",      "callback_data": f"wallet~{wallet_id}"}],
                    [{"text": "Main Menu",   "callback_data": "main_menu"}],
                ]}
            )
        return

    if data.startswith("confirm_remove~"):
        wallet_id = int(data.split("~")[1])
        w         = db_get_wallet(wallet_id)
        if w and w["chat_id"] == chat_id:
            stop_monitor(chat_id, wallet_id)
            db_remove_wallet(wallet_id)
            edit_message(chat_id, message_id,
                "✅ *Wallet removed.*",
                reply_markup={"inline_keyboard": [
                    [{"text": "My Wallets", "callback_data": "my_wallets"}],
                    [{"text": "Main Menu",  "callback_data": "main_menu"}],
                ]}
            )
        return

    if data == "change_api_key":
        set_step(chat_id, "waiting_new_api_key")
        edit_message(chat_id, message_id,
            "Please enter your new API key:\n\n🔑 New API key:",
            reply_markup={"inline_keyboard": [[{"text": "Cancel", "callback_data": "main_menu"}]]}
        )
        return


# ============================================================
# DAILY SUMMARY SCHEDULER
# ============================================================

def daily_summary_loop():
    last_sent_date = None
    while True:
        try:
            now       = datetime.utcnow()
            today_str = str(date.today())
            if now.hour == 23 and now.minute >= 58 and last_sent_date != today_str:
                print(f"[Daily] Sending summaries for {today_str}")
                chat_ids = db_get_all_users_with_wallets()
                for chat_id in chat_ids:
                    try:
                        msg = build_daily_summary(chat_id)
                        if msg:
                            send_message(chat_id, msg)
                    except Exception as e:
                        print(f"[Daily] Error for chat {chat_id}: {repr(e)}")
                last_sent_date = today_str
                print(f"[Daily] Done for {today_str}")
        except Exception as e:
            print(f"[Daily] Loop error: {repr(e)}")
        time.sleep(30)


# ============================================================
# MAIN
# ============================================================

def run_bot():
    init_db()
    print("Opitrack v2 bot started.")

    wallets = db_get_all_active_wallets()
    for w in wallets:
        print(f"[Resume] chat={w['chat_id']} wallet={w['eoa']}")
        start_monitor(w["chat_id"], w["api_key"], w)

    threading.Thread(target=daily_summary_loop, daemon=True).start()

    offset        = 0
    processed_ids = set()

    while True:
        try:
            resp = requests.get(
                TG_BASE.format(token=TELEGRAM_BOT_TOKEN, method="getUpdates"),
                params={"offset": offset, "timeout": 30},
                timeout=40
            )
            updates = resp.json().get("result", [])

            for update in updates:
                uid    = update["update_id"]
                offset = uid + 1
                if uid in processed_ids:
                    continue
                processed_ids.add(uid)
                if len(processed_ids) > 1000:
                    processed_ids = set(list(processed_ids)[-500:])

                if "message" in update:
                    handle_message(update["message"])
                elif "callback_query" in update:
                    handle_callback(update["callback_query"])

        except Exception as e:
            print(f"[Bot] Update loop error: {repr(e)}")
            time.sleep(5)


if __name__ == "__main__":
    if not TELEGRAM_BOT_TOKEN:
        print("ERROR: Missing TELEGRAM_BOT_TOKEN in .env")
    else:
        run_bot()
