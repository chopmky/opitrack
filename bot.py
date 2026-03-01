import os
import time
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
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))

OPINION_TRADE_URL     = "https://openapi.opinion.trade/openapi/trade/user/{wallet}"
OPINION_POSITIONS_URL = "https://openapi.opinion.trade/openapi/positions/user/{wallet}"
TG_BASE               = "https://api.telegram.org/bot{token}/{method}"

POLL_SECONDS      = 5
HEARTBEAT_SECONDS = 3600
DB_PATH           = "opitrack.db"


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
                chat_id      INTEGER PRIMARY KEY,
                api_key      TEXT,
                eoa          TEXT,
                last_seen_id TEXT,
                joined_date  TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS daily (
                chat_id  INTEGER,
                date     TEXT,
                total    INTEGER DEFAULT 0,
                volume   REAL    DEFAULT 0,
                markets  TEXT    DEFAULT '[]',
                PRIMARY KEY (chat_id, date)
            )
        """)

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
                sets = ", ".join(f"{k}=?" for k in kwargs)
                conn.execute(f"UPDATE users SET {sets} WHERE chat_id=?", (*kwargs.values(), chat_id))
            else:
                kwargs["chat_id"] = chat_id
                kwargs.setdefault("joined_date", str(date.today()))
                cols = ", ".join(kwargs.keys())
                placeholders = ", ".join("?" * len(kwargs))
                conn.execute(f"INSERT INTO users ({cols}) VALUES ({placeholders})", list(kwargs.values()))

def db_count_users():
    with DB_LOCK:
        with get_db() as conn:
            total  = conn.execute("SELECT COUNT(*) FROM users WHERE api_key IS NOT NULL").fetchone()[0]
            active = conn.execute("SELECT COUNT(*) FROM users WHERE eoa IS NOT NULL").fetchone()[0]
            today  = conn.execute("SELECT COUNT(*) FROM users WHERE joined_date=?", (str(date.today()),)).fetchone()[0]
            return total, active, today

def db_get_all_active_users():
    with DB_LOCK:
        with get_db() as conn:
            rows = conn.execute(
                "SELECT * FROM users WHERE eoa IS NOT NULL AND api_key IS NOT NULL"
            ).fetchall()
            return [dict(r) for r in rows]

def db_get_daily(chat_id):
    import json
    today = str(date.today())
    with DB_LOCK:
        with get_db() as conn:
            row = conn.execute(
                "SELECT * FROM daily WHERE chat_id=? AND date=?", (chat_id, today)
            ).fetchone()
            if row:
                d = dict(row)
                d["markets"] = json.loads(d["markets"])
                return d
            return {"chat_id": chat_id, "date": today, "total": 0, "volume": 0.0, "markets": []}

def db_add_trade_to_daily(chat_id, trade):
    import json
    daily = db_get_daily(chat_id)
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
                INSERT INTO daily (chat_id, date, total, volume, markets)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(chat_id, date) DO UPDATE SET
                    total=excluded.total,
                    volume=excluded.volume,
                    markets=excluded.markets
            """, (chat_id, daily["date"], daily["total"], daily["volume"], json.dumps(daily["markets"])))
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
# MENUS
# ============================================================

def back_btn():
    return {"inline_keyboard": [[{"text": "Main Menu", "callback_data": "main_menu"}]]}

def main_menu_markup(has_wallet):
    if has_wallet:
        return {"inline_keyboard": [
            [{"text": "Positions",      "callback_data": "view_positions"},
             {"text": "Trade History",  "callback_data": "view_history"}],
            [{"text": "Change Wallet",  "callback_data": "change_wallet"},
             {"text": "Change API Key", "callback_data": "change_api_key"}],
        ]}
    else:
        return {"inline_keyboard": [
            [{"text": "Set Wallet", "callback_data": "set_wallet"}],
        ]}

def main_menu_text(user_name, user):
    eoa = user.get("eoa") if user else None
    name = user_name or "there"
    if eoa:
        return (
            f"Welcome *{name}* to Opitrack Bot\n\n"
            f"Monitoring: `{eoa}`\n"
            f"Status: 🟢 Active"
        )
    else:
        return (
            f"Welcome *{name}* to Opitrack Bot\n\n"
            f"No wallet set yet."
        )

def send_main_menu(chat_id, user_name):
    user   = db_get_user(chat_id)
    text   = main_menu_text(user_name, user)
    markup = main_menu_markup(bool(user and user.get("eoa")))
    send_message(chat_id, text, reply_markup=markup)

def edit_main_menu(chat_id, message_id, user_name):
    user   = db_get_user(chat_id)
    text   = main_menu_text(user_name, user)
    markup = main_menu_markup(bool(user and user.get("eoa")))
    edit_message(chat_id, message_id, text, reply_markup=markup)


# ============================================================
# OPINION API
# ============================================================

def validate_api_key(api_key):
    """Call a dummy address to check if the API key is accepted."""
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

def fetch_positions_text(api_key, eoa):
    url = OPINION_POSITIONS_URL.format(wallet=eoa)
    try:
        resp = requests.get(url, headers={"apikey": api_key}, timeout=30)
        resp.raise_for_status()
        positions = resp.json().get("result", {}).get("list", [])
        if not positions:
            return "No open positions."
        lines = [f"*Open Positions ({len(positions)})*\n"]
        for i, p in enumerate(positions, 1):
            root    = p.get("rootMarketTitle") or p.get("marketTitle") or f"Market {p.get('marketId','?')}"
            sub     = p.get("marketTitle") or ""
            outcome = "YES" if p.get("outcomeSide") == 1 else "NO"
            try:    shares = f"{float(p.get('sharesOwned') or 0):.4f}"
            except: shares = "?"
            try:    value  = f"${float(p.get('currentValueInQuoteToken') or 0):.4f}"
            except: value  = "?"
            try:    avg    = f"{float(p.get('avgEntryPrice') or 0):.4f}c"
            except: avg    = "?"
            try:
                pnl     = float(p.get("unrealizedPnl") or 0)
                pnl_pct = float(p.get("unrealizedPnlPercent") or 0) * 100
                pnl_s   = f"+${pnl:.4f}" if pnl >= 0 else f"-${abs(pnl):.4f}"
                pnl_p   = f"+{pnl_pct:.1f}%" if pnl_pct >= 0 else f"{pnl_pct:.1f}%"
            except:
                pnl_s = pnl_p = "?"
            lines.append(f"{i}. *{root}*")
            if sub and sub != root:
                lines.append(f"   {sub}")
            lines.append(f"   {outcome} | Shares: {shares} | Value: {value}")
            lines.append(f"   Avg Cost: {avg} | PnL: {pnl_s} ({pnl_p})\n")
        return "\n".join(lines)
    except Exception as e:
        return "Could not fetch positions. Please try again later."

def fetch_history_text(api_key, eoa):
    url = OPINION_TRADE_URL.format(wallet=eoa)
    try:
        resp = requests.get(url, headers={"apikey": api_key}, timeout=30)
        resp.raise_for_status()
        trades = resp.json().get("result", {}).get("list") or []
        if not trades:
            return "No trades found."
        trades = trades[:10]
        lines = ["*Last 10 Trades*\n"]
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
    except Exception as e:
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

def format_trade_alert(wallet, t):
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

    return "\n".join([
        "✅ *TRADE EXECUTED*",
        "",
        f"Market: {link}",
        "",
        f"Target Wallet: `{wallet}`",
        f"• Action: {action} for {usd}",
        f"• Order Price: {price}",
    ])

def build_daily_summary(wallet, daily):
    d       = daily.get("date", str(date.today()))
    total   = daily.get("total", 0)
    volume  = daily.get("volume", 0.0)
    markets = daily.get("markets", [])
    try:
        d_fmt = datetime.strptime(d, "%Y-%m-%d").strftime("%b %d, %Y")
    except Exception:
        d_fmt = d
    lines = [
        f"📊 *Daily Report — {d_fmt}*",
        f"Wallet: `{wallet}`",
        "",
        f"Total trades today: {total}",
        f"Total volume: ${volume:.2f}",
        "",
        "Markets traded:",
    ]
    for m in (markets or ["(none)"]):
        lines.append(f"• {m}")
    return "\n".join(lines)


# ============================================================
# MONITOR THREAD
# ============================================================

class MonitorThread(threading.Thread):
    def __init__(self, chat_id, api_key, wallet, last_seen_id):
        super().__init__(daemon=True)
        self.chat_id      = chat_id
        self.api_key      = api_key
        self.wallet       = wallet
        self.last_seen_id = last_seen_id
        self.stop_event   = threading.Event()
        self.last_heartbeat = 0

    def stop(self):
        self.stop_event.set()

    def run(self):
        print(f"[Monitor] START chat={self.chat_id} wallet={self.wallet}")
        consecutive_errors = 0

        while not self.stop_event.is_set():
            try:
                now_ts = time.time()
                if now_ts - self.last_heartbeat >= HEARTBEAT_SECONDS:
                    print(f"[Monitor] ALIVE chat={self.chat_id} wallet={self.wallet}")
                    self.last_heartbeat = now_ts

                trades = fetch_trades(self.api_key, self.wallet)
                consecutive_errors = 0

                if isinstance(trades, list) and trades:
                    if self.last_seen_id is None:
                        # First run: set checkpoint silently, no alerts
                        self.last_seen_id = pick_id(trades[0])
                        db_upsert_user(self.chat_id, last_seen_id=self.last_seen_id)
                    else:
                        new_trades = []
                        for tr in trades:
                            if pick_id(tr) == self.last_seen_id:
                                break
                            new_trades.append(tr)

                        for tr in reversed(new_trades):
                            send_message(self.chat_id, format_trade_alert(self.wallet, tr))
                            db_add_trade_to_daily(self.chat_id, tr)

                        if new_trades:
                            self.last_seen_id = pick_id(trades[0])
                            db_upsert_user(self.chat_id, last_seen_id=self.last_seen_id)

            except Exception as e:
                print(f"[Monitor] POLL ERROR chat={self.chat_id}: {repr(e)}")
                consecutive_errors += 1
                if consecutive_errors == 10:
                    send_message(self.chat_id,
                        f"⚠️ Monitor encountered 10 consecutive errors.\n"
                        f"Last error: `{repr(e)}`\n\n"
                        f"Please verify your API key with /start.")

            self.stop_event.wait(POLL_SECONDS)

        print(f"[Monitor] STOP chat={self.chat_id}")


# ============================================================
# MONITOR REGISTRY
# ============================================================

MONITORS: dict = {}
MONITORS_LOCK  = threading.Lock()

def start_monitor(chat_id, api_key, wallet, last_seen_id=None):
    with MONITORS_LOCK:
        old = MONITORS.get(chat_id)
        if old and old.is_alive():
            old.stop()
            old.join(timeout=10)
        t = MonitorThread(chat_id, api_key, wallet, last_seen_id)
        t.start()
        MONITORS[chat_id] = t

def stop_monitor(chat_id):
    with MONITORS_LOCK:
        t = MONITORS.pop(chat_id, None)
        if t and t.is_alive():
            t.stop()
            t.join(timeout=10)


# ============================================================
# CONVERSATION STATE
# ============================================================

CHAT_STATE: dict = {}  # chat_id -> step

def get_step(chat_id):    return CHAT_STATE.get(chat_id)
def set_step(chat_id, s): CHAT_STATE[chat_id] = s
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
        if not user or not user.get("api_key") or not user.get("eoa"):
            send_message(chat_id,
                "👋 *Welcome to Opitrack!*\n\nTrack any wallet on opinion.trade - get instant alerts, daily summaries and last trade history in one place.\n\n"
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

    step = get_step(chat_id)

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
            send_message(chat_id,
                "❌ Invalid API key. Please try again.\n\n"
                "🔑 Enter your API key:"
            )
        return

    if step == "waiting_new_api_key":
        api_key = text.strip()
        send_message(chat_id, "🔄 Validating API key...")
        if validate_api_key(api_key):
            db_upsert_user(chat_id, api_key=api_key)
            user = db_get_user(chat_id)
            if user and user.get("eoa"):
                start_monitor(chat_id, api_key, user["eoa"], user.get("last_seen_id"))
            clear_step(chat_id)
            send_message(chat_id,
                "✅ *API key updated successfully!*",
                reply_markup=back_btn()
            )
        else:
            send_message(chat_id,
                "❌ Invalid API key. Please try again.\n\n"
                "🔑 Enter your new API key:"
            )
        return

    if step in ("waiting_eoa", "waiting_new_eoa"):
        eoa = text.strip()
        if not (eoa.startswith("0x") and len(eoa) == 42):
            send_message(chat_id,
                "❌ Invalid wallet address. Must start with `0x` and be 42 characters long.\n\n"
                "Please enter a valid EOA address:"
            )
            return
        user    = db_get_user(chat_id)
        api_key = user["api_key"] if user else None
        db_upsert_user(chat_id, eoa=eoa, last_seen_id=None)
        start_monitor(chat_id, api_key, eoa, last_seen_id=None)
        clear_step(chat_id)
        send_message(chat_id,
            f"✅ *Now monitoring:* `{eoa}`\n"
            f"You will be notified when a new trade is executed.",
            reply_markup={"inline_keyboard": [[{"text": "Main Menu", "callback_data": "main_menu"}]]}
        )
        return

    send_message(chat_id, "Use /start to open the menu.")


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

    if data == "set_wallet":
        set_step(chat_id, "waiting_eoa")
        edit_message(chat_id, message_id,
            "Please enter the EOA wallet address you want to monitor:",
            reply_markup={"inline_keyboard": [[{"text": "Cancel", "callback_data": "main_menu"}]]}
        )
        return

    if data == "change_wallet":
        user = db_get_user(chat_id)
        eoa  = user.get("eoa") if user else None
        set_step(chat_id, "waiting_new_eoa")
        prefix = f"Current wallet: `{eoa}`\n\n" if eoa else ""
        edit_message(chat_id, message_id,
            prefix + "Please enter the new EOA wallet address:",
            reply_markup={"inline_keyboard": [[{"text": "Cancel", "callback_data": "main_menu"}]]}
        )
        return

    if data == "change_api_key":
        set_step(chat_id, "waiting_new_api_key")
        edit_message(chat_id, message_id,
            "Please enter your new API key:\n\n"
            "🔑 New API key:",
            reply_markup={"inline_keyboard": [[{"text": "Cancel", "callback_data": "main_menu"}]]}
        )
        return

    if data == "view_positions":
        user = db_get_user(chat_id)
        eoa  = user.get("eoa") if user else None
        if eoa:
            edit_message(chat_id, message_id, "🔄 Fetching positions...")
            msg = fetch_positions_text(user["api_key"], eoa)
            edit_message(chat_id, message_id, msg, reply_markup=back_btn())
        else:
            edit_message(chat_id, message_id,
                "No wallet set. Use /start to set one.", reply_markup=back_btn())
        return

    if data == "view_history":
        user = db_get_user(chat_id)
        eoa  = user.get("eoa") if user else None
        if eoa:
            edit_message(chat_id, message_id, "🔄 Fetching trade history...")
            msg = fetch_history_text(user["api_key"], eoa)
            edit_message(chat_id, message_id, msg, reply_markup=back_btn())
        else:
            edit_message(chat_id, message_id,
                "No wallet set. Use /start to set one.", reply_markup=back_btn())
        return


# ============================================================
# DAILY SUMMARY SCHEDULER
# ============================================================

def daily_summary_loop():
    last_sent_date = None
    while True:
        try:
            now       = datetime.now()
            today_str = str(date.today())
            if now.hour == 23 and now.minute >= 58 and last_sent_date != today_str:
                users = db_get_all_active_users()
                for user in users:
                    try:
                        daily  = db_get_daily(user["chat_id"])
                        msg    = build_daily_summary(user["eoa"], daily)
                        send_message(user["chat_id"], msg)
                    except Exception as e:
                        print(f"[Daily] Error for chat {user['chat_id']}: {repr(e)}")
                last_sent_date = today_str
                print(f"[Daily] Summaries sent for {today_str}")
        except Exception as e:
            print(f"[Daily] Loop error: {repr(e)}")
        time.sleep(30)


# ============================================================
# MAIN
# ============================================================

def run_bot():
    init_db()
    print("Opitrack bot started.")

    # Resume monitors for all active users on restart
    for user in db_get_all_active_users():
        print(f"[Resume] chat={user['chat_id']} wallet={user['eoa']}")
        start_monitor(user["chat_id"], user["api_key"], user["eoa"], user.get("last_seen_id"))

    # Daily summary background thread
    threading.Thread(target=daily_summary_loop, daemon=True).start()

    # Telegram long-polling
    offset       = 0
    processed_ids: set = set()

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
