# ╔══════════════════════════════════════════════════════════════╗
# ║     MIK OTP Bot — Full System Bot                         ║
# ║     Numbers + OTP Forwarding + Full Admin Suite             ║
# ║     Developed by @elitetrade11                              ║
# ╚══════════════════════════════════════════════════════════════╝

import asyncio
import requests
import re
import ssl
import json
import os
import sys
import time
import logging
import sqlite3
import websockets
import phonenumbers
from phonenumbers import geocoder
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, Update, CopyTextButton
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes
)

# ═══════════════════════════════════════════════════════════════
#  CONFIGURATION
# ═══════════════════════════════════════════════════════════════
BOT_TOKEN      = "8793722255:AAHDqmFTdsrlik7d1Z50CoMnexgDBX0ig-M"
OWNER_IDS      = [7064636445]
ADMIN_IDS      = [7064636445]
OTP_GROUP_LINK = "https://t.me/+0YairwHa63NkNWM0"
BOT_NAME       = "MIK OTP Bot"

REQUIRED_CHANNELS = [
    {"name": "OTP Group",         "link": "https://t.me/+0YairwHa63NkNWM0",    "id": None},
    {"name": "Chat & Support",    "link": "https://t.me/+wksiFMvdngdjNGY0",    "id": None},
    {"name": "Main Channel",      "link": "https://t.me/+u-mSc5zdIalhMDM0",   "id": None},
    {"name": "Backup Channel",    "link": "https://t.me/+Rf1oYlInYOA5ZTk0",   "id": None},
    {"name": "Backup Group",      "link": "https://t.me/+Qa_cZA0ikrU5ZDU0",   "id": None},
    {"name": "Method Channel",    "link": "https://t.me/realsurvices1",         "id": "@realsurvices1"},
    {"name": "All Groups Folder", "link": "https://t.me/addlist/66eMAe6ietM4NTc1", "id": None},
]
DEV_CONTACT    = "@elitetrade11"

DEFAULT_PANELS = {
    "sami": {
        "url":      "http://147.135.212.197/crapi/st/viewstats",
        "token":    "RVdWQ0RBUzRaT1FGY2-EYWmTVWhZiIpea39rXV2YjoRDVW9YQW-ViA==",
        "records":  20,
        "api_type": "sami"   # sami = original format  |  mdr = MOAPI MDR format
    }
}

# ── MDR API (MOAPI) endpoint ──────────────────────────────────────────────────
MDR_API_URL = "http://137.74.1.203/crapi/reseller/mdr.php"

OTP_GROUP_IDS = [-1003851180469]

OTP_FILE      = "otp_store.json"
PANEL_FILE    = "panels.json"
IVAS_FILE     = "ivas.json"
USER_FILE     = "users.json"
GROUP_FILE    = "groups.json"
CONFIG_FILE   = "bot_config.json"
ADMINS_FILE   = "admins.json"
LOG_FILE      = "bot.log"
DB_FILE       = "bot_data.db"

FLAG_MAP = {
    "Kyrgyzstan":"🇰🇬","Kazakhstan":"🇰🇿","Russia":"🇷🇺","India":"🇮🇳",
    "USA":"🇺🇸","UK":"🇬🇧","Pakistan":"🇵🇰","Germany":"🇩🇪","France":"🇫🇷",
    "Turkey":"🇹🇷","Brazil":"🇧🇷","Indonesia":"🇮🇩","Bangladesh":"🇧🇩",
    "Vietnam":"🇻🇳","Philippines":"🇵🇭","China":"🇨🇳","Nigeria":"🇳🇬",
    "Ukraine":"🇺🇦","Egypt":"🇪🇬",
}

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

STATS = {"start_time": time.time(), "otps_sent": 0, "otps_dropped": 0,
         "errors": 0, "panel_hits": {}, "ivas_hits": {}}

PANEL_ADD_STATES   = {}
IVAS_ADD_STATES    = {}
BROADCAST_STATES   = {}
SETTING_STATES     = {}
NB_STATE           = {}
FETCH_STATES       = {}
USER_PREFIX_STATES = {}
USER_SERVICE       = {}   # uid -> selected service string
STATE_TIMEOUT    = 300

IVAS_TASKS: Dict[str, asyncio.Task] = {}
REST_TASKS: Dict[str, asyncio.Task] = {}

OTP_LOG: List[dict] = []
OTP_LOG_MAX = 200

# ═══════════════════════════════════════════════════════════════
#  SQLITE DATABASE
# ═══════════════════════════════════════════════════════════════
def init_db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS numbers
                 (id INTEGER PRIMARY KEY, country TEXT, phone TEXT, service TEXT DEFAULT '',
                  added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
    try:
        c.execute("ALTER TABLE numbers ADD COLUMN service TEXT DEFAULT ''")
    except Exception:
        pass
    c.execute("""CREATE TABLE IF NOT EXISTS tg_users
                 (user_id INTEGER PRIMARY KEY, first_seen TEXT,
                  last_seen TEXT, total_commands INTEGER DEFAULT 0,
                  user_prefix TEXT DEFAULT '')""")
    # Migrate: add user_prefix column if not exists
    try:
        c.execute("ALTER TABLE tg_users ADD COLUMN user_prefix TEXT DEFAULT ''")
    except Exception:
        pass
    c.execute("""CREATE TABLE IF NOT EXISTS otp_history
                 (id INTEGER PRIMARY KEY, number TEXT, service TEXT,
                  otp TEXT, source TEXT,
                  received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
    c.execute("""CREATE TABLE IF NOT EXISTS assigned_numbers
                 (phone TEXT PRIMARY KEY, user_id INTEGER NOT NULL,
                  assigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
    conn.commit()
    return conn

db = init_db()

def db_add_user(user_id: int):
    c = db.cursor()
    now = datetime.now().isoformat()
    c.execute("INSERT OR IGNORE INTO tg_users (user_id,first_seen,last_seen) VALUES (?,?,?)",
              (user_id, now, now))
    c.execute("UPDATE tg_users SET last_seen=?,total_commands=total_commands+1 WHERE user_id=?",
              (now, user_id))
    db.commit()

def db_get_countries(service: str = ""):
    c = db.cursor()
    if service:
        c.execute("SELECT country,COUNT(*) FROM numbers WHERE service=? GROUP BY country ORDER BY COUNT(*) DESC", (service,))
    else:
        c.execute("SELECT country,COUNT(*) FROM numbers GROUP BY country ORDER BY COUNT(*) DESC")
    return c.fetchall()

def db_get_services():
    """Return list of distinct services that have numbers in stock."""
    c = db.cursor()
    c.execute("SELECT DISTINCT service FROM numbers WHERE service != '' ORDER BY service")
    rows = c.fetchall()
    return [r[0] for r in rows]

def db_get_country_numbers(country: str, limit: int = 9999):
    c = db.cursor()
    c.execute("SELECT phone FROM numbers WHERE country=? LIMIT ?", (country, limit))
    return [r[0] for r in c.fetchall()]

def db_pop_number(country: str):
    c = db.cursor()
    c.execute("SELECT id,phone FROM numbers WHERE country=? LIMIT 1", (country,))
    row = c.fetchone()
    if row:
        c.execute("DELETE FROM numbers WHERE id=?", (row[0],))
        db.commit()
        return row[1]
    return None

def db_pop_numbers(country: str, count: int = 3, service: str = ""):
    """Pop up to `count` numbers from a country (optionally filtered by service), return list."""
    c = db.cursor()
    if service:
        c.execute("SELECT id,phone FROM numbers WHERE country=? AND service=? LIMIT ?", (country, service, count))
    else:
        c.execute("SELECT id,phone FROM numbers WHERE country=? LIMIT ?", (country, count))
    rows = c.fetchall()
    if rows:
        ids = [r[0] for r in rows]
        c.execute(f"DELETE FROM numbers WHERE id IN ({','.join('?'*len(ids))})", ids)
        db.commit()
        return [r[1] for r in rows]
    return []

def db_delete_country(country: str):
    c = db.cursor()
    c.execute("DELETE FROM numbers WHERE country=?", (country,))
    db.commit()

def db_add_numbers(country: str, nums: list, service: str = ""):
    c = db.cursor()
    c.executemany("INSERT INTO numbers (country,phone,service) VALUES (?,?,?)",
                  [(country, n, service) for n in nums])
    db.commit()

def db_total_numbers():
    c = db.cursor()
    c.execute("SELECT COUNT(*) FROM numbers")
    return c.fetchone()[0]

def db_save_otp_history(number: str, service: str, otp: str, source: str):
    c = db.cursor()
    c.execute("INSERT INTO otp_history (number,service,otp,source) VALUES (?,?,?,?)",
              (number, service, otp or "N/A", source))
    db.commit()

def db_assign_numbers(user_id: int, phones: list):
    """Record which user was assigned which numbers."""
    c = db.cursor()
    for phone in phones:
        clean = re.sub(r"[^0-9]", "", phone)
        c.execute("INSERT OR REPLACE INTO assigned_numbers (phone, user_id) VALUES (?,?)",
                  (clean, user_id))
    db.commit()

def db_get_owner_of_number(number: str) -> Optional[int]:
    """Return user_id of whoever was assigned this number, or None."""
    clean = re.sub(r"[^0-9]", "", number)
    c = db.cursor()
    # Try exact match first
    c.execute("SELECT user_id FROM assigned_numbers WHERE phone=?", (clean,))
    row = c.fetchone()
    if row:
        return row[0]
    # Try last-5-digit suffix match
    if len(clean) >= 5:
        suffix = clean[-5:]
        c.execute("SELECT user_id FROM assigned_numbers WHERE phone LIKE ?", (f"%{suffix}",))
        row = c.fetchone()
        if row:
            return row[0]
    return None

def db_clear_old_assignments(days: int = 2):
    """Cleanup assignments older than N days."""
    c = db.cursor()
    c.execute("DELETE FROM assigned_numbers WHERE assigned_at < datetime('now', ?)",
              (f"-{days} days",))
    db.commit()

def db_get_otp_history(limit: int = 20):
    c = db.cursor()
    c.execute("""SELECT number,service,otp,source,received_at FROM otp_history
                 ORDER BY id DESC LIMIT ?""", (limit,))
    return c.fetchall()

def db_search_otp_by_number(target: str):
    c = db.cursor()
    c.execute("""SELECT number,service,otp,source,received_at FROM otp_history
                 WHERE number LIKE ? ORDER BY id DESC LIMIT 10""", (f"%{target}%",))
    return c.fetchall()

def db_clear_otp_history():
    c = db.cursor()
    c.execute("DELETE FROM otp_history")
    db.commit()

def db_user_stats():
    c = db.cursor()
    today    = datetime.now().date().isoformat()
    week_ago = (datetime.now() - timedelta(days=7)).isoformat()
    c.execute("SELECT COUNT(*) FROM tg_users")
    total = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM tg_users WHERE last_seen LIKE ?", (f"{today}%",))
    active_today = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM tg_users WHERE last_seen >= ?", (week_ago,))
    active_week = c.fetchone()[0]
    return total, active_today, active_week

# ═══════════════════════════════════════════════════════════════
#  JSON HELPERS
# ═══════════════════════════════════════════════════════════════
def db_get_user_prefix(user_id: int) -> str:
    c = db.cursor()
    c.execute("SELECT user_prefix FROM tg_users WHERE user_id=?", (user_id,))
    row = c.fetchone()
    return (row[0] or "") if row else ""

def db_set_user_prefix(user_id: int, prefix: str):
    c = db.cursor()
    c.execute("UPDATE tg_users SET user_prefix=? WHERE user_id=?", (prefix, user_id))
    db.commit()

def db_pop_numbers_with_prefix(country: str, prefix: str, count: int = 3, service: str = ""):
    """Pop numbers from a country that start with the given prefix."""
    c = db.cursor()
    clean_prefix = re.sub(r"[^0-9]", "", prefix)
    if service:
        c.execute("SELECT id,phone FROM numbers WHERE country=? AND service=? AND phone LIKE ?",
                  (country, service, f"{clean_prefix}%"))
    else:
        c.execute("SELECT id,phone FROM numbers WHERE country=? AND phone LIKE ?",
                  (country, f"{clean_prefix}%"))
    rows = c.fetchmany(count)
    if rows:
        ids = [r[0] for r in rows]
        c.execute(f"DELETE FROM numbers WHERE id IN ({','.join('?'*len(ids))})", ids)
        db.commit()
        return [r[1] for r in rows]
    return []

def db_count_prefix_numbers(country: str, prefix: str, service: str = "") -> int:
    """Count how many numbers in a country start with the given prefix."""
    c = db.cursor()
    clean_prefix = re.sub(r"[^0-9]", "", prefix)
    if service:
        c.execute("SELECT COUNT(*) FROM numbers WHERE country=? AND service=? AND phone LIKE ?",
                  (country, service, f"{clean_prefix}%"))
    else:
        c.execute("SELECT COUNT(*) FROM numbers WHERE country=? AND phone LIKE ?",
                  (country, f"{clean_prefix}%"))
    return c.fetchone()[0]

def load_json(file: str, default: Any = None) -> Any:
    if default is None:
        default = {}
    if not os.path.exists(file):
        return default
    try:
        with open(file, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading {file}: {e}")
        return default

def save_json(file: str, data: Any) -> None:
    try:
        with open(file, 'w') as f:
            json.dump(data, f, indent=4)
    except Exception as e:
        logger.error(f"Error saving {file}: {e}")

def load_panels():      return load_json(PANEL_FILE, DEFAULT_PANELS.copy())
def save_panels(d):     save_json(PANEL_FILE, d)
def load_ivas():        return load_json(IVAS_FILE, {})
def save_ivas(d):       save_json(IVAS_FILE, d)
def load_otp_store():   return load_json(OTP_FILE, {})
def save_otp_store(d):  save_json(OTP_FILE, d)
def load_users():       return load_json(USER_FILE, {})
def save_users(d):      save_json(USER_FILE, d)
def load_groups():      return load_json(GROUP_FILE, OTP_GROUP_IDS.copy())
def save_groups(d):     save_json(GROUP_FILE, d)

# ═══════════════════════════════════════════════════════════════
#  PERMISSION SYSTEM
#  Each admin has a set of permission keys you can toggle.
#  Owners always have all permissions.
# ═══════════════════════════════════════════════════════════════

# All available permissions with display names
ALL_PERMISSIONS = {
    "numbers":    "📦 Add/Delete Numbers",
    "panels":     "📡 Manage Panels",
    "ivas":       "🔌 Manage IVAS",
    "groups":     "👥 Manage Groups",
    "broadcast":  "📢 Broadcast",
    "otp_history":"📋 OTP History",
    "fetch_sms":  "🔄 Fetch SMS",
    "files":      "📂 File Manager",
    "settings":   "⚙️ Settings",
    "advanced":   "🔧 Advanced Tools",
    "stats":      "📊 Stats/Status",
}

def load_staff() -> dict:
    """
    Returns dict: { user_id_str: {"name": str, "perms": [list of perm keys]} }
    """
    data = load_json(ADMINS_FILE, {"owners": list(OWNER_IDS), "staff": {}})
    return data.get("staff", {})

def save_staff(staff: dict):
    data = load_json(ADMINS_FILE, {"owners": list(OWNER_IDS), "staff": {}})
    data["staff"] = staff
    save_json(ADMINS_FILE, data)

def load_admins() -> list:
    """Back-compat: return all staff user IDs."""
    staff = load_staff()
    return [int(k) for k in staff.keys()] + list(OWNER_IDS)

def save_admins(admins: list, number_managers: list = None):
    """Back-compat stub — does nothing, use staff system instead."""
    pass

def load_number_managers() -> list:
    return []

def get_staff_perms(uid: int) -> list:
    if is_owner(uid):
        return list(ALL_PERMISSIONS.keys())
    staff = load_staff()
    return staff.get(str(uid), {}).get("perms", [])

def has_perm(uid: int, perm: str) -> bool:
    if is_owner(uid):
        return True
    return perm in get_staff_perms(uid)

def add_staff(uid: int, name: str, perms: list):
    staff = load_staff()
    staff[str(uid)] = {"name": name, "perms": perms}
    save_staff(staff)

def remove_staff(uid: int):
    staff = load_staff()
    staff.pop(str(uid), None)
    save_staff(staff)

def update_staff_perms(uid: int, perms: list):
    staff = load_staff()
    if str(uid) in staff:
        staff[str(uid)]["perms"] = perms
        save_staff(staff)

def load_config():
    return load_json(CONFIG_FILE, {
        "channel_link":    OTP_GROUP_LINK,
        "number_bot_link": "http://t.me/primezone3",
        "otp_forward":     True,
        "forward_delay":   0,
        "log_group":       None,
        "number_prefix":   "",
    })

def save_config(c): save_json(CONFIG_FILE, c)

def is_owner(uid: int) -> bool:
    return uid in OWNER_IDS

def is_admin(uid: int) -> bool:
    """True if user is owner OR in staff."""
    if is_owner(uid):
        return True
    staff = load_staff()
    return str(uid) in staff

def is_number_manager(uid: int) -> bool:
    return False  # replaced by granular perms

def has_any_access(uid: int) -> bool:
    return is_admin(uid)

def get_role_label(uid: int) -> str:
    if is_owner(uid):
        return "👑 Owner"
    staff = load_staff()
    entry = staff.get(str(uid))
    if entry:
        perms = entry.get("perms", [])
        return f"🛡️ Staff ({len(perms)} perms)"
    return "👤 User"

API_PANELS = load_panels()

# ═══════════════════════════════════════════════════════════════
#  OTP HELPERS
# ═══════════════════════════════════════════════════════════════
SERVICE_SHORT = {
    "whatsapp":"WS","telegram":"TG","facebook":"FB","instagram":"IG",
    "twitter":"TW","tiktok":"TT","snapchat":"SC","google":"GG","gmail":"GM",
    "microsoft":"MS","amazon":"AM","apple":"AP","uber":"UB","lyft":"LF",
    "paypal":"PP","viber":"VB","line":"LN","wechat":"WC","yahoo":"YH",
    "netflix":"NF","discord":"DC","linkedin":"LI","shopify":"SH",
    "binance":"BN","coinbase":"CB","steam":"ST","twitch":"TC",
    "signal":"SG","hinge":"HN","bumble":"BM","tinder":"TD",
}

REGION_LANGUAGE = {
    "DE":"German","AT":"German","CH":"German","FR":"French","BE":"French",
    "ES":"Spanish","MX":"Spanish","AR":"Spanish","PT":"Portuguese","BR":"Portuguese",
    "RU":"Russian","UA":"Russian","BY":"Russian","TR":"Turkish",
    "SA":"Arabic","AE":"Arabic","EG":"Arabic","CN":"Chinese","TW":"Chinese",
    "JP":"Japanese","KR":"Korean","IN":"Hindi","PK":"Urdu","IT":"Italian",
    "NL":"Dutch","PL":"Polish","SE":"Swedish","NO":"Norwegian","DK":"Danish",
    "FI":"Finnish","GR":"Greek","IR":"Persian","TH":"Thai","VN":"Vietnamese",
    "ID":"Indonesian","NG":"English","PH":"Filipino",
}

def get_service_short(service: str) -> str:
    s = service.lower().strip()
    for key, short in SERVICE_SHORT.items():
        if key in s:
            return short
    clean = re.sub(r"[^a-zA-Z]", "", service)
    return clean[:2].upper() if clean else "OT"

def extract_otp(message: str) -> Optional[str]:
    for pat in [r'\b\d{6}\b', r'\b\d{5}\b', r'\b\d{4}\b', r'\d{3}[- ]\d{3}']:
        m = re.search(pat, message)
        if m:
            return m.group(0)
    return None

def get_country_info(number_str: str) -> tuple:
    try:
        if not number_str.startswith("+"):
            number_str = "+" + number_str
        parsed  = phonenumbers.parse(number_str)
        country = geocoder.description_for_number(parsed, "en")
        region  = phonenumbers.region_code_for_number(parsed)
        flag    = "🌍"
        if region and len(region) == 2:
            base = 127462 - ord("A")
            flag = chr(base + ord(region[0])) + chr(base + ord(region[1]))
        return country or "Unknown", flag
    except:
        return "Unknown", "🌍"

def get_region_code(number_str: str) -> str:
    try:
        n = number_str if number_str.startswith("+") else "+" + number_str
        return phonenumbers.region_code_for_number(phonenumbers.parse(n)) or ""
    except:
        return ""

def get_country_code_str(number_str: str) -> str:
    try:
        n = number_str if number_str.startswith("+") else "+" + number_str
        return f"+{phonenumbers.parse(n).country_code}"
    except:
        return ""

def get_last5(number_str: str) -> str:
    digits = re.sub(r"[^0-9]", "", number_str)
    return digits[-5:] if len(digits) >= 5 else digits

def detect_language_from_text(text: str) -> str:
    """Detect language from SMS message content using Unicode script ranges."""
    if not text:
        return None
    # Japanese — Hiragana or Katakana ONLY (not shared CJK)
    if re.search(r'[\u3041-\u3096\u30A1-\u30FA]', text): return "Japanese"
    # Korean — Hangul syllables
    if re.search(r'[\uAC00-\uD7AF]', text):               return "Korean"
    # Chinese — CJK Unified (no Japanese kana present means it's Chinese)
    if re.search(r'[\u4e00-\u9fff\u3400-\u4DBF]', text):  return "Chinese"
    # Arabic / Urdu / Persian
    if re.search(r'[\u0600-\u06FF]', text):                return "Arabic"
    # Russian / Cyrillic
    if re.search(r'[\u0400-\u04FF]', text):                return "Russian"
    # Hindi / Devanagari
    if re.search(r'[\u0900-\u097F]', text):                return "Hindi"
    # Thai
    if re.search(r'[\u0E00-\u0E7F]', text):                return "Thai"
    # Greek
    if re.search(r'[\u0370-\u03FF]', text):                return "Greek"
    # Persian extras
    if re.search(r'[\u06A9\u06AF\u06CC\u06BE]', text):    return "Persian"
    # Hebrew
    if re.search(r'[\u0590-\u05FF]', text):                return "Hebrew"
    # Vietnamese
    if re.search(r'[àáâãèéêìíòóôõùúýăđơưạảấầẩẫậắặẻẽẹềểễệỉịọỏốồổỗộớờởỡợụủứừửữựỳỵỷỹ]', text, re.I):
        return "Vietnamese"
    return None  # fallback to region-based

def format_otp_message(number: str, service: str, otp: str,
                        source_label: str = "", sms_text: str = "") -> str:
    _, flag   = get_country_info(number)
    region    = get_region_code(number)
    dial      = get_country_code_str(number)
    last5     = get_last5(number)
    svc       = get_service_short(service)

    # Detect language: SMS text first, fall back to region
    lang = detect_language_from_text(sms_text)
    if not lang:
        lang = REGION_LANGUAGE.get(region, "English")

    # Format: #WS #DE🇩🇪 +49--Mik--72982 #English
    return (
        f"<b>#{svc} #{region}</b>{flag} "
        f"<code>{dial}--Mik--{last5}</code> "
        f"<b>#{lang}</b>"
    )

def get_otp_keyboard(number: str, otp: str) -> InlineKeyboardMarkup:
    clean_otp = re.sub(r"[^0-9]", "", otp) if otp else otp
    fmt_otp   = f"{clean_otp[:3]}-{clean_otp[3:]}" if clean_otp and len(clean_otp) == 6 else clean_otp
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"🔑 {fmt_otp}", copy_text=CopyTextButton(text=clean_otp))],
        [
            InlineKeyboardButton("📲 Get Number", url="https://t.me/MIKNUMBERBOT"),
            InlineKeyboardButton("📣 Channel",   url="https://t.me/+u-mSc5zdIalhMDM0")
        ],
        [InlineKeyboardButton("💬 WhatsApp", url="https://whatsapp.com/channel/0029Vb7aods60eBdwlqJab40")]
    ])

async def _auto_delete_message(bot, chat_id, message_id, delay_seconds: int = 3600):
    """Wait delay_seconds then delete the message from the chat."""
    await asyncio.sleep(delay_seconds)
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
        logger.info(f"🗑️ Auto-deleted message {message_id} from {chat_id}")
    except Exception as e:
        logger.warning(f"Auto-delete failed for {message_id} in {chat_id}: {e}")

async def send_to_all_groups(msg: str, reply_markup=None):
    config = load_config()
    if not config.get("otp_forward", True):
        STATS["otps_dropped"] += 1
        return
    delay = config.get("forward_delay", 0)
    if delay:
        await asyncio.sleep(delay)
    groups = load_groups()
    if not groups:
        logger.warning("send_to_all_groups: no groups configured!")
        return
    bot = Bot(token=BOT_TOKEN)
    try:
        for gid in groups:
            try:
                sent = await bot.send_message(chat_id=gid, text=msg,
                                              parse_mode="HTML", reply_markup=reply_markup)
                STATS["otps_sent"] += 1
                logger.info(f"✅ OTP sent to group {gid}")
                # Schedule auto-delete after 1 hour
                asyncio.create_task(_auto_delete_message(bot, gid, sent.message_id, 3600))
            except Exception as e:
                STATS["errors"] += 1
                logger.error(f"send_to_all_groups failed for {gid}: {e}")
        log_group = config.get("log_group")
        if log_group:
            try:
                await bot.send_message(chat_id=log_group,
                                       text=f"📡 <b>LOG</b>\n{msg}", parse_mode="HTML")
            except Exception as e:
                logger.warning(f"Log group send failed: {e}")
    finally:
        pass  # keep bot open for delayed deletes — tasks hold their own reference

async def send_otp_to_owner(number: str, service: str, otp: str,
                             sms_text: str = "", source_label: str = ""):
    """Send OTP privately to the user who was assigned this number."""
    owner_id  = db_get_owner_of_number(number)
    if not owner_id:
        logger.debug(f"send_otp_to_owner: no owner found for {number}")
        return
    from html import escape as _e
    clean_otp = re.sub(r"[^0-9]", "", otp) if otp and otp != "N/A" else (otp or "N/A")
    _, flag   = get_country_info(number)
    region    = get_region_code(number)
    dial      = get_country_code_str(number)
    last5     = get_last5(number)
    svc       = get_service_short(service)
    lang      = detect_language_from_text(sms_text) or REGION_LANGUAGE.get(region, "English")
    fmt_otp   = f"{clean_otp[:3]}-{clean_otp[3:]}" if clean_otp and len(clean_otp) == 6 else clean_otp

    msg = (
        f"<b>#{svc} #{region}</b>{flag} "
        f"<code>{dial}--Mik--{last5}</code> "
        f"<b>#{lang}</b>"
    )

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"🔑 {fmt_otp}", copy_text=CopyTextButton(text=clean_otp))],
        [
            InlineKeyboardButton("📲 Get Number", url="https://t.me/MIKNUMBERBOT"),
            InlineKeyboardButton("📣 Channel",   url="https://t.me/+u-mSc5zdIalhMDM0")
        ],
        [InlineKeyboardButton("💬 WhatsApp", url="https://whatsapp.com/channel/0029Vb7aods60eBdwlqJab40")]
    ])
    bot = Bot(token=BOT_TOKEN)
    try:
        await bot.send_message(chat_id=owner_id, text=msg,
                               parse_mode="HTML", reply_markup=kb)
        logger.info(f"✅ OTP DM sent to user {owner_id} for ...{last5}")
    except Exception as e:
        logger.warning(f"Could not DM user {owner_id}: {e}")
    finally:
        await bot.close()

async def broadcast_to_all_users(msg: str, reply_markup=None):
    """Broadcast a message to all users who have started the bot."""
    c = db.cursor()
    c.execute("SELECT user_id FROM tg_users")
    user_ids = [row[0] for row in c.fetchall()]
    bot = Bot(token=BOT_TOKEN)
    sent = 0
    failed = 0
    try:
        for user_id in user_ids:
            try:
                await bot.send_message(chat_id=user_id, text=msg,
                                       parse_mode="HTML", reply_markup=reply_markup)
                sent += 1
                await asyncio.sleep(0.05)  # avoid flood limits
            except Exception:
                failed += 1
    finally:
        await bot.close()
    logger.info(f"Broadcast done: {sent} sent, {failed} failed")
    return sent, failed

def log_otp_memory(number: str, service: str, otp: str, source: str):
    global OTP_LOG
    OTP_LOG.append({"number": number, "service": service, "otp": otp or "N/A",
                    "source": source, "time": datetime.now().strftime("%H:%M:%S")})
    if len(OTP_LOG) > OTP_LOG_MAX:
        OTP_LOG = OTP_LOG[-OTP_LOG_MAX:]
    db_save_otp_history(number, service, otp, source)

# ═══════════════════════════════════════════════════════════════
#  REST PANEL FETCH
# ═══════════════════════════════════════════════════════════════
def _parse_mdr_response(data: dict, limit: int = 1) -> list:
    """Parse MOAPI MDR-style response. Returns list of normalised dicts."""
    results = []
    if isinstance(data, dict) and data.get("status", "").lower() == "success":
        for row in data.get("data", [])[:limit]:
            results.append({
                "time":    row.get("datetime", ""),
                "number":  row.get("number",   ""),
                "service": row.get("cli",       "Unknown"),
                "message": row.get("message",   ""),
            })
    return results

def _build_mdr_params(cfg: dict) -> dict:
    """Build query-string params for the MDR API."""
    now   = datetime.now()
    start = (now - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
    end   = now.strftime("%Y-%m-%d %H:%M:%S")
    return {
        "token":        cfg["token"],
        "fromdate":     start,
        "todate":       end,
        "searchnumber": cfg.get("searchnumber", ""),
        "searchcli":    cfg.get("searchcli",    ""),
        "records":      cfg.get("records", 20),
    }

def fetch_latest(panel_name: str) -> Optional[Dict]:
    if panel_name not in API_PANELS:
        return None
    cfg      = API_PANELS[panel_name]
    api_type = cfg.get("api_type", "sami")
    try:
        if api_type == "mdr":
            # ── MOAPI MDR format ──────────────────────────────────────
            url    = cfg.get("url", MDR_API_URL)
            params = _build_mdr_params(cfg)
            r      = requests.get(url, params=params, timeout=10)
            ct     = r.headers.get("content-type", "")
            if "application/json" not in ct:
                return None
            rows = _parse_mdr_response(r.json(), limit=1)
            return rows[0] if rows else None
        else:
            # ── Original sami format ──────────────────────────────────
            r  = requests.get(cfg["url"],
                              params={"token": cfg["token"], "records": cfg.get("records", 20)},
                              timeout=10)
            ct = r.headers.get("content-type", "")
            if "application/json" not in ct:
                return None
            data = r.json()
            if isinstance(data, dict) and data.get("status") == "success":
                rows = data.get("data", [])
                if rows:
                    row = rows[0]
                    return {"time": row.get("dt",""), "number": row.get("num",""),
                            "service": row.get("cli",""), "message": row.get("message","")}
            elif isinstance(data, list) and data:
                row = data[0]
                if len(row) >= 4:
                    return {"time": row[3], "number": row[1],
                            "service": row[0] or "Unknown", "message": row[2]}
            return None
    except Exception as e:
        STATS["errors"] += 1
        logger.error(f"Fetch error {panel_name}: {e}")
        return None

def fetch_all_panels(limit: int = 5) -> list:
    results = []
    for panel_name, cfg in API_PANELS.items():
        api_type = cfg.get("api_type", "sami")
        try:
            if api_type == "mdr":
                url    = cfg.get("url", MDR_API_URL)
                params = _build_mdr_params(cfg)
                params["records"] = limit
                r  = requests.get(url, params=params, timeout=10)
                ct = r.headers.get("content-type", "")
                if "application/json" not in ct:
                    continue
                rows = _parse_mdr_response(r.json(), limit=limit)
                for row in rows:
                    results.append({"panel": panel_name, **row})
            else:
                r  = requests.get(cfg["url"],
                                  params={"token": cfg["token"], "records": limit},
                                  timeout=10)
                ct = r.headers.get('content-type', '')
                if 'application/json' not in ct:
                    continue
                data = r.json()
                if isinstance(data, dict) and data.get("status") == "success":
                    for row in data.get("data", [])[:limit]:
                        results.append({"panel": panel_name, "time": row.get("dt",""),
                                        "number": row.get("num",""), "service": row.get("cli",""),
                                        "message": row.get("message","")})
                elif isinstance(data, list):
                    for row in data[:limit]:
                        if len(row) >= 4:
                            results.append({"panel": panel_name, "time": row[3], "number": row[1],
                                            "service": row[0] or "Unknown", "message": row[2]})
        except Exception as e:
            logger.error(f"fetch_all_panels error {panel_name}: {e}")
    return results

# ═══════════════════════════════════════════════════════════════
#  EXCEPTION HANDLER
# ═══════════════════════════════════════════════════════════════
def handle_task_exception(task: asyncio.Task):
    try:
        task.result()
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error(f"Task {task.get_name()} exception: {e}", exc_info=True)

# ═══════════════════════════════════════════════════════════════
#  IVAS WEBSOCKET WORKER
# ═══════════════════════════════════════════════════════════════
async def _ivas_ping(ws, interval_ms):
    while True:
        await asyncio.sleep(interval_ms / 1000)
        try:
            await ws.send("3")
        except:
            break

async def ivas_worker(name: str):
    logger.info(f"🔌 IVAS worker starting: {name}")
    seen = set()
    while True:
        try:
            accounts = load_ivas()
            if name not in accounts:
                logger.info(f"IVAS '{name}' removed — stopping.")
                break
            uri = accounts[name].get("uri", "")
            if not uri:
                await asyncio.sleep(10)
                continue
            ssl_ctx = ssl._create_unverified_context()
            try:
                async with websockets.connect(uri, ssl=ssl_ctx) as ws:
                    logger.info(f"✅ IVAS [{name}] connected.")
                    initial = await ws.recv()
                    ping_interval = 25000
                    try:
                        if initial.startswith("0{"):
                            ping_interval = json.loads(initial[1:]).get("pingInterval", 25000)
                    except:
                        pass
                    await ws.send("40/livesms,")
                    ping_task = asyncio.create_task(_ivas_ping(ws, ping_interval))
                    try:
                        while True:
                            if name not in load_ivas():
                                break
                            msg = await ws.recv()
                            if not msg.startswith("42/livesms,"):
                                continue
                            try:
                                data = json.loads(msg[msg.find("["):])
                                if not (isinstance(data, list) and len(data) > 1
                                        and isinstance(data[1], dict)):
                                    continue
                                sms     = data[1]
                                number  = sms.get("recipient", "")
                                text    = sms.get("message", "") or ""
                                service = sms.get("originator", "Unknown")
                                country = sms.get("range", "")
                                otp_m   = re.search(r"\b\d{3,6}(?:[- ]\d{2,6})?\b", text)
                                otp     = otp_m.group(0) if otp_m else None
                                uniq    = f"{number}-{text[:20]}"
                                if uniq in seen:
                                    continue
                                seen.add(uniq)
                                if len(seen) > 500:
                                    seen.clear()
                                STATS["ivas_hits"][name] = STATS["ivas_hits"].get(name, 0) + 1
                                log_otp_memory(number, service, otp, f"IVAS:{name}")
                                if otp and number:
                                    store = load_otp_store()
                                    store[number] = otp
                                    save_otp_store(store)
                                formatted = format_otp_message(number, service, otp or "N/A",
                                    source_label=f"IVAS:{name}", sms_text=text)
                                keyboard = get_otp_keyboard(number, otp) if otp else None
                                await send_to_all_groups(formatted, reply_markup=keyboard)
                                # DM the user who owns this number
                                if otp:
                                    await send_otp_to_owner(number, service, otp,
                                        sms_text=text, source_label=f"IVAS:{name}")
                            except Exception as e:
                                logger.error(f"IVAS [{name}] parse error: {e}")
                    finally:
                        ping_task.cancel()
            except websockets.exceptions.WebSocketException as e:
                logger.error(f"IVAS [{name}] WS error: {e}. Retry in 5s...")
                await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"IVAS [{name}] error: {e}. Retry in 5s...")
                await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"IVAS [{name}] critical: {e}. Retry in 10s...")
            await asyncio.sleep(10)

# ═══════════════════════════════════════════════════════════════
#  REST API WORKER
# ═══════════════════════════════════════════════════════════════
async def api_worker(panel: str):
    last_uniq = None
    logger.info(f"📡 REST worker starting: {panel}")
    while True:
        try:
            if panel not in API_PANELS:
                break
            data = fetch_latest(panel)
            if data:
                uniq = f"{data['number']}-{data['message'][:15]}"
                if uniq != last_uniq:
                    last_uniq = uniq
                    otp = extract_otp(data["message"])
                    STATS["panel_hits"][panel] = STATS["panel_hits"].get(panel, 0) + 1
                    log_otp_memory(data["number"], data["service"], otp, f"REST:{panel}")
                    if otp and data["number"]:
                        store = load_otp_store()
                        store[data["number"]] = otp
                        save_otp_store(store)
                    formatted = format_otp_message(
                        data["number"], data["service"], otp or "N/A",
                        source_label=f"REST:{panel}", sms_text=data.get("message", ""))
                    keyboard = get_otp_keyboard(data["number"], otp) if otp else None
                    await send_to_all_groups(formatted, reply_markup=keyboard)
                    # DM the user who owns this number
                    if otp:
                        await send_otp_to_owner(data["number"], data["service"], otp,
                            sms_text=data.get("message",""), source_label=f"REST:{panel}")
        except Exception as e:
            logger.error(f"REST worker error {panel}: {e}")
        await asyncio.sleep(4)

# ═══════════════════════════════════════════════════════════════
#  MONITOR & CLEANUP
# ═══════════════════════════════════════════════════════════════
async def monitor_tasks():
    while True:
        await asyncio.sleep(60)
        for name in load_ivas():
            if name not in IVAS_TASKS or IVAS_TASKS[name].done():
                logger.warning(f"IVAS '{name}' dead — restarting...")
                task = asyncio.create_task(ivas_worker(name), name=f"IVAS-{name}")
                task.add_done_callback(handle_task_exception)
                IVAS_TASKS[name] = task
        for panel in list(API_PANELS.keys()):
            if panel not in REST_TASKS or REST_TASKS[panel].done():
                logger.warning(f"REST '{panel}' dead — restarting...")
                task = asyncio.create_task(api_worker(panel), name=f"REST-{panel}")
                task.add_done_callback(handle_task_exception)
                REST_TASKS[panel] = task

async def cleanup_states():
    while True:
        await asyncio.sleep(60)
        now = time.time()
        for d in [PANEL_ADD_STATES, IVAS_ADD_STATES, BROADCAST_STATES,
                  SETTING_STATES, FETCH_STATES, USER_PREFIX_STATES]:
            for uid in list(d.keys()):
                if now - d[uid].get("timestamp", 0) > STATE_TIMEOUT:
                    del d[uid]

# ═══════════════════════════════════════════════════════════════
#  KEYBOARDS
# ═══════════════════════════════════════════════════════════════
def b(text, cb=None, url=None):
    return InlineKeyboardButton(text, callback_data=cb) if cb else InlineKeyboardButton(text, url=url)

def get_main_menu_keyboard(user_id: int = 0):
    return InlineKeyboardMarkup([
        [b("🧇 𝗚𝗲𝘁 𝗡𝘂𝗺𝗯𝗲𝗿", "show_countries"),  b("🫁 𝗣𝗿𝗼𝗳𝗶𝗹𝗲", "user_profile")],
        [b("🧠 𝗗𝗲𝘃𝗲𝗹𝗼𝗽𝗲𝗿", url="https://t.me/elitetrade11")],
    ])

def get_join_keyboard():
    return InlineKeyboardMarkup([
        [b("📢 OTP Group",           url="https://t.me/+0YairwHa63NkNWM0")],
        [b("💬 Chat & Support",       url="https://t.me/+wksiFMvdngdjNGY0")],
        [b("📣 Main Channel",         url="https://t.me/+u-mSc5zdIalhMDM0")],
        [b("📌 Backup Channel",       url="https://t.me/+Rf1oYlInYOA5ZTk0")],
        [b("👥 Backup Group",         url="https://t.me/+Qa_cZA0ikrU5ZDU0")],
        [b("🔧 Method Channel",       url="https://t.me/realsurvices1")],
        [b("📁 All Groups Folder",    url="https://t.me/addlist/66eMAe6ietM4NTc1")],
        [b("✅ I Joined — Check", "check_join")],
    ])

def get_admin_keyboard(uid: int = 0):
    """Build admin panel buttons based on what the user has permission for."""
    rows = []
    # Row: Numbers + Files
    row = []
    if has_perm(uid, "numbers"): row.append(b("📦 Numbers",    "menu_numbers"))
    if has_perm(uid, "files"):   row.append(b("📂 Files",      "menu_files"))
    if row: rows.append(row)
    # Row: Panels + IVAS
    row = []
    if has_perm(uid, "panels"):  row.append(b("📡 Panels",     "menu_panels"))
    if has_perm(uid, "ivas"):    row.append(b("🔌 IVAS",       "menu_ivas"))
    if row: rows.append(row)
    # Row: Fetch SMS + OTP History
    row = []
    if has_perm(uid, "fetch_sms"):    row.append(b("🔄 Fetch SMS",    "menu_fetch"))
    if has_perm(uid, "otp_history"):  row.append(b("📋 OTP History",  "menu_otp_history"))
    if row: rows.append(row)
    # Row: Groups + Broadcast
    row = []
    if has_perm(uid, "groups"):    row.append(b("👥 Groups",    "menu_groups"))
    if has_perm(uid, "broadcast"): row.append(b("📢 Broadcast", "broadcast"))
    if row: rows.append(row)
    # Row: Settings + Advanced (owner/full only)
    row = []
    if has_perm(uid, "settings"):  row.append(b("⚙️ Settings",  "menu_settings"))
    if has_perm(uid, "advanced"):  row.append(b("🔧 Advanced",  "menu_advanced"))
    if row: rows.append(row)
    # Row: Stats always shown
    if has_perm(uid, "stats"):
        rows.append([b("📊 Stats", "stats"), b("📡 Status", "status")])
    rows.append([b("🔄 Refresh", "refresh_admin"), b("❌ Close", "close_admin")])
    return InlineKeyboardMarkup(rows)

def get_numbers_menu():
    return InlineKeyboardMarkup([
        [b("➕ Add Numbers","nb_add_numbers"),  b("📋 View Stock","nb_number_list")],
        [b("🗑️ Delete Country","nb_delete_menu"),b("📤 Export","nb_export")],
        [b("📊 Stock Stats","nb_stock_stats"),  b("🔙 Back","back_to_admin")],
    ])

def get_number_manager_keyboard():
    """Restricted keyboard — maps to the same nb_ handlers full admins use."""
    return InlineKeyboardMarkup([
        [b("➕ Add Numbers",    "nb_add_numbers"),  b("📋 View Stock",   "nb_number_list")],
        [b("🗑️ Delete Country","nb_delete_menu"),   b("📊 Stock Stats",  "nb_stock_stats")],
    ])

def get_files_menu():
    return InlineKeyboardMarkup([
        [b("📂 List Files","fm_list"),          b("📥 Download Log","fm_download_log")],
        [b("📥 Download OTP Store","fm_download_otp"),b("📥 Download DB","fm_download_db")],
        [b("🗑️ Clear Log","fm_clear_log"),      b("🔙 Back","back_to_admin")],
    ])

def get_panels_menu():
    return InlineKeyboardMarkup([
        [b("📋 List Panels","list_panels"),     b("➕ Add Panel","add_panel")],
        [b("🗑️ Remove Panel","remove_panel"),  b("🧪 Test All","test_panels_menu")],
        [b("🔄 Fetch Latest","panel_fetch_all"),b("🔙 Back","back_to_admin")],
    ])

def get_panels_keyboard(action="view"):
    panels  = load_panels()
    keyboard = []
    for name in panels:
        active = (name in REST_TASKS and not REST_TASKS[name].done())
        st     = "🟢" if active else "🔴"
        cb     = f"remove_panel_{name}" if action == "remove" else f"view_panel_{name}"
        label  = f"{st} {name.upper()}" + (" 🗑️" if action == "remove" else "")
        keyboard.append([b(label, cb)])
    keyboard.append([b("🔙 Back","menu_panels")])
    return InlineKeyboardMarkup(keyboard)

def get_ivas_menu():
    return InlineKeyboardMarkup([
        [b("📋 List IVAS","list_ivas"),         b("➕ Add IVAS","add_ivas")],
        [b("🗑️ Remove IVAS","remove_ivas"),     b("🔄 Restart All","ivas_restart_all")],
        [b("🔙 Back","back_to_admin")],
    ])

def get_ivas_keyboard(action="view"):
    accounts = load_ivas()
    keyboard = []
    for name in accounts:
        active = (name in IVAS_TASKS and not IVAS_TASKS[name].done())
        st     = "🟢" if active else "🔴"
        cb     = f"remove_ivas_{name}" if action == "remove" else f"view_ivas_{name}"
        label  = f"{st} {name.upper()}" + (" 🗑️" if action == "remove" else "")
        keyboard.append([b(label, cb)])
    keyboard.append([b("🔙 Back","menu_ivas")])
    return InlineKeyboardMarkup(keyboard)

def get_groups_menu():
    groups  = load_groups()
    config  = load_config()
    keyboard = []
    for gid in groups:
        keyboard.append([b(f"🗑️ Remove {gid}", f"del_group_{gid}")])
    keyboard.append([b("➕ Add Group","add_group_prompt")])
    keyboard.append([b("📋 Set Log Group","set_log_group"),
                     b("❌ Clear Log Group","clear_log_group")])
    keyboard.append([b("🔙 Back","back_to_admin")])
    return InlineKeyboardMarkup(keyboard)

def get_otp_history_menu():
    return InlineKeyboardMarkup([
        [b("📋 Last 10","otp_hist_10"),         b("📋 Last 20","otp_hist_20")],
        [b("🔍 Search by Number","otp_search_num"),b("📤 Export CSV","otp_export_hist")],
        [b("🗑️ Clear History","otp_clear_hist"),b("🔙 Back","back_to_admin")],
    ])

def get_fetch_menu():
    return InlineKeyboardMarkup([
        [b("🔄 Fetch All Panels","fetch_all_now")],
        [b("🔍 Fetch by Number","fetch_by_number")],
        [b("📡 Fetch Single Panel","fetch_single_panel")],
        [b("🔙 Back","back_to_admin")],
    ])

def get_settings_menu():
    config = load_config()
    fwd    = "✅ ON" if config.get("otp_forward", True) else "❌ OFF"
    delay  = config.get("forward_delay", 0)
    lg     = str(config.get("log_group") or "None")[:12]
    prefix = config.get("number_prefix", "") or "None"
    return InlineKeyboardMarkup([
        [b(f"📤 OTP Forward: {fwd}","toggle_otp_forward")],
        [b(f"⏱ Delay: {delay}s","set_forward_delay"),
         b("📢 Channel Link","set_channel")],
        [b("🤖 NumberBot Link","set_numberbot"),
         b(f"📋 Log Group: {lg}","set_log_group")],
        [b("🔗 OTP Group Link","set_otp_group_link"),
         b("👤 Admin Manager","menu_admin_manager")],
        [b(f"🔢 Number Prefix: {prefix}","set_number_prefix")],
        [b("🔙 Back","back_to_admin")],
    ])

def get_admin_manager_keyboard():
    staff    = load_staff()
    keyboard = []
    # Owners (non-editable)
    for aid in OWNER_IDS:
        keyboard.append([b(f"👑 {aid}  (Owner)", "noop")])
    # Staff members
    for uid_str, info in staff.items():
        uid_int = int(uid_str)
        if uid_int in OWNER_IDS:
            continue
        name  = info.get("name", uid_str)
        perms = info.get("perms", [])
        keyboard.append([
            b(f"🛡️ {name} ({len(perms)} perms)", f"edit_staff_{uid_str}"),
            b("🗑️", f"remove_staff_{uid_str}")
        ])
    keyboard.append([b("➕ Add Staff Member", "add_staff_prompt")])
    keyboard.append([b("🔙 Back", "menu_settings")])
    return InlineKeyboardMarkup(keyboard)

def get_staff_perms_keyboard(uid_str: str):
    """Toggle keyboard for editing a staff member's permissions."""
    staff   = load_staff()
    info    = staff.get(uid_str, {})
    cur     = info.get("perms", [])
    keyboard = []
    for perm_key, perm_label in ALL_PERMISSIONS.items():
        has = perm_key in cur
        icon = "✅" if has else "☑️"
        keyboard.append([b(f"{icon} {perm_label}", f"toggle_perm_{uid_str}_{perm_key}")])
    keyboard.append([
        b("✅ Grant All",  f"grant_all_{uid_str}"),
        b("❌ Revoke All", f"revoke_all_{uid_str}")
    ])
    keyboard.append([b("🔙 Back", "menu_admin_manager")])
    return InlineKeyboardMarkup(keyboard)

def get_advanced_keyboard():
    return InlineKeyboardMarkup([
        [b("🔄 Restart All Workers","restart_workers"),
         b("🛑 Stop Forwarding","stop_forward")],
        [b("▶️ Start Forwarding","start_forward"),
         b("🔄 Reload Config","reload_config")],
        [b("🗑️ Clear OTP Store","clear_all_otps"),
         b("📤 Export OTP Store","export_otps")],
        [b("📋 View Logs","view_logs"),
         b("🔁 Restart Bot","restart_bot")],
        [b("🧪 Test All Panels","test_panels_adv"),
         b("📊 Worker Status","worker_status")],
        [b("🔙 Back","back_to_admin")],
    ])

def get_confirmation_keyboard(action: str, extra: str = ""):
    cd = f"confirm_{action}" + (f"_{extra}" if extra else "")
    return InlineKeyboardMarkup([
        [b("✅ Confirm", cd), b("❌ Cancel","cancel_action")]
    ])

def get_broadcast_keyboard():
    return InlineKeyboardMarkup([
        [b("📝 Text Only","broadcast_text"),
         b("🔘 With Buttons","broadcast_with_buttons")],
        [b("🔙 Back","back_to_admin")]
    ])

SERVICE_ICONS = {
    "WhatsApp": "📱", "Facebook": "📘", "Telegram": "✈️", "Instagram": "📸",
    "Twitter": "🐦", "TikTok": "🎵", "Google": "🔍", "Gmail": "📧",
    "Snapchat": "👻", "Viber": "📞", "Line": "💬", "WeChat": "🟢",
    "Signal": "🔒", "Discord": "🎮", "LinkedIn": "💼", "Bumble": "🐝",
    "Tinder": "🔥", "Hinge": "💛", "Binance": "🪙", "Coinbase": "💰",
}

def get_services_keyboard(back_cb: str = "back_to_main"):
    services = db_get_services()
    if not services:
        return None
    buttons = []
    for svc in services:
        icon = SERVICE_ICONS.get(svc, "📱")
        buttons.append([b(f"{icon} {svc}", f"svc_select|{svc}")])
    buttons.append([b("🔙 Back", back_cb)])
    return InlineKeyboardMarkup(buttons)

def get_countries_keyboard(service: str = ""):
    rows = db_get_countries(service)
    if not rows:
        return None
    buttons = [b(f"{FLAG_MAP.get(c,'🌍')} {c} ({n})", f"nb_get|{c}") for c, n in rows]
    kb = [buttons[i:i+2] for i in range(0, len(buttons), 2)]
    kb.append([b("🔄 Refresh", f"svc_select|{service}" if service else "show_countries")])
    return InlineKeyboardMarkup(kb)

def get_nb_stock_keyboard():
    rows     = db_get_countries()
    keyboard = [[b(f"❌ {c} ({n})  Delete", f"nb_del|{c}")] for c, n in rows]
    keyboard.append([b("🔙 Back","menu_numbers")])
    return InlineKeyboardMarkup(keyboard)

# ═══════════════════════════════════════════════════════════════
#  COMMANDS
# ═══════════════════════════════════════════════════════════════
async def check_join(bot, uid: int) -> bool:
    """Check if user has joined all required channels. Returns True if all joined."""
    for ch in REQUIRED_CHANNELS:
        ch_id = ch.get("id")
        if not ch_id:
            continue  # private invite link — can't check membership, skip
        try:
            member = await bot.get_chat_member(ch_id, uid)
            if member.status in ("left", "kicked"):
                return False
        except Exception:
            return False
    return True

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    from html import escape
    uid   = update.effective_user.id
    first = escape(update.effective_user.first_name or "User")
    db_add_user(uid)

    # Skip join check for admins
    if not is_admin(uid):
        joined = await check_join(context.bot, uid)
        if not joined:
            await update.message.reply_text(
                f"👋 <b>HI {first}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"⚠️ <b>Join required channels first!</b>\n\n"
                f"Please join both channels below then tap <b>I Joined</b>.",
                parse_mode="HTML",
                reply_markup=get_join_keyboard())
            return

    msg = (
        f"👋 <b>HI {first}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"💎 <b>MIK OTP BOT</b>\n"
        f"⚡ Fastest OTP Service\n"
        f"🤖 Auto-Assign System\n"
        f"🧬 Multi-Panel + IVAS Support\n\n"
        f"🆔 <b>Version :</b> 7.0\n\n"
        f"👆 <b>Select an option:</b>"
    )
    if is_admin(uid):
        msg += "\n\n🛡️ <b>ADMIN</b> — /admin"
    await update.message.reply_text(msg, parse_mode="HTML",
                                    reply_markup=get_main_menu_keyboard(uid))

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_admin(uid):
        await update.message.reply_text("❌ Unauthorized!")
        return
    role = "👑 Owner" if is_owner(uid) else "🛡️ Admin"
    msg = (
        f"📖 <b>MIK OTP BOT — COMMAND LIST</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>Role:</b> {role}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"<b>🔧 General Commands</b>\n"
        f"/start — Start the bot & show main menu\n"
        f"/help — Show this command list <i>(Admin only)</i>\n"
        f"/admin — Open admin panel\n\n"
        f"<b>📡 OTP & SMS Commands</b>\n"
        f"/otpfor [number] — Search OTP for a specific number\n"
        f"  Example: <code>/otpfor 923312443021</code>\n"
        f"/fetchsms — Fetch latest SMS from all panels\n\n"
        f"<b>📊 Status & Stats Commands</b>\n"
        f"/status — Show bot status (uptime, panels, IVAS)\n"
        f"/stats — Show user & OTP statistics\n\n"
        f"<b>👥 Group Management Commands</b>\n"
        f"/addgroup [chat_id] — Add an OTP group to forward to\n"
        f"  Example: <code>/addgroup -1001234567890</code>\n"
        f"/removegroup [chat_id] — Remove a group from forwarding\n"
        f"  Example: <code>/removegroup -1001234567890</code>\n\n"
        f"<b>⚙️ System Commands</b>\n"
        f"/reload — Reload all REST & IVAS workers\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>🖥️ Admin Panel Sections</b>\n"
        f"📦 <b>Numbers</b> — Add/delete/view/export phone numbers\n"
        f"📂 <b>Files</b> — Download logs, OTP store, database\n"
        f"📡 <b>Panels</b> — Add/remove/test REST API panels\n"
        f"🔌 <b>IVAS</b> — Add/remove/restart WebSocket IVAS accounts\n"
        f"👥 <b>Groups</b> — Manage OTP forwarding groups\n"
        f"📢 <b>Broadcast</b> — Send message to all users + groups\n"
        f"📋 <b>OTP History</b> — View/search/export OTP history\n"
        f"🔄 <b>Fetch SMS</b> — Manually fetch SMS from panels\n"
        f"⚙️ <b>Settings</b> — Toggle forwarding, set prefix, manage staff\n"
        f"🔧 <b>Advanced</b> — Restart workers, clear OTP store, view logs\n"
        f"📊 <b>Stats/Status</b> — Live bot statistics\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🤖 <b>Dev:</b> {DEV_CONTACT}"
    )
    await update.message.reply_text(msg, parse_mode="HTML")

async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if is_admin(uid):
        await update.message.reply_text(
            f"💎 <b>ADMIN PANEL</b>\n━━━━━━━━━━━━━━━━━━━━━━\nSelect option:",
            parse_mode="HTML", reply_markup=get_admin_keyboard(uid))
    else:
        await update.message.reply_text("❌ Unauthorized!")

async def otpfor_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("❌ Use: /otpfor 447123456789")
        return
    target = context.args[0].replace("+", "")
    wait   = await update.message.reply_text(f"🔄 Scanning <code>{target}</code>...", parse_mode="HTML")
    found  = None
    store  = load_otp_store()
    for k, v in store.items():
        if target in k:
            found = v
            break
    if not found:
        rows = db_search_otp_by_number(target)
        if rows:
            found = rows[0][2]
    if not found:
        for panel in API_PANELS:
            d2 = fetch_latest(panel)
            if d2 and target in d2["number"]:
                found = extract_otp(d2["message"])
                if found:
                    break
    if found:
        await wait.edit_text(
            f"✅ <b>OTP FOUND</b>\n━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📞 <code>{target}</code>\n🔑 <code>{found}</code>", parse_mode="HTML")
    else:
        await wait.edit_text(
            f"❌ <b>No OTP found</b> for <code>{target}</code>", parse_mode="HTML")

async def fetchsms_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Unauthorized!")
        return
    wait    = await update.message.reply_text("🔄 Fetching...")
    results = fetch_all_panels(limit=5)
    if not results:
        await wait.edit_text("❌ No SMS fetched.")
        return
    text = f"📨 <b>LATEST SMS ({len(results)})</b>\n━━━━━━━━━━━━━━━━━━━━━━\n"
    for r in results[:10]:
        otp  = extract_otp(r["message"]) or "N/A"
        text += (f"\n📡 <b>{r['panel']}</b> | {r['service']}\n"
                 f"📞 <code>{r['number']}</code>\n"
                 f"🔑 OTP: <code>{otp}</code>\n"
                 f"💬 {r['message'][:80]}\n──────────────────────\n")
    await wait.edit_text(text[:4000], parse_mode="HTML")

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Unauthorized!")
        return
    await update.message.reply_text(_build_status(), parse_mode="HTML")

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Unauthorized!")
        return
    total, active_today, active_week = db_user_stats()
    uptime = str(datetime.now() - datetime.fromtimestamp(STATS['start_time'])).split('.')[0]
    await update.message.reply_text(
        f"📊 <b>BOT STATS</b>\n━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⏱ Uptime: <code>{uptime}</code>\n"
        f"👤 Total Users: <code>{total}</code>\n"
        f"🟢 Active Today: <code>{active_today}</code>\n"
        f"📅 This Week: <code>{active_week}</code>\n"
        f"📊 OTPs Sent: <code>{STATS['otps_sent']}</code>\n"
        f"🚫 Dropped: <code>{STATS['otps_dropped']}</code>\n"
        f"❌ Errors: <code>{STATS['errors']}</code>\n"
        f"📦 Numbers in DB: <code>{db_total_numbers()}</code>\n"
        f"🗄 OTP Store: <code>{len(load_otp_store())}</code>\n"
        f"📡 REST Panels: <code>{len(load_panels())}</code>\n"
        f"🔌 IVAS Accounts: <code>{len(load_ivas())}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━", parse_mode="HTML")

async def addgroup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Unauthorized!")
        return
    if not context.args:
        await update.message.reply_text("Usage: /addgroup <chat_id>")
        return
    try:
        gid    = int(context.args[0])
        groups = load_groups()
        if gid in groups:
            await update.message.reply_text("🟡 Already exists.")
            return
        groups.append(gid)
        save_groups(groups)
        await update.message.reply_text(f"✅ Group <code>{gid}</code> added.", parse_mode="HTML")
    except ValueError:
        await update.message.reply_text("❌ Invalid chat ID.")

async def removegroup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Unauthorized!")
        return
    if not context.args:
        await update.message.reply_text("Usage: /removegroup <chat_id>")
        return
    try:
        gid    = int(context.args[0])
        groups = load_groups()
        if gid not in groups:
            await update.message.reply_text("❌ Not found.")
            return
        groups.remove(gid)
        save_groups(groups)
        await update.message.reply_text("✅ Group removed.")
    except ValueError:
        await update.message.reply_text("❌ Invalid chat ID.")

async def reload_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Unauthorized!")
        return
    await _restart_all_workers()
    await update.message.reply_text(
        f"✅ Reloaded — REST: {len(API_PANELS)}, IVAS: {len(load_ivas())}, Groups: {len(load_groups())}")



# ═══════════════════════════════════════════════════════════════
#  INTERNAL HELPERS
# ═══════════════════════════════════════════════════════════════
def _build_status() -> str:
    uptime = str(datetime.now() - datetime.fromtimestamp(STATS['start_time'])).split('.')[0]
    config = load_config()
    fwd_st = "✅ ON" if config.get("otp_forward", True) else "❌ OFF"
    panels = load_panels()
    ivas   = load_ivas()
    plines = "\n".join([
        f"  {'🟢' if (p in REST_TASKS and not REST_TASKS[p].done()) else '🔴'} "
        f"{p} (hits:{STATS['panel_hits'].get(p,0)})"
        for p in panels]) or "  None"
    ilines = "\n".join([
        f"  {'🟢' if (n in IVAS_TASKS and not IVAS_TASKS[n].done()) else '🔴'} "
        f"{n} (hits:{STATS['ivas_hits'].get(n,0)})"
        for n in ivas]) or "  None"
    return (
        f"🖥 <b>BOT STATUS</b>\n━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⏱ Uptime: <code>{uptime}</code>\n"
        f"📊 OTPs Sent: <code>{STATS['otps_sent']}</code>\n"
        f"❌ Errors: <code>{STATS['errors']}</code>\n"
        f"📤 Forward: {fwd_st} | Groups: {len(load_groups())}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📡 <b>REST Panels:</b>\n{plines}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔌 <b>IVAS Accounts:</b>\n{ilines}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━"
    )

async def _restart_all_workers():
    global API_PANELS
    API_PANELS = load_panels()
    for panel in list(REST_TASKS.keys()):
        REST_TASKS[panel].cancel()
        del REST_TASKS[panel]
    for panel in API_PANELS:
        task = asyncio.create_task(api_worker(panel), name=f"REST-{panel}")
        task.add_done_callback(handle_task_exception)
        REST_TASKS[panel] = task
    for name in list(IVAS_TASKS.keys()):
        IVAS_TASKS[name].cancel()
        del IVAS_TASKS[name]
    for name in load_ivas():
        task = asyncio.create_task(ivas_worker(name), name=f"IVAS-{name}")
        task.add_done_callback(handle_task_exception)
        IVAS_TASKS[name] = task

# ═══════════════════════════════════════════════════════════════
#  CALLBACK QUERY HANDLER
# ═══════════════════════════════════════════════════════════════
async def callback_query_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global API_PANELS
    query = update.callback_query
    try:
        await query.answer()
    except Exception:
        pass
    uid  = query.from_user.id
    data = query.data

    try:
        await _callback_query_inner(update, context, query, uid, data)
    except Exception as e:
        logger.error(f"Callback error [{data}]: {e}", exc_info=True)
        try:
            await query.answer(f"❌ Error: {str(e)[:100]}", show_alert=True)
        except Exception:
            pass

async def _callback_query_inner(update, context, query, uid, data):
    global API_PANELS

    if data == "noop":
        return

    # ── Public ────────────────────────────────────────────────
    if data.startswith("copy_"):
        await query.answer(f"🔑 {data[5:]}", show_alert=True)
        return

    if data == "show_help":
        role = "👑 Owner" if is_owner(uid) else "🛡️ Admin" if is_admin(uid) else "👤 User"
        msg = (
            f"📖 <b>MIK OTP BOT — COMMAND LIST</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>Role:</b> {role}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"<b>🔧 General Commands</b>\n"
            f"/start — Start the bot\n"
            f"/admin — Open admin panel\n\n"
            f"<b>📡 OTP & SMS Commands</b>\n"
            f"/otpfor [number] — Search OTP\n"
            f"/fetchsms — Fetch latest SMS\n\n"
            f"<b>📊 Status Commands</b>\n"
            f"/status — Bot status\n"
            f"/stats — Statistics\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🤖 <b>Dev:</b> {DEV_CONTACT}"
        )
        await query.edit_message_text(msg, parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[b("🔙 Back", "back_to_main")]]))
        return

    if data == "public_stats":
        total, today, _ = db_user_stats()
        await query.edit_message_text(
            f"📊 <b>PUBLIC STATS</b>\n━━━━━━━━━━━━━━━━━━━━━━\n"
            f"Users: {total} | Active Today: {today}\n"
            f"OTPs in DB: {len(load_otp_store())}\n"
            f"Numbers: {db_total_numbers()}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━", parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[b("🔙 Back","back_to_main")]]))
        return

    if data == "search_otp":
        await query.edit_message_text(
            "ℹ️ Use: <code>/otpfor [number]</code>", parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[b("🔙 Back","back_to_main")]]))
        return

    if data == "user_profile":
        from html import escape
        user       = query.from_user
        first      = escape(user.first_name or "")
        last       = escape(user.last_name or "")
        username   = f"@{user.username}" if user.username else "N/A"
        uid_val    = user.id
        # fetch user DB record
        c_cur = db.cursor()
        c_cur.execute("SELECT first_seen, last_seen, total_commands FROM tg_users WHERE user_id=?",
                      (uid_val,))
        row    = c_cur.fetchone()
        joined = row[0][:10] if row else "Unknown"
        cmds   = row[2] if row else 0
        # count numbers taken (= commands is a proxy; no separate table needed)
        profile_msg = (
            f"🫁 <b>YOUR PROFILE</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"👤 <b>Name:</b> {first} {last}\n"
            f"🔖 <b>Username:</b> {username}\n"
            f"🆔 <b>User ID:</b> <code>{uid_val}</code>\n"
            f"📅 <b>Joined:</b> {joined}\n"
            f"📊 <b>Commands Used:</b> {cmds}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📦 <b>Numbers Available:</b> {db_total_numbers()}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━"
        )
        await query.edit_message_text(
            profile_msg,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[b("🔙 𝗕𝗮𝗰𝗸", "back_to_main")]]))
        return

    if data == "back_to_main":
        from html import escape
        first = escape(query.from_user.first_name or "User")
        msg = (
            f"👋 <b>HI {first}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"💎 <b>MIK OTP BOT</b>\n"
            f"⚡ Fastest OTP Service\n"
            f"🤖 Auto-Assign System\n"
            f"🧬 Multi-Panel + IVAS Support\n\n"
            f"🆔 <b>Version :</b> 7.0\n\n"
            f"👆 <b>Select an option:</b>"
        )
        if is_admin(query.from_user.id):
            msg += "\n\n🛡️ <b>ADMIN</b> — /admin"
        try:
            await query.edit_message_text(msg, parse_mode="HTML",
                                          reply_markup=get_main_menu_keyboard(query.from_user.id))
        except Exception:
            pass
        return

    if data == "check_join":
        from html import escape
        cb_uid = query.from_user.id
        joined = await check_join(context.bot, cb_uid)
        if joined:
            first = escape(query.from_user.first_name or "User")
            msg = (
                f"👋 <b>HI {first}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"💎 <b>MIK OTP BOT</b>\n"
                f"⚡ Fastest OTP Service\n"
                f"🤖 Auto-Assign System\n"
                f"🧬 Multi-Panel + IVAS Support\n\n"
                f"🆔 <b>Version :</b> 7.0\n\n"
                f"👆 <b>Select an option:</b>"
            )
            if is_admin(cb_uid):
                msg += "\n\n🛡️ <b>ADMIN</b> — /admin"
            await query.edit_message_text(msg, parse_mode="HTML",
                                          reply_markup=get_main_menu_keyboard(cb_uid))
        else:
            await query.answer("❌ You haven't joined all channels yet!", show_alert=True)
        return

    if data == "set_user_prefix":
        current = db_get_user_prefix(uid)
        cur_txt = f"Current prefix: <code>{current}</code>" if current else "No prefix set."
        await query.edit_message_text(
            f"🔢 <b>SET YOUR NUMBER PREFIX</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{cur_txt}\n\n"
            f"Send your prefix (digits only, e.g. <code>6788</code>).\n"
            f"The bot will give you numbers that <b>start with this prefix</b>.\n\n"
            f"Send <b>none</b> to remove prefix and get any available number.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[b("❌ Cancel","back_to_main")]]))
        USER_PREFIX_STATES[uid] = {"step": "waiting_prefix", "timestamp": time.time()}
        return

    if data == "show_countries":
        # Show service selection first
        kb = get_services_keyboard()
        if not kb:
            await query.edit_message_text("❌ No numbers available.",
                reply_markup=InlineKeyboardMarkup([[b("🔙 Back","back_to_main")]]))
            return
        await query.edit_message_text(
            "📱 <b>Select Service:</b>",
            parse_mode="HTML", reply_markup=kb)
        return

    if data.startswith("svc_select|"):
        service = data.split("|", 1)[1]
        USER_SERVICE[uid] = service
        rows = db_get_countries(service)
        if not rows:
            await query.answer(f"❌ No numbers for {service}", show_alert=True)
            return
        icon = SERVICE_ICONS.get(service, "📱")
        kb = get_countries_keyboard(service)
        await query.edit_message_text(
            f"{icon} <b>Select Country</b> — {service}",
            parse_mode="HTML", reply_markup=kb)
        return

    if data.startswith("nb_get|"):
        country     = data.split("|", 1)[1]
        user_prefix = db_get_user_prefix(uid)
        service     = USER_SERVICE.get(uid, "")
        icon        = SERVICE_ICONS.get(service, "📱") if service else "🌍"

        # Fetch numbers matching user prefix if set, else any numbers
        if user_prefix:
            phones = db_pop_numbers_with_prefix(country, user_prefix, 3, service)
            if not phones:
                prefix_count = db_count_prefix_numbers(country, user_prefix, service)
                await query.answer(
                    f"❌ No numbers with prefix '{user_prefix}' in {country}"
                    + (f" for {service}" if service else "")
                    + f". ({prefix_count} match in stock)",
                    show_alert=True)
                return
        else:
            phones = db_pop_numbers(country, 3, service)

        if phones:
            flag      = FLAG_MAP.get(country, "🌍")
            remaining = db_get_countries(service)
            rem_count = next((n for c, n in remaining if c == country), 0)

            # Save assignment
            db_assign_numbers(uid, phones)

            # Apply global admin prefix if set
            config    = load_config()
            num_pfx   = config.get("number_prefix", "")
            display_phones = []
            for ph in phones:
                if num_pfx:
                    digits = re.sub(r"[^0-9]", "", ph)
                    display_phones.append(f"+{num_pfx}{digits}")
                else:
                    digits = re.sub(r"[^0-9]", "", ph)
                    display_phones.append(f"+{digits}")

            # Build number lines
            num_lines = ""
            for i, ph in enumerate(display_phones, 1):
                num_lines += f"<b>{i}.</b>  <code>{ph}</code>\n"

            prefix_status = f"ON ({user_prefix})" if user_prefix else "OFF"
            svc_line = f"{icon} <b>Service:</b> {service} {flag}\n" if service else ""

            msg = (
                f"🎉 <b>Numbers Assigned!</b>\n"
                f"{'─'*32}\n"
                f"{svc_line}"
                f"\n{num_lines}\n"
                f"{'─'*32}\n"
                f"⚡ <b>Waiting for SMS...</b>"
            )

            # Keyboard matching screenshot layout
            pref_btn = b(f"🔢 Prefix: {prefix_status}", "set_user_prefix")
            kb = InlineKeyboardMarkup([
                [b("🚫 Block Number",    f"block_num|{country}"),
                 b("🔄 Change Number",   f"nb_get|{country}")],
                [b("🌍 Change Country",  "show_countries"),
                 pref_btn],
                [b("📋 Change Service",  "show_countries")],
                [b("💬 OTP Group", url="https://t.me/+0YairwHa63NkNWM0")],
            ])
            await query.edit_message_text(msg, parse_mode="HTML", reply_markup=kb)
        else:
            await query.answer(
                f"❌ Out of stock!" + (f" ({service}, {country})" if service else f" ({country})"),
                show_alert=True)
        return

    # ── Block number (user) ──────────────────────────────────
    if data.startswith("block_num|"):
        country = data.split("|", 1)[1]
        service = USER_SERVICE.get(uid, "")
        await query.answer("🚫 Number blocked. Getting a new one...", show_alert=False)
        # Simulate getting a fresh number for same country/service
        new_phones = db_pop_numbers(country, 1, service)
        if new_phones:
            db_assign_numbers(uid, new_phones)
            config    = load_config()
            num_pfx   = config.get("number_prefix", "")
            ph        = new_phones[0]
            if num_pfx:
                ph = f"+{num_pfx}{re.sub(r'[^0-9]', '', ph)}"
            else:
                ph = f"+{re.sub(r'[^0-9]', '', ph)}"
            user_prefix   = db_get_user_prefix(uid)
            prefix_status = f"ON ({user_prefix})" if user_prefix else "OFF"
            icon = SERVICE_ICONS.get(service, "🌍")
            flag = FLAG_MAP.get(country, "🌍")
            svc_line = f"{icon} <b>Service:</b> {service} {flag}\n" if service else ""
            msg = (f"🎉 <b>Numbers Assigned!</b>\n{'─'*32}\n{svc_line}\n"
                   f"<b>1.</b>  <code>{ph}</code>\n\n{'─'*32}\n⚡ <b>Waiting for SMS...</b>")
            pref_btn = b(f"🔢 Prefix: {prefix_status}", "set_user_prefix")
            kb = InlineKeyboardMarkup([
                [b("🚫 Block Number",   f"block_num|{country}"),
                 b("🔄 Change Number",  f"nb_get|{country}")],
                [b("🌍 Change Country", "show_countries"), pref_btn],
                [b("📋 Change Service", "show_countries")],
                [b("💬 OTP Group", url="https://t.me/+0YairwHa63NkNWM0")],
            ])
            await query.edit_message_text(msg, parse_mode="HTML", reply_markup=kb)
        else:
            await query.edit_message_text("❌ No more numbers available.",
                reply_markup=InlineKeyboardMarkup([[b("🔙 Back","back_to_main")]]))
        return

    # ── Admin gate ────────────────────────────────────────────
    if not is_admin(uid):
        await query.edit_message_text("❌ Unauthorized!")
        return

    # ── Admin nav ─────────────────────────────────────────────
    if data in ("back_to_admin", "refresh_admin"):
        try:
            await query.edit_message_text(
                "💎 <b>ADMIN PANEL</b>\n━━━━━━━━━━━━━━━━━━━━━━\nSelect option:",
                parse_mode="HTML", reply_markup=get_admin_keyboard(uid))
        except Exception:
            pass
        return

    if data == "close_admin":
        await query.delete_message()
        return

    # ══ NUMBERS ═══════════════════════════════════════════════
    if data == "menu_numbers":
        if not has_perm(uid,"numbers"): await query.answer("❌ No permission.",show_alert=True); return
        rows  = db_get_countries()
        total = db_total_numbers()
        lines = "\n".join([f"  {FLAG_MAP.get(c,'🌍')} {c}: <b>{n}</b>" for c,n in rows]) or "  Empty"
        await query.edit_message_text(
            f"📦 <b>NUMBER MANAGER</b>\n━━━━━━━━━━━━━━━━━━━━━━\nTotal: <b>{total}</b>\n\n{lines}",
            parse_mode="HTML", reply_markup=get_numbers_menu())
        return

    if data == "nb_add_numbers":
        NB_STATE[uid] = {"step": "waiting_service", "timestamp": time.time()}
        await query.edit_message_text(
            "📦 <b>ADD NUMBERS</b>\n━━━━━━━━━━━━━━━━━━━━━━\n"
            "Step 1: Send the <b>Service Name</b> (e.g. WhatsApp, Facebook):",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[b("❌ Cancel","cancel_action")]]))
        return

    if data == "nb_number_list":
        rows = db_get_countries()
        if not rows:
            await query.edit_message_text("📦 Stock is empty.",
                reply_markup=InlineKeyboardMarkup([[b("🔙 Back","menu_numbers")]]))
            return
        await query.edit_message_text("📋 <b>STOCK</b> - Tap to delete:",
            parse_mode="HTML", reply_markup=get_nb_stock_keyboard())
        return

    if data == "nb_delete_menu":
        rows = db_get_countries()
        if not rows:
            await query.answer("No stock", show_alert=True)
            return
        await query.edit_message_text("🗑️ <b>SELECT COUNTRY TO DELETE:</b>",
            parse_mode="HTML", reply_markup=get_nb_stock_keyboard())
        return

    if data.startswith("nb_del|"):
        country = data.split("|", 1)[1]
        db_delete_country(country)
        await query.edit_message_text(f"✅ <b>{country}</b> deleted.", parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[b("🔙 Back","menu_numbers")]]))
        return

    if data == "nb_stock_stats":
        rows  = db_get_countries()
        total = db_total_numbers()
        lines = "\n".join([f"  {FLAG_MAP.get(c,'🌍')} {c}: {n}" for c,n in rows]) or "  Empty"
        await query.edit_message_text(
            f"📊 <b>STOCK STATS</b>\n━━━━━━━━━━━━━━━━━━━━━━\nTotal: <b>{total}</b>\n\n{lines}",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[b("🔙 Back","menu_numbers")]]))
        return

    if data == "nb_export":
        rows = db_get_countries()
        if not rows:
            await query.answer("No numbers to export", show_alert=True)
            return
        lines = []
        for country, _ in rows:
            nums = db_get_country_numbers(country)
            lines.append(f"=== {country} ===")
            lines.extend(nums)
        fname = f"numbers_export_{int(time.time())}.txt"
        with open(fname, "w") as f:
            f.write("\n".join(lines))
        async with Bot(token=BOT_TOKEN) as bot_inst:
            await bot_inst.send_document(chat_id=query.message.chat_id,
                document=open(fname, "rb"),
                caption=f"📤 Numbers Export — {db_total_numbers()} total")
        os.remove(fname)
        await query.edit_message_text("✅ Numbers exported.",
            reply_markup=InlineKeyboardMarkup([[b("🔙 Back","menu_numbers")]]))
        return

    # ══ FILE MANAGER ══════════════════════════════════════════
    if data == "menu_files":
        if not has_perm(uid,"files"): await query.answer("❌ No permission.",show_alert=True); return
        flist = []
        for f in [LOG_FILE, OTP_FILE, DB_FILE, PANEL_FILE, IVAS_FILE,
                  CONFIG_FILE, ADMINS_FILE, GROUP_FILE]:
            if os.path.exists(f):
                flist.append(f"{f} — {os.path.getsize(f):,}b")
        await query.edit_message_text(
            f"📂 <b>FILE MANAGER</b>\n━━━━━━━━━━━━━━━━━━━━━━\n<code>"
            + "\n".join(flist) + "</code>",
            parse_mode="HTML", reply_markup=get_files_menu())
        return

    if data == "fm_list":
        flist = []
        for f in [LOG_FILE, OTP_FILE, DB_FILE, PANEL_FILE, IVAS_FILE,
                  CONFIG_FILE, ADMINS_FILE, GROUP_FILE]:
            if os.path.exists(f):
                flist.append(f"{f} ({os.path.getsize(f):,}b)")
        await query.edit_message_text(
            "📂 <b>FILES:</b>\n<code>" + "\n".join(flist) + "</code>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[b("🔙 Back","menu_files")]]))
        return

    for file_data, file_path, caption in [
        ("fm_download_log", LOG_FILE, "📋 Bot Log"),
        ("fm_download_otp", OTP_FILE, "🗄 OTP Store"),
        ("fm_download_db",  DB_FILE,  "🗄 SQLite DB"),
    ]:
        if data == file_data:
            if not os.path.exists(file_path):
                await query.answer("File not found", show_alert=True)
                return
            async with Bot(token=BOT_TOKEN) as bot_inst:
                await bot_inst.send_document(chat_id=query.message.chat_id,
                    document=open(file_path, "rb"), caption=caption)
            await query.answer("✅ Sent", show_alert=True)
            return

    if data == "fm_clear_log":
        await query.edit_message_text("🗑️ Clear the bot log file?",
            reply_markup=get_confirmation_keyboard("clear_log"))
        return

    if data == "confirm_clear_log":
        open(LOG_FILE, "w").close()
        await query.edit_message_text("✅ Log cleared.",
            reply_markup=InlineKeyboardMarkup([[b("🔙 Back","menu_files")]]))
        return

    # ══ PANEL MANAGER ═════════════════════════════════════════
    if data == "menu_panels":
        if not has_perm(uid,"panels"): await query.answer("❌ No permission.",show_alert=True); return
        await query.edit_message_text(
            "📡 <b>PANEL MANAGER</b>\n━━━━━━━━━━━━━━━━━━━━━━\nSelect option:",
            parse_mode="HTML", reply_markup=get_panels_menu())
        return

    if data == "list_panels":
        panels = load_panels()
        text   = "📋 <b>REST PANELS</b>\n━━━━━━━━━━━━━━━━━━━━━━\n"
        for name, pd in panels.items():
            active = (name in REST_TASKS and not REST_TASKS[name].done())
            hits   = STATS["panel_hits"].get(name, 0)
            st     = "🟢" if active else "🔴"
            text  += f"\n{st} <b>{name}</b>\n   {pd['url'][:50]}\n   Hits: {hits}\n"
        await query.edit_message_text(text or "No panels.", parse_mode="HTML",
            reply_markup=get_panels_keyboard("view"))
        return

    if data == "add_panel":
        PANEL_ADD_STATES[uid] = {"step":"name","data":{},"timestamp":time.time()}
        await query.edit_message_text(
            "➕ <b>ADD REST PANEL</b>\n━━━━━━━━━━━━━━━━━━━━━━\nStep 1: Panel name:",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[b("❌ Cancel","cancel_action")]]))
        return

    if data == "remove_panel":
        await query.edit_message_text("🗑️ <b>SELECT PANEL TO REMOVE:</b>",
            parse_mode="HTML", reply_markup=get_panels_keyboard("remove"))
        return

    if data.startswith("view_panel_"):
        panel  = data.replace("view_panel_", "")
        panels = load_panels()
        if panel not in panels:
            await query.answer("Not found", show_alert=True)
            return
        pd       = panels[panel]
        active   = (panel in REST_TASKS and not REST_TASKS[panel].done())
        api_type = pd.get("api_type", "sami")
        api_lbl  = "📡 MOAPI MDR" if api_type == "mdr" else "📡 Sami/Standard"
        await query.edit_message_text(
            f"{'🟢' if active else '🔴'} <b>{panel.upper()}</b>\n━━━━━━━━━━━━━━━━━━━━━━\n"
            f"URL: <code>{pd['url']}</code>\n"
            f"Token: <code>{pd['token'][:30]}...</code>\n"
            f"Type: <b>{api_lbl}</b>\n"
            f"Records: <code>{pd.get('records',20)}</code>\n"
            f"Hits: <code>{STATS['panel_hits'].get(panel,0)}</code>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[b("🔙 Back","list_panels")]]))
        return

    if data.startswith("remove_panel_"):
        panel = data.replace("remove_panel_", "")
        await query.edit_message_text(f"🟡 Remove panel <b>{panel}</b>?", parse_mode="HTML",
            reply_markup=get_confirmation_keyboard("remove_panel", panel))
        return

    if data.startswith("confirm_remove_panel_"):
        panel  = data.replace("confirm_remove_panel_", "")
        panels = load_panels()
        if panel in panels:
            del panels[panel]
            save_panels(panels)
            API_PANELS = panels
        if panel in REST_TASKS:
            REST_TASKS[panel].cancel()
            del REST_TASKS[panel]
        await query.edit_message_text(f"✅ Panel <b>{panel}</b> removed.", parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[b("🔙 Back","menu_panels")]]))
        return

    if data == "confirm_add_panel":
        if uid not in PANEL_ADD_STATES or PANEL_ADD_STATES[uid]["step"] != "confirm":
            await query.edit_message_text("❌ No pending panel.")
            return
        pd = PANEL_ADD_STATES[uid]["data"]
        API_PANELS[pd["name"]] = {
            "url":      pd["url"],
            "token":    pd["token"],
            "records":  pd.get("records", 20),
            "api_type": pd.get("api_type", "sami"),
        }
        save_panels(API_PANELS)
        task = asyncio.create_task(api_worker(pd["name"]), name=f"REST-{pd['name']}")
        task.add_done_callback(handle_task_exception)
        REST_TASKS[pd["name"]] = task
        del PANEL_ADD_STATES[uid]
        api_lbl = "MOAPI MDR" if pd.get("api_type") == "mdr" else "Sami/Standard"
        await query.edit_message_text(
            f"✅ Panel <b>{pd['name']}</b> added and started!\n"
            f"📡 Type: <b>{api_lbl}</b>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[b("🔙 Back","menu_panels")]]))
        return

    if data == "panel_fetch_all":
        wait    = await context.bot.send_message(chat_id=query.message.chat_id, text="🔄 Fetching...")
        results = fetch_all_panels(limit=3)
        if not results:
            await wait.edit_text("❌ Nothing fetched.")
            return
        text = f"📨 <b>FETCHED {len(results)}</b>\n━━━━━━━━━━━━━━━━━━━━━━\n"
        for r in results[:8]:
            otp  = extract_otp(r["message"]) or "N/A"
            text += (f"📡 <b>{r['panel']}</b> | {r['service']}\n"
                     f"📞 <code>{r['number']}</code>  🔑 <code>{otp}</code>\n"
                     f"💬 {r['message'][:60]}\n──────────────────────\n")
        await wait.edit_text(text[:4000], parse_mode="HTML")
        return

    # ══ IVAS MANAGER ══════════════════════════════════════════
    if data == "menu_ivas":
        if not has_perm(uid,"ivas"): await query.answer("❌ No permission.",show_alert=True); return
        await query.edit_message_text(
            "🔌 <b>IVAS MANAGER</b>\n━━━━━━━━━━━━━━━━━━━━━━\nSelect option:",
            parse_mode="HTML", reply_markup=get_ivas_menu())
        return

    if data == "list_ivas":
        accounts = load_ivas()
        if not accounts:
            await query.edit_message_text("🟡 No IVAS accounts yet.",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([
                    [b("➕ Add IVAS","add_ivas")],
                    [b("🔙 Back","menu_ivas")]]))
            return
        text = "🔌 <b>IVAS ACCOUNTS</b>\n━━━━━━━━━━━━━━━━━━━━━━\n"
        for name in accounts:
            active = (name in IVAS_TASKS and not IVAS_TASKS[name].done())
            hits   = STATS["ivas_hits"].get(name, 0)
            st     = "🟢" if active else "🔴"
            text  += f"\n{st} <b>{name}</b> (hits:{hits})\n"
        await query.edit_message_text(text, parse_mode="HTML",
            reply_markup=get_ivas_keyboard("view"))
        return

    if data == "add_ivas":
        IVAS_ADD_STATES[uid] = {"step":"name","data":{},"timestamp":time.time()}
        await query.edit_message_text(
            "🔌 <b>ADD IVAS</b>\n━━━━━━━━━━━━━━━━━━━━━━\nStep 1: Account name:",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[b("❌ Cancel","cancel_action")]]))
        return

    if data == "remove_ivas":
        await query.edit_message_text("🗑️ <b>SELECT IVAS TO REMOVE:</b>",
            parse_mode="HTML", reply_markup=get_ivas_keyboard("remove"))
        return

    if data == "ivas_restart_all":
        for name in list(IVAS_TASKS.keys()):
            IVAS_TASKS[name].cancel()
            del IVAS_TASKS[name]
        for name in load_ivas():
            task = asyncio.create_task(ivas_worker(name), name=f"IVAS-{name}")
            task.add_done_callback(handle_task_exception)
            IVAS_TASKS[name] = task
        await query.edit_message_text("✅ All IVAS workers restarted.",
            reply_markup=InlineKeyboardMarkup([[b("🔙 Back","menu_ivas")]]))
        return

    if data.startswith("view_ivas_"):
        name     = data.replace("view_ivas_", "")
        accounts = load_ivas()
        if name not in accounts:
            await query.answer("Not found", show_alert=True)
            return
        active = (name in IVAS_TASKS and not IVAS_TASKS[name].done())
        hits   = STATS["ivas_hits"].get(name, 0)
        uri    = accounts[name].get("uri", "N/A")
        await query.edit_message_text(
            f"{'🟢' if active else '🔴'} <b>IVAS: {name.upper()}</b>\n━━━━━━━━━━━━━━━━━━━━━━\n"
            f"Status: {'Running' if active else 'Stopped'}\n"
            f"Hits: <code>{hits}</code>\n"
            f"URI: <code>{uri[:80]}...</code>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[b("🔙 Back","list_ivas")]]))
        return

    if data.startswith("remove_ivas_"):
        name = data.replace("remove_ivas_", "")
        await query.edit_message_text(f"🟡 Remove IVAS <b>{name}</b>?", parse_mode="HTML",
            reply_markup=get_confirmation_keyboard("remove_ivas", name))
        return

    if data.startswith("confirm_remove_ivas_"):
        name     = data.replace("confirm_remove_ivas_", "")
        accounts = load_ivas()
        if name in accounts:
            del accounts[name]
            save_ivas(accounts)
        if name in IVAS_TASKS:
            IVAS_TASKS[name].cancel()
            del IVAS_TASKS[name]
        await query.edit_message_text(f"✅ IVAS <b>{name}</b> removed.", parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[b("🔙 Back","menu_ivas")]]))
        return

    if data == "confirm_add_ivas":
        if uid not in IVAS_ADD_STATES or IVAS_ADD_STATES[uid]["step"] != "confirm":
            await query.edit_message_text("❌ No pending IVAS.")
            return
        pd       = IVAS_ADD_STATES[uid]["data"]
        accounts = load_ivas()
        accounts[pd["name"]] = {"uri": pd["uri"]}
        save_ivas(accounts)
        task = asyncio.create_task(ivas_worker(pd["name"]), name=f"IVAS-{pd['name']}")
        task.add_done_callback(handle_task_exception)
        IVAS_TASKS[pd["name"]] = task
        del IVAS_ADD_STATES[uid]
        await query.edit_message_text(f"✅ IVAS <b>{pd['name']}</b> added!",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[b("🔙 Back","menu_ivas")]]))
        return

    # ══ FETCH SMS ══════════════════════════════════════════════
    if data == "menu_fetch":
        if not has_perm(uid,"fetch_sms"): await query.answer("❌ No permission.",show_alert=True); return
        await query.edit_message_text(
            "🔄 <b>FETCH SMS</b>\n━━━━━━━━━━━━━━━━━━━━━━\nSelect method:",
            parse_mode="HTML", reply_markup=get_fetch_menu())
        return

    if data == "fetch_all_now":
        wait    = await context.bot.send_message(chat_id=query.message.chat_id, text="🔄 Fetching...")
        results = fetch_all_panels(limit=5)
        if not results:
            await wait.edit_text("❌ Nothing fetched.")
            return
        text = f"📨 <b>LATEST SMS ({len(results)})</b>\n━━━━━━━━━━━━━━━━━━━━━━\n"
        for r in results[:10]:
            otp  = extract_otp(r["message"]) or "N/A"
            text += (f"📡 <b>{r['panel']}</b> | {r['service']}\n"
                     f"📞 <code>{r['number']}</code>\n"
                     f"🔑 OTP: <code>{otp}</code>\n"
                     f"💬 {r['message'][:80]}\n──────────────────────\n")
        await wait.edit_text(text[:4000], parse_mode="HTML")
        return

    if data == "fetch_by_number":
        FETCH_STATES[uid] = {"step":"waiting_number","timestamp":time.time()}
        await query.edit_message_text(
            "🔍 <b>FETCH BY NUMBER</b>\n━━━━━━━━━━━━━━━━━━━━━━\nSend the phone number:",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[b("❌ Cancel","cancel_action")]]))
        return

    if data == "fetch_single_panel":
        panels = load_panels()
        kb     = [[b(name.upper(), f"fetch_panel_{name}")] for name in panels]
        kb.append([b("🔙 Back","menu_fetch")])
        await query.edit_message_text("📡 <b>SELECT PANEL:</b>",
            parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb))
        return

    if data.startswith("fetch_panel_"):
        panel  = data.replace("fetch_panel_", "")
        result = fetch_latest(panel)
        if result:
            otp = extract_otp(result["message"]) or "N/A"
            await query.edit_message_text(
                f"📡 <b>Panel: {panel}</b>\n━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📞 <code>{result['number']}</code>\n"
                f"🔑 OTP: <code>{otp}</code>\n"
                f"📱 Service: {result['service']}\n"
                f"💬 {result['message'][:200]}\n"
                f"⏱ {result['time']}",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[b("🔙 Back","menu_fetch")]]))
        else:
            await query.edit_message_text(f"❌ No data from <b>{panel}</b>.", parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[b("🔙 Back","menu_fetch")]]))
        return

    # ══ OTP HISTORY ═══════════════════════════════════════════
    if data == "menu_otp_history":
        if not has_perm(uid,"otp_history"): await query.answer("❌ No permission.",show_alert=True); return
        last    = db_get_otp_history(5)
        preview = "".join([f"📞 <code>{r[0]}</code> 🔑 <code>{r[2]}</code> {r[3]}\n"
                           for r in last]) or "No history yet"
        await query.edit_message_text(
            f"📋 <b>OTP HISTORY</b>\n━━━━━━━━━━━━━━━━━━━━━━\n{preview}",
            parse_mode="HTML", reply_markup=get_otp_history_menu())
        return

    if data in ("otp_hist_10","otp_hist_20"):
        limit = 10 if data == "otp_hist_10" else 20
        rows  = db_get_otp_history(limit)
        if not rows:
            await query.answer("No history", show_alert=True)
            return
        text = f"📋 <b>LAST {limit} OTPs</b>\n━━━━━━━━━━━━━━━━━━━━━━\n"
        for row in rows:
            text += (f"📞 <code>{row[0]}</code> | <b>{row[1]}</b>\n"
                     f"🔑 <code>{row[2]}</code> | {row[3]} | {row[4]}\n──────────\n")
        await query.edit_message_text(text[:4000], parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[b("🔙 Back","menu_otp_history")]]))
        return

    if data == "otp_export_hist":
        rows = db_get_otp_history(9999)
        if not rows:
            await query.answer("No history", show_alert=True)
            return
        fname = f"otp_history_{int(time.time())}.csv"
        with open(fname, "w") as f:
            f.write("number,service,otp,source,time\n")
            for row in rows:
                f.write(",".join(str(x) for x in row) + "\n")
        async with Bot(token=BOT_TOKEN) as bot_inst:
            await bot_inst.send_document(chat_id=query.message.chat_id,
                document=open(fname,"rb"),
                caption=f"📤 OTP History — {len(rows)} records")
        os.remove(fname)
        await query.answer("✅ Exported", show_alert=True)
        return

    if data == "otp_search_num":
        FETCH_STATES[uid] = {"step":"waiting_number","timestamp":time.time()}
        await query.edit_message_text(
            "🔍 <b>SEARCH OTP BY NUMBER</b>\n━━━━━━━━━━━━━━━━━━━━━━\nSend the number:",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[b("❌ Cancel","cancel_action")]]))
        return

    if data == "otp_clear_hist":
        await query.edit_message_text("🗑️ Clear ALL OTP history?",
            reply_markup=get_confirmation_keyboard("otp_clear_hist"))
        return

    if data == "confirm_otp_clear_hist":
        db_clear_otp_history()
        await query.edit_message_text("✅ History cleared.",
            reply_markup=InlineKeyboardMarkup([[b("🔙 Back","menu_otp_history")]]))
        return

    # ══ GROUP MANAGER ═════════════════════════════════════════
    if data == "menu_groups":
        if not has_perm(uid,"groups"): await query.answer("❌ No permission.",show_alert=True); return
        groups = load_groups()
        config = load_config()
        lg     = config.get("log_group") or "Not set"
        await query.edit_message_text(
            f"👥 <b>GROUP MANAGER</b>\n━━━━━━━━━━━━━━━━━━━━━━\n"
            f"OTP Groups: <b>{len(groups)}</b>\n"
            f"Log Group: <code>{lg}</code>",
            parse_mode="HTML", reply_markup=get_groups_menu())
        return

    if data == "add_group_prompt":
        SETTING_STATES[uid] = {"step":"waiting_group_id","timestamp":time.time()}
        await query.edit_message_text(
            "➕ <b>ADD OTP GROUP</b>\n━━━━━━━━━━━━━━━━━━━━━━\nSend the group chat ID:",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[b("❌ Cancel","cancel_action")]]))
        return

    if data.startswith("del_group_"):
        try:
            gid    = int(data.replace("del_group_", ""))
            groups = load_groups()
            if gid in groups:
                groups.remove(gid)
                save_groups(groups)
            await query.edit_message_text(f"✅ Group <code>{gid}</code> removed.", parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[b("🔙 Back","menu_groups")]]))
        except:
            await query.answer("Error", show_alert=True)
        return

    if data in ("set_log_group","set_log_group_settings"):
        SETTING_STATES[uid] = {"step":"waiting_log_group","timestamp":time.time()}
        await query.edit_message_text(
            "📋 <b>SET LOG GROUP</b>\n━━━━━━━━━━━━━━━━━━━━━━\nSend log group chat ID:",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[b("❌ Cancel","cancel_action")]]))
        return

    if data == "clear_log_group":
        config = load_config()
        config["log_group"] = None
        save_config(config)
        await query.edit_message_text("✅ Log group cleared.",
            reply_markup=InlineKeyboardMarkup([[b("🔙 Back","menu_groups")]]))
        return

    # ══ STATS / STATUS ════════════════════════════════════════
    if data == "stats":
        if not has_perm(uid,"stats"): await query.answer("❌ No permission.",show_alert=True); return
        total, active_today, active_week = db_user_stats()
        uptime = str(datetime.now() - datetime.fromtimestamp(STATS['start_time'])).split('.')[0]
        await query.edit_message_text(
            f"📊 <b>BOT STATS</b>\n━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⏱ Uptime: <code>{uptime}</code>\n"
            f"👤 Users: <code>{total}</code> | Today: <code>{active_today}</code>\n"
            f"📊 OTPs Sent: <code>{STATS['otps_sent']}</code>\n"
            f"🚫 Dropped: <code>{STATS['otps_dropped']}</code>\n"
            f"❌ Errors: <code>{STATS['errors']}</code>\n"
            f"📦 Numbers: <code>{db_total_numbers()}</code>\n"
            f"🗄 OTP Store: <code>{len(load_otp_store())}</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━", parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[b("🔙 Back","back_to_admin")]]))
        return

    if data == "status":
        if not has_perm(uid,"stats"): await query.answer("❌ No permission.",show_alert=True); return
        await query.edit_message_text(_build_status(), parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[b("🔙 Back","back_to_admin")]]))
        return

    # ══ BROADCAST ═════════════════════════════════════════════
    if data == "broadcast":
        if not has_perm(uid,"broadcast"): await query.answer("❌ No permission.",show_alert=True); return
        await query.edit_message_text("📢 <b>BROADCAST</b>\n━━━━━━━━━━━━━━━━━━━━━━\nChoose type:",
            parse_mode="HTML", reply_markup=get_broadcast_keyboard())
        return

    if data == "broadcast_text":
        BROADCAST_STATES[uid] = {"type":"text","step":"waiting_message","timestamp":time.time()}
        await query.edit_message_text("📢 Send your broadcast message:",
            reply_markup=InlineKeyboardMarkup([[b("❌ Cancel","cancel_action")]]))
        return

    if data == "broadcast_with_buttons":
        BROADCAST_STATES[uid] = {"type":"with_buttons","step":"waiting_message","timestamp":time.time()}
        await query.edit_message_text(
            "📢 Send text + buttons.\nFormat:\nYour message\n[Button Label|https://url]",
            reply_markup=InlineKeyboardMarkup([[b("❌ Cancel","cancel_action")]]))
        return

    if data == "confirm_broadcast":
        if uid not in BROADCAST_STATES or BROADCAST_STATES[uid]["step"] != "confirm":
            await query.edit_message_text("❌ No pending broadcast.")
            return
        await query.edit_message_text("📢 Broadcasting...")
        msg_text = BROADCAST_STATES[uid]["message"]
        sent, failed = await broadcast_to_all_users(msg_text)
        await send_to_all_groups(msg_text)
        del BROADCAST_STATES[uid]
        await query.edit_message_text(f"✅ Broadcast done!\n📨 Users: {sent} sent, {failed} failed\n📢 Sent to OTP groups too.",
            reply_markup=InlineKeyboardMarkup([[b("🔙 Back","back_to_admin")]]))
        return

    if data == "confirm_broadcast_buttons":
        if uid not in BROADCAST_STATES or BROADCAST_STATES[uid]["step"] != "confirm":
            await query.edit_message_text("❌ No pending broadcast.")
            return
        state   = BROADCAST_STATES[uid]
        buttons = state.get("buttons", [])
        kb      = InlineKeyboardMarkup([[btn_] for btn_ in buttons]) if buttons else None
        await query.edit_message_text("📢 Broadcasting...")
        sent, failed = await broadcast_to_all_users(state["message"], kb)
        await send_to_all_groups(state["message"], kb)
        del BROADCAST_STATES[uid]
        await query.edit_message_text(f"✅ Broadcast done!\n📨 Users: {sent} sent, {failed} failed\n📢 Sent to OTP groups too.",
            reply_markup=InlineKeyboardMarkup([[b("🔙 Back","back_to_admin")]]))
        return

    # ══ SETTINGS ══════════════════════════════════════════════
    if data == "menu_settings":
        if not has_perm(uid,"settings"): await query.answer("❌ No permission.",show_alert=True); return
        await query.edit_message_text(
            "⚙️ <b>SETTINGS</b>\n━━━━━━━━━━━━━━━━━━━━━━\nSelect option:",
            parse_mode="HTML", reply_markup=get_settings_menu())
        return

    if data == "toggle_otp_forward":
        config = load_config()
        config["otp_forward"] = not config.get("otp_forward", True)
        save_config(config)
        st = "✅ ENABLED" if config["otp_forward"] else "❌ DISABLED"
        await query.edit_message_text(f"📤 OTP Forwarding: <b>{st}</b>", parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[b("🔙 Back","menu_settings")]]))
        return

    if data in ("set_forward_delay","set_channel","set_numberbot",
                "set_otp_group_link","set_log_group","set_number_prefix"):
        step_map = {
            "set_forward_delay": ("waiting_delay",    "⏱ SET FORWARD DELAY\nSend seconds (0-60):"),
            "set_channel":       ("waiting_channel",  "📢 SET CHANNEL LINK\nSend the URL:"),
            "set_numberbot":     ("waiting_numberbot","🤖 SET NUMBER BOT LINK\nSend the URL:"),
            "set_otp_group_link":("waiting_otp_link", "🔗 SET OTP GROUP LINK\nSend the URL:"),
            "set_log_group":     ("waiting_log_group","📋 SET LOG GROUP\nSend the chat ID:"),
            "set_number_prefix": ("waiting_prefix",   "🔢 SET NUMBER PREFIX\nSend the prefix to add to numbers when assigned (e.g. +225)\nSend <b>none</b> to disable prefix."),
        }
        step, prompt = step_map[data]
        SETTING_STATES[uid] = {"step": step, "timestamp": time.time()}
        await query.edit_message_text(
            f"⚙️ <b>{prompt}</b>", parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[b("❌ Cancel","cancel_action")]]))
        return

    if data == "menu_admin_manager":
        if not is_owner(uid):
            await query.answer("❌ Owner only!", show_alert=True)
            return
        staff = load_staff()
        await query.edit_message_text(
            f"👤 <b>STAFF MANAGER</b>\n━━━━━━━━━━━━━━━━━━━━━━\n"
            f"👑 Owners: <b>{len(OWNER_IDS)}</b>\n"
            f"🛡️ Staff Members: <b>{len(staff)}</b>\n\n"
            f"Tap a staff member to edit their permissions.\n"
            f"Each permission can be toggled ON/OFF individually.",
            parse_mode="HTML", reply_markup=get_admin_manager_keyboard())
        return

    if data == "add_staff_prompt":
        if not is_owner(uid):
            await query.answer("❌ Owner only!", show_alert=True)
            return
        SETTING_STATES[uid] = {"step": "waiting_staff_id", "timestamp": time.time()}
        await query.edit_message_text(
            "➕ <b>ADD STAFF MEMBER</b>\n━━━━━━━━━━━━━━━━━━━━━━\n"
            "Send the Telegram <b>User ID</b> of the new staff member:",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[b("❌ Cancel", "cancel_action")]]))
        return

    if data.startswith("edit_staff_"):
        if not is_owner(uid):
            await query.answer("❌ Owner only!", show_alert=True)
            return
        target_str = data.replace("edit_staff_", "")
        staff      = load_staff()
        info       = staff.get(target_str, {})
        name       = info.get("name", target_str)
        perms      = info.get("perms", [])
        perm_lines = "\n".join([
            f"  {'✅' if p in perms else '☑️'} {ALL_PERMISSIONS[p]}"
            for p in ALL_PERMISSIONS
        ])
        await query.edit_message_text(
            f"🛡️ <b>EDIT PERMISSIONS</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"User: <code>{target_str}</code> ({name})\n"
            f"Active Perms: <b>{len(perms)}/{len(ALL_PERMISSIONS)}</b>\n\n"
            f"{perm_lines}",
            parse_mode="HTML",
            reply_markup=get_staff_perms_keyboard(target_str))
        return

    if data.startswith("toggle_perm_"):
        if not is_owner(uid):
            await query.answer("❌ Owner only!", show_alert=True)
            return
        # format: toggle_perm_{uid_str}_{perm_key}
        rest       = data.replace("toggle_perm_", "", 1)
        # perm key is always last part, uid_str may contain underscores... use ALL_PERMISSIONS
        perm_key   = None
        target_str = None
        for pk in ALL_PERMISSIONS:
            if rest.endswith("_" + pk):
                perm_key   = pk
                target_str = rest[:-(len(pk)+1)]
                break
        if not perm_key or not target_str:
            await query.answer("Error parsing perm", show_alert=True)
            return
        staff = load_staff()
        if target_str not in staff:
            await query.answer("Staff not found", show_alert=True)
            return
        perms = staff[target_str].get("perms", [])
        if perm_key in perms:
            perms.remove(perm_key)
            action = "Removed"
        else:
            perms.append(perm_key)
            action = "Added"
        staff[target_str]["perms"] = perms
        save_staff(staff)
        await query.answer(f"{action}: {ALL_PERMISSIONS[perm_key]}", show_alert=False)
        # Refresh the keyboard
        info       = staff[target_str]
        name       = info.get("name", target_str)
        perm_lines = "\n".join([
            f"  {'✅' if p in perms else '☑️'} {ALL_PERMISSIONS[p]}"
            for p in ALL_PERMISSIONS
        ])
        await query.edit_message_text(
            f"🛡️ <b>EDIT PERMISSIONS</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"User: <code>{target_str}</code> ({name})\n"
            f"Active Perms: <b>{len(perms)}/{len(ALL_PERMISSIONS)}</b>\n\n"
            f"{perm_lines}",
            parse_mode="HTML",
            reply_markup=get_staff_perms_keyboard(target_str))
        return

    if data.startswith("grant_all_"):
        if not is_owner(uid):
            await query.answer("❌ Owner only!", show_alert=True)
            return
        target_str = data.replace("grant_all_", "")
        staff      = load_staff()
        if target_str in staff:
            staff[target_str]["perms"] = list(ALL_PERMISSIONS.keys())
            save_staff(staff)
        await query.answer("✅ All permissions granted", show_alert=True)
        await query.edit_message_text(
            f"✅ All permissions granted to <code>{target_str}</code>.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[b("🔙 Back", "menu_admin_manager")]]))
        return

    if data.startswith("revoke_all_"):
        if not is_owner(uid):
            await query.answer("❌ Owner only!", show_alert=True)
            return
        target_str = data.replace("revoke_all_", "")
        staff      = load_staff()
        if target_str in staff:
            staff[target_str]["perms"] = []
            save_staff(staff)
        await query.answer("❌ All permissions revoked", show_alert=True)
        await query.edit_message_text(
            f"❌ All permissions revoked from <code>{target_str}</code>.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[b("🔙 Back", "menu_admin_manager")]]))
        return

    if data.startswith("remove_staff_"):
        if not is_owner(uid):
            await query.answer("❌ Owner only!", show_alert=True)
            return
        target_str = data.replace("remove_staff_", "")
        try:
            target_int = int(target_str)
            if target_int in OWNER_IDS:
                await query.answer("❌ Cannot remove owner!", show_alert=True)
                return
            remove_staff(target_int)
            await query.edit_message_text(
                f"✅ Staff member <code>{target_str}</code> removed.",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[b("🔙 Back", "menu_admin_manager")]]))
        except:
            await query.answer("Error", show_alert=True)
        return

    # Legacy stubs (no-op, kept for back-compat)
    if data in ("add_admin_prompt", "add_manager_prompt",
                "remove_admin_dummy", "remove_manager_dummy"):
        await query.answer("Use Staff Manager instead.", show_alert=True)
        return

    # ══ ADVANCED ══════════════════════════════════════════════
    if data == "menu_advanced":
        if not has_perm(uid,"advanced"): await query.answer("❌ No permission.",show_alert=True); return
        await query.edit_message_text(
            "🔧 <b>ADVANCED TOOLS</b>\n━━━━━━━━━━━━━━━━━━━━━━\nSelect option:",
            parse_mode="HTML", reply_markup=get_advanced_keyboard())
        return

    if data == "restart_workers":
        await _restart_all_workers()
        await query.edit_message_text("✅ All workers restarted.",
            reply_markup=InlineKeyboardMarkup([[b("🔙 Back","menu_advanced")]]))
        return

    if data == "restart_bot":
        if not is_owner(uid):
            await query.answer("❌ Owner only!", show_alert=True)
            return
        await query.edit_message_text("🔁 Restarting bot...")
        await asyncio.sleep(1)
        os.execv(sys.executable, [sys.executable] + sys.argv)
        return

    if data in ("stop_forward","start_forward"):
        config = load_config()
        config["otp_forward"] = (data == "start_forward")
        save_config(config)
        st = "✅ ENABLED" if config["otp_forward"] else "❌ DISABLED"
        await query.edit_message_text(f"📤 OTP Forwarding: <b>{st}</b>", parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[b("🔙 Back","menu_advanced")]]))
        return

    if data == "reload_config":
        API_PANELS = load_panels()
        await query.edit_message_text("✅ Config reloaded from all JSON files.",
            reply_markup=InlineKeyboardMarkup([[b("🔙 Back","menu_advanced")]]))
        return

    if data == "clear_all_otps":
        await query.edit_message_text("🗑️ Delete ALL stored OTPs?",
            reply_markup=get_confirmation_keyboard("clear_all_otps"))
        return

    if data == "confirm_clear_all_otps":
        save_otp_store({})
        await query.edit_message_text("✅ OTP store cleared.",
            reply_markup=InlineKeyboardMarkup([[b("🔙 Back","menu_advanced")]]))
        return

    if data == "export_otps":
        store = load_otp_store()
        if not store:
            await query.answer("No OTPs to export", show_alert=True)
            return
        fname = f"otp_export_{int(time.time())}.json"
        with open(fname, "w") as f:
            json.dump(store, f, indent=4)
        async with Bot(token=BOT_TOKEN) as bot_inst:
            await bot_inst.send_document(chat_id=query.message.chat_id,
                document=open(fname,"rb"),
                caption=f"📤 OTP Store — {len(store)} entries")
        os.remove(fname)
        await query.answer("✅ Exported", show_alert=True)
        return

    if data == "view_logs":
        try:
            with open(LOG_FILE, "r") as f:
                lines = f.readlines()[-25:]
            log_text = "".join(lines)
            if len(log_text) > 3500:
                log_text = log_text[-3500:]
            await query.edit_message_text(
                f"<b>Last 25 log lines:</b>\n<pre>{log_text}</pre>",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[b("🔙 Back","menu_advanced")]]))
        except Exception as e:
            await query.edit_message_text(f"Error reading logs: {e}")
        return

    if data in ("test_panels_menu","test_panels_adv"):
        panels = load_panels()
        if not panels:
            await query.edit_message_text("No panels configured.")
            return
        results = []
        for name in panels:
            try:
                d2 = fetch_latest(name)
                results.append(f"{'✅' if d2 else '❌'} {name}: {'Online' if d2 else 'Offline'}")
            except Exception as e:
                results.append(f"❌ {name}: {e}")
        await query.edit_message_text(
            "🧪 <b>Panel Test Results</b>\n━━━━━━━━━━━━━━━━━━━━━━\n" + "\n".join(results),
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[b("🔙 Back","menu_advanced")]]))
        return

    if data == "worker_status":
        lines = []
        for p in load_panels():
            alive = (p in REST_TASKS and not REST_TASKS[p].done())
            lines.append(f"{'🟢' if alive else '🔴'} REST: {p} (hits:{STATS['panel_hits'].get(p,0)})")
        for n in load_ivas():
            alive = (n in IVAS_TASKS and not IVAS_TASKS[n].done())
            lines.append(f"{'🟢' if alive else '🔴'} IVAS: {n} (hits:{STATS['ivas_hits'].get(n,0)})")
        await query.edit_message_text(
            "📊 <b>WORKER STATUS</b>\n━━━━━━━━━━━━━━━━━━━━━━\n" + "\n".join(lines or ["No workers"]),
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[b("🔙 Back","menu_advanced")]]))
        return

    # ── Misc confirmations ────────────────────────────────────
    if data == "confirm_clear_otps":
        save_otp_store({})
        await query.edit_message_text("✅ OTP store cleared.",
            reply_markup=InlineKeyboardMarkup([[b("🔙 Back","back_to_admin")]]))
        return

    # ── Cancel ────────────────────────────────────────────────
    if data == "cancel_action":
        for d in [PANEL_ADD_STATES, IVAS_ADD_STATES, BROADCAST_STATES,
                  SETTING_STATES, FETCH_STATES, NB_STATE, USER_PREFIX_STATES]:
            d.pop(uid, None)
        await query.edit_message_text("❌ Action cancelled.",
            reply_markup=InlineKeyboardMarkup([[b("🔙 Back","back_to_admin")]]))
        return

# ═══════════════════════════════════════════════════════════════
#  MESSAGE HANDLER
# ═══════════════════════════════════════════════════════════════
async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    text = update.message.text or ""

    if text == "/cancel":
        for d in [PANEL_ADD_STATES, IVAS_ADD_STATES, BROADCAST_STATES,
                  SETTING_STATES, FETCH_STATES, NB_STATE, USER_PREFIX_STATES]:
            d.pop(uid, None)
        await update.message.reply_text("❌ Cancelled.")
        return

    # ── Number add flow ───────────────────────────────────────
    if uid in NB_STATE:
        state = NB_STATE[uid]
        if isinstance(state, dict):
            step = state.get("step")
            if step == "waiting_service":
                NB_STATE[uid] = {"step":"waiting_country","service":text.strip(),"timestamp":time.time()}
                await update.message.reply_text(
                    f"✅ Service: <b>{text.strip()}</b>\n\nStep 2: Send the <b>Country Name</b>:",
                    parse_mode="HTML")
            elif step == "waiting_country":
                NB_STATE[uid] = {"step":"waiting_file","country":text.strip(),
                                  "service":state.get("service",""),"timestamp":time.time()}
                await update.message.reply_text(
                    f"✅ Country: <b>{text.strip()}</b>\n\nStep 3: Send a <b>.txt file</b> (one number per line):",
                    parse_mode="HTML")
        return

    # ── Settings wizard ───────────────────────────────────────
    if uid in SETTING_STATES:
        state = SETTING_STATES[uid]
        state["timestamp"] = time.time()
        step  = state.get("step")

        if step == "waiting_group_id":
            try:
                gid = int(text.strip())
                groups = load_groups()
                if gid not in groups:
                    groups.append(gid)
                    save_groups(groups)
                    await update.message.reply_text(f"✅ Group <code>{gid}</code> added.", parse_mode="HTML")
                else:
                    await update.message.reply_text("🟡 Already exists.")
            except:
                await update.message.reply_text("❌ Invalid chat ID.")
            del SETTING_STATES[uid]

        elif step == "waiting_log_group":
            try:
                gid = int(text.strip())
                config = load_config()
                config["log_group"] = gid
                save_config(config)
                await update.message.reply_text(f"✅ Log group set to <code>{gid}</code>.", parse_mode="HTML")
            except:
                await update.message.reply_text("❌ Invalid chat ID.")
            del SETTING_STATES[uid]

        elif step == "waiting_delay":
            try:
                delay = int(text.strip())
                if not 0 <= delay <= 60:
                    raise ValueError
                config = load_config()
                config["forward_delay"] = delay
                save_config(config)
                await update.message.reply_text(f"✅ Forward delay set to <b>{delay}s</b>.", parse_mode="HTML")
            except:
                await update.message.reply_text("❌ Enter a number 0-60.")
            del SETTING_STATES[uid]

        elif step == "waiting_channel":
            config = load_config()
            config["channel_link"] = text.strip()
            save_config(config)
            await update.message.reply_text("✅ Channel link updated.")
            del SETTING_STATES[uid]

        elif step == "waiting_numberbot":
            config = load_config()
            config["number_bot_link"] = text.strip()
            save_config(config)
            await update.message.reply_text("✅ Number bot link updated.")
            del SETTING_STATES[uid]

        elif step == "waiting_otp_link":
            config = load_config()
            config["channel_link"] = text.strip()
            save_config(config)
            await update.message.reply_text("✅ OTP group link updated.")
            del SETTING_STATES[uid]

        elif step == "waiting_prefix":
            val = text.strip()
            config = load_config()
            if val.lower() == "none":
                config["number_prefix"] = ""
                save_config(config)
                await update.message.reply_text("✅ Number prefix <b>disabled</b>.", parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup([[b("🔙 Back to Settings","menu_settings")]]))
            else:
                config["number_prefix"] = val
                save_config(config)
                await update.message.reply_text(f"✅ Number prefix set to <code>{val}</code>.", parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup([[b("🔙 Back to Settings","menu_settings")]]))
            del SETTING_STATES[uid]

        elif step == "waiting_staff_id":
            if not is_owner(uid):
                await update.message.reply_text("❌ Owner only!")
                del SETTING_STATES[uid]
                return
            try:
                new_uid = int(text.strip())
                if new_uid in OWNER_IDS:
                    await update.message.reply_text("🟡 That user is already an owner.")
                    del SETTING_STATES[uid]
                    return
                staff = load_staff()
                if str(new_uid) in staff:
                    await update.message.reply_text("🟡 Already a staff member. Use edit to change permissions.")
                    del SETTING_STATES[uid]
                    return
                # Add with zero permissions — owner edits them next
                add_staff(new_uid, str(new_uid), [])
                await update.message.reply_text(
                    f"✅ Staff member <code>{new_uid}</code> added with no permissions.\n\n"
                    f"Now go to Staff Manager and tap their name to assign permissions.",
                    parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup([[b("👤 Open Staff Manager", "menu_admin_manager")]]))
            except ValueError:
                await update.message.reply_text("❌ Invalid user ID. Send a numeric Telegram user ID.")
            del SETTING_STATES[uid]

        # Legacy step kept for back-compat
        elif step == "waiting_new_admin":
            del SETTING_STATES[uid]
            await update.message.reply_text("Use /admin > Settings > Staff Manager to add staff.")
        return

    # ── User prefix setting ──────────────────────────────────
    if uid in USER_PREFIX_STATES:
        state = USER_PREFIX_STATES[uid]
        state["timestamp"] = time.time()
        if state.get("step") == "waiting_prefix":
            val = text.strip()
            if val.lower() == "none":
                db_set_user_prefix(uid, "")
                await update.message.reply_text(
                    "✅ Prefix <b>removed</b>. You will now receive any available numbers.",
                    parse_mode="HTML",
                    reply_markup=get_main_menu_keyboard(uid))
            else:
                clean = re.sub(r"[^0-9]", "", val)
                if not clean:
                    await update.message.reply_text("❌ Invalid prefix. Send digits only (e.g. <code>6788</code>).", parse_mode="HTML")
                    return
                db_set_user_prefix(uid, clean)
                await update.message.reply_text(
                    f"✅ Prefix set to <code>{clean}</code>\n"
                    f"The bot will now give you numbers starting with <b>{clean}</b>.",
                    parse_mode="HTML",
                    reply_markup=get_main_menu_keyboard(uid))
            del USER_PREFIX_STATES[uid]
        return

    # ── Fetch by number ───────────────────────────────────────
    if uid in FETCH_STATES:
        state = FETCH_STATES[uid]
        state["timestamp"] = time.time()
        if state.get("step") == "waiting_number":
            target = text.strip().replace("+", "")
            found  = None
            store  = load_otp_store()
            for k, v in store.items():
                if target in k:
                    found = v
                    break
            if not found:
                rows = db_search_otp_by_number(target)
                if rows:
                    found = rows[0][2]
            if not found:
                for panel in API_PANELS:
                    d2 = fetch_latest(panel)
                    if d2 and target in d2["number"]:
                        found = extract_otp(d2["message"])
                        break
            if found:
                await update.message.reply_text(
                    f"✅ <b>OTP FOUND</b>\n📞 <code>{target}</code>\n🔑 <code>{found}</code>",
                    parse_mode="HTML")
            else:
                await update.message.reply_text(
                    f"❌ No OTP found for <code>{target}</code>", parse_mode="HTML")
            del FETCH_STATES[uid]
        return

    # ── Panel add wizard ──────────────────────────────────────
    if uid in PANEL_ADD_STATES:
        state = PANEL_ADD_STATES[uid]
        state["timestamp"] = time.time()
        if state["step"] == "name":
            state["data"]["name"] = text
            state["step"] = "url"
            await update.message.reply_text("Step 2: API URL (http://...):")
        elif state["step"] == "url":
            if not text.startswith("http"):
                await update.message.reply_text("❌ Must start with http:")
                return
            state["data"]["url"] = text
            state["step"] = "token"
            await update.message.reply_text("Step 3: API Token:")
        elif state["step"] == "token":
            state["data"]["token"] = text
            state["step"] = "api_type"
            await update.message.reply_text(
                "Step 4: API Type\n\n"
                "Send <b>1</b> for <b>Sami / Standard</b> panel\n"
                "Send <b>2</b> for <b>MOAPI MDR</b> panel (http://137.74.1.203/crapi/reseller/mdr.php format)",
                parse_mode="HTML")
        elif state["step"] == "api_type":
            if text.strip() == "2":
                state["data"]["api_type"] = "mdr"
                api_label = "MOAPI MDR"
            else:
                state["data"]["api_type"] = "sami"
                api_label = "Sami / Standard"
            state["data"]["_api_label"] = api_label
            state["step"] = "records"
            await update.message.reply_text("Step 5: Records count (1-200):")
        elif state["step"] == "records":
            try:
                rec = int(text)
                if not 1 <= rec <= 200:
                    raise ValueError
            except:
                await update.message.reply_text("❌ Enter 1-200:")
                return
            state["data"]["records"] = rec
            api_label = state["data"].pop("_api_label", state["data"].get("api_type","sami"))
            state["step"] = "confirm"
            await update.message.reply_text(
                f"➕ <b>CONFIRM ADD PANEL</b>\n━━━━━━━━━━━━━━━━━━━━━━\n"
                f"Name: <code>{state['data']['name']}</code>\n"
                f"URL: <code>{state['data']['url']}</code>\n"
                f"Token: <code>{state['data']['token'][:30]}...</code>\n"
                f"Type: <b>{api_label}</b>\n"
                f"Records: <code>{rec}</code>\n━━━━━━━━━━━━━━━━━━━━━━\nConfirm?",
                parse_mode="HTML", reply_markup=get_confirmation_keyboard("add_panel"))
        return

    # ── IVAS add wizard ───────────────────────────────────────
    if uid in IVAS_ADD_STATES:
        state = IVAS_ADD_STATES[uid]
        state["timestamp"] = time.time()
        if state["step"] == "name":
            state["data"]["name"] = text
            state["step"] = "uri"
            await update.message.reply_text(
                "Step 2: IVAS WebSocket URI (<code>wss://...</code>):", parse_mode="HTML")
        elif state["step"] == "uri":
            if not text.startswith("wss://"):
                await update.message.reply_text("❌ Must start with <code>wss://</code>", parse_mode="HTML")
                return
            state["data"]["uri"] = text
            state["step"] = "confirm"
            await update.message.reply_text(
                f"🔌 <b>CONFIRM ADD IVAS</b>\n━━━━━━━━━━━━━━━━━━━━━━\n"
                f"Name: <code>{state['data']['name']}</code>\n"
                f"URI: <code>{state['data']['uri'][:80]}...</code>\n━━━━━━━━━━━━━━━━━━━━━━\nConfirm?",
                parse_mode="HTML", reply_markup=get_confirmation_keyboard("add_ivas"))
        return

    # ── Broadcast wizard ──────────────────────────────────────
    if uid in BROADCAST_STATES:
        state = BROADCAST_STATES[uid]
        state["timestamp"] = time.time()
        if state["step"] == "waiting_message":
            if state["type"] == "text":
                state["message"] = text
                state["step"]    = "confirm"
                await update.message.reply_text(
                    f"Preview:\n{text[:200]}{'...' if len(text)>200 else ''}\n\nSend?",
                    reply_markup=get_confirmation_keyboard("broadcast"))
            elif state["type"] == "with_buttons":
                lines    = text.split('\n')
                msg_text = ""
                buttons  = []
                for line in lines:
                    if line.startswith('[') and line.endswith(']') and '|' in line:
                        parts = line[1:-1].split('|', 1)
                        buttons.append(InlineKeyboardButton(parts[0].strip(), url=parts[1].strip()))
                    else:
                        msg_text += line + '\n'
                state["message"] = msg_text.strip()
                state["buttons"] = buttons
                state["step"]    = "confirm"
                await update.message.reply_text(
                    f"Preview with {len(buttons)} button(s). Send?",
                    reply_markup=get_confirmation_keyboard("broadcast_buttons"))
        return

# ═══════════════════════════════════════════════════════════════
#  DOCUMENT HANDLER
# ═══════════════════════════════════════════════════════════════
async def document_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid not in NB_STATE or not isinstance(NB_STATE[uid], dict):
        return
    state = NB_STATE[uid]
    if state.get("step") != "waiting_file":
        return
    country = state["country"]
    service = state.get("service", "")
    try:
        file       = await update.message.document.get_file()
        file_bytes = await file.download_as_bytearray()
        file_text  = file_bytes.decode("utf-8")
        all_lines  = [n.strip() for n in file_text.splitlines() if n.strip()]
        nums       = [n for n in all_lines if n.isdigit()] or all_lines
        db_add_numbers(country, nums, service)
        svc_txt = f" [{service}]" if service else ""
        await update.message.reply_text(
            f"✅ <b>{len(nums)}</b> numbers added to <b>{country}{svc_txt}</b>!",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[b("🔙 Back to Admin","back_to_admin")]]))
        del NB_STATE[uid]
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")

# ═══════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════
async def main():
    logger.info(f"🚀 {BOT_NAME} starting...")
    logger.info(f"📡 OTP Groups: {load_groups()}")
    logger.info(f"🔌 IVAS accounts: {list(load_ivas().keys())}")
    logger.info(f"📋 REST panels: {list(API_PANELS.keys())}")
    logger.info(f"⚙️  OTP forward: {load_config().get('otp_forward', True)}")

    asyncio.create_task(cleanup_states())
    asyncio.create_task(monitor_tasks())

    for panel in API_PANELS:
        task = asyncio.create_task(api_worker(panel), name=f"REST-{panel}")
        task.add_done_callback(handle_task_exception)
        REST_TASKS[panel] = task

    for name in load_ivas():
        task = asyncio.create_task(ivas_worker(name), name=f"IVAS-{name}")
        task.add_done_callback(handle_task_exception)
        IVAS_TASKS[name] = task

    app = Application.builder().token(BOT_TOKEN).build()

    for cmd, handler in [
        ("start",       start_command),
        ("help",        help_command),
        ("admin",       admin_command),
        ("otpfor",      otpfor_command),
        ("fetchsms",    fetchsms_command),
        ("status",      status_command),
        ("stats",       stats_command),
        ("addgroup",    addgroup_command),
        ("removegroup", removegroup_command),
        ("reload",      reload_command),
    ]:
        app.add_handler(CommandHandler(cmd, handler))

    app.add_handler(CallbackQueryHandler(callback_query_handler))
    app.add_handler(MessageHandler(filters.Document.ALL, document_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))

    await app.initialize()
    await app.start()
    # Remove all commands from Telegram menu (/ suggestions)
    await app.bot.set_my_commands([])
    await app.updater.start_polling()
    logger.info("🟢 Bot is online.")
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Bot stopped.")
        await app.stop()

if __name__ == "__main__":
    asyncio.run(main())
