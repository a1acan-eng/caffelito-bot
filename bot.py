"""
CAFFELITO TELEGRAM BOT ☕
Заказ, Задачи, Уборка и ОКК контроль
"""

import json, os, logging, sqlite3
from datetime import datetime, timezone, timedelta
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    WebAppInfo, BotCommand, BotCommandScopeChat, BotCommandScopeDefault,
    MenuButtonCommands, MenuButtonWebApp, MenuButtonDefault,
    KeyboardButton, ReplyKeyboardMarkup,
)
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    ContextTypes, MessageHandler, filters
)

BOT_TOKEN = os.getenv("BOT_TOKEN", "BURAYA_BOT_TOKEN_YAZ")
WEBAPP_URL = os.getenv("WEBAPP_URL", "")
GROUP_CHAT_ID = os.getenv("GROUP_CHAT_ID", "")  # Grup ID — /setgroup komutuyla alınır
MINIAPP_SHORT_NAME = os.getenv("MINIAPP_SHORT_NAME", "app")  # BotFather'a verdiğin Short name
ACCESS_CODE = os.getenv("ACCESS_CODE", "")  # Boşsa giriş kodu kapalı; doluysa /login KOD gerekiyor (eski sistem — fallback)
# 🗂  DB yolu — Railway Volume için: env DB_PATH=/data/caffelito.db
# Boş bırakılırsa current dir'de "caffelito.db" kullanılır (LOCAL test için).
DB_PATH = os.getenv("DB_PATH", "caffelito.db")
TZ = timezone(timedelta(hours=5))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info(f"DB_PATH = {DB_PATH}")

# ─── Markdown güvenli escape (özel karakterler kullanıcı adında varsa parse hatası vermesin) ───
def md_safe(text):
    """Telegram parse_mode='Markdown' için tehlikeli karakterleri escape'le."""
    if text is None:
        return ""
    s = str(text)
    # Markdown legacy: _ * ` [ özel
    return s.replace("\\", "\\\\").replace("_", "\\_").replace("*", "\\*").replace("`", "\\`").replace("[", "\\[")

# ─── DATABASE ───
def get_db():
    # DB_PATH dizin varsa otomatik oluştur (Railway Volume mount'u için güvenli)
    try:
        d = os.path.dirname(DB_PATH)
        if d and not os.path.exists(d):
            os.makedirs(d, exist_ok=True)
    except Exception as _e:
        logger.warning(f"DB dir create skipped: {_e}")
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    db.execute("""CREATE TABLE IF NOT EXISTS shops (
        chat_id INTEGER PRIMARY KEY, name TEXT DEFAULT 'Caffelito')""")
    db.execute("""CREATE TABLE IF NOT EXISTS orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id INTEGER, user_id INTEGER, user_name TEXT,
        items TEXT, created_at TEXT)""")
    db.execute("""CREATE TABLE IF NOT EXISTS tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id INTEGER, user_id INTEGER, user_name TEXT,
        category TEXT, tasks TEXT, date TEXT, created_at TEXT)""")
    # ─── Maaş sistemi ───
    db.execute("""CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        name TEXT,
        username TEXT,
        role TEXT DEFAULT 'barista',
        chat_id INTEGER,
        created_at TEXT,
        display_name TEXT)""")
    # display_name eski DB'lerde yoksa ekle
    try:
        db.execute("ALTER TABLE users ADD COLUMN display_name TEXT")
    except sqlite3.OperationalError:
        pass
    # authorized: ACCESS_CODE doğru girildiyse 1 olur
    try:
        db.execute("ALTER TABLE users ADD COLUMN authorized INTEGER DEFAULT 0")
    except sqlite3.OperationalError:
        pass
    # password: her bariastanın kendi şifresi (owner atayıp/silebilir)
    try:
        db.execute("ALTER TABLE users ADD COLUMN password TEXT")
    except sqlite3.OperationalError:
        pass
    # archived: 1 → kullanıcı arşivde (geçmiş duruyor ama aktif listede gözükmez, bot girişi kapalı)
    try:
        db.execute("ALTER TABLE users ADD COLUMN archived INTEGER DEFAULT 0")
    except sqlite3.OperationalError:
        pass
    # archived_at: arşive alındığı tarih
    try:
        db.execute("ALTER TABLE users ADD COLUMN archived_at TEXT")
    except sqlite3.OperationalError:
        pass
    db.execute("""CREATE TABLE IF NOT EXISTS shifts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER, hours REAL, drinks TEXT,
        bonus INTEGER, hourly_pay INTEGER, total INTEGER,
        date TEXT, period TEXT, created_at TEXT,
        start_time TEXT, end_time TEXT, note TEXT)""")
    # ── Migration: eski DB'de bu kolonlar yoksa ekle ──
    for col, ddl in [("start_time", "TEXT"), ("end_time", "TEXT"), ("note", "TEXT"),
                     ("desserts", "TEXT"), ("dessert_bonus", "INTEGER DEFAULT 0")]:
        try:
            db.execute(f"ALTER TABLE shifts ADD COLUMN {col} {ddl}")
        except sqlite3.OperationalError:
            pass  # Kolon zaten var
    db.execute("""CREATE TABLE IF NOT EXISTS fines (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER, amount INTEGER, reason TEXT,
        type TEXT, period TEXT,
        added_by INTEGER, added_by_name TEXT,
        created_at TEXT)""")
    db.execute("""CREATE TABLE IF NOT EXISTS payments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER, amount INTEGER, period TEXT,
        paid_by INTEGER, paid_by_name TEXT, paid_at TEXT)""")
    # ─── Çaевые (Bahşiş) ───
    db.execute("""CREATE TABLE IF NOT EXISTS tips (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER, amount INTEGER, period TEXT,
        note TEXT,
        added_by INTEGER, added_by_name TEXT,
        created_at TEXT)""")
    # ─── Bardak fiyatları (override) ───
    db.execute("""CREATE TABLE IF NOT EXISTS prices (
        drink_id TEXT PRIMARY KEY,
        amount INTEGER,
        updated_by INTEGER, updated_by_name TEXT,
        updated_at TEXT)""")
    # ─── Tatlı kataloğu (owner yönetir) ───
    db.execute("""CREATE TABLE IF NOT EXISTS desserts_catalog (
        id TEXT PRIMARY KEY,
        label TEXT,
        icon TEXT,
        price INTEGER DEFAULT 500,
        sort_order INTEGER DEFAULT 0,
        active INTEGER DEFAULT 1,
        updated_by INTEGER,
        updated_by_name TEXT,
        updated_at TEXT)""")
    # Eski "soft-delete"li (active=0) tatlıları kataloğdan tamamen temizle —
    # kullanıcı "скрыт" görmek istemiyor, tamamen silinsin.
    try:
        db.execute("DELETE FROM desserts_catalog WHERE COALESCE(active,1)=0")
    except sqlite3.OperationalError:
        pass
    # Tatlı kataloğu boşsa default seed
    cnt = db.execute("SELECT COUNT(*) as c FROM desserts_catalog").fetchone()
    if (cnt["c"] or 0) == 0:
        defaults = [
            ("cookie", "Печенье",   "🍪", 500, 1),
            ("cheesecake","Чизкейк","🍰", 500, 2),
            ("brownie", "Брауни",   "🍫", 500, 3),
            ("tiramisu","Тирамису","🥮", 500, 4),
            ("muffin",  "Маффин",  "🧁", 500, 5),
            ("croissant","Круассан","🥐",500, 6),
            ("other_sweet","Другое","🍮",500, 99),
        ]
        for d in defaults:
            db.execute("INSERT OR IGNORE INTO desserts_catalog (id,label,icon,price,sort_order,active,updated_at) VALUES (?,?,?,?,?,1,?)",
                       (d[0], d[1], d[2], d[3], d[4], datetime.now(TZ).isoformat()))
    # ─── Audit log ───
    db.execute("""CREATE TABLE IF NOT EXISTS logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        action TEXT,
        actor_id INTEGER, actor_name TEXT,
        target_id INTEGER, target_name TEXT,
        details TEXT,
        created_at TEXT)""")
    # ─── Recipe Trainer (Тренажёр рецептов) ───
    db.execute("""CREATE TABLE IF NOT EXISTS rt_progress (
        user_id INTEGER PRIMARY KEY,
        level INTEGER DEFAULT 1,
        max_level INTEGER DEFAULT 1,
        xp INTEGER DEFAULT 0,
        best_streak INTEGER DEFAULT 0,
        total_sessions INTEGER DEFAULT 0,
        total_correct INTEGER DEFAULT 0,
        total_questions INTEGER DEFAULT 0,
        last_played_at TEXT)""")
    db.execute("""CREATE TABLE IF NOT EXISTS rt_sessions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        level INTEGER,
        correct INTEGER,
        total INTEGER,
        xp_earned INTEGER,
        max_streak INTEGER,
        passed INTEGER,
        played_at TEXT)""")
    db.execute("""CREATE TABLE IF NOT EXISTS rt_exams (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        correct INTEGER,
        total INTEGER,
        score INTEGER,
        passed INTEGER,
        taken_at TEXT)""")
    # Resmi sınav daveti (owner → barista)
    db.execute("""CREATE TABLE IF NOT EXISTS rt_exam_invites (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        barista_id INTEGER,
        owner_id INTEGER,
        owner_name TEXT,
        status TEXT DEFAULT 'pending',
        score INTEGER,
        correct INTEGER,
        total INTEGER,
        created_at TEXT,
        finished_at TEXT)""")
    db.commit()
    return db


# ═══════════════════════════════════════
#  ЗАРПЛАТА (MAAŞ SİSTEMİ)
# ═══════════════════════════════════════
HOURLY_RATE = 12000  # сум за час

BONUS_RATES = {
    "ml100": 500,
    "ml200": 700,
    "ml300": 1000,
    "ml400": 1200,
    "ml500": 1400,
    "dome300": 1000,
    "dome400": 1300,
}

# Tatlılar — her biri 500 сум sabit
DESSERT_RATE = 500
DESSERT_LIST = [
    {"id": "cookie",      "label": "🍪 Печенье"},
    {"id": "cheesecake",  "label": "🍰 Чизкейк"},
    {"id": "brownie",     "label": "🍫 Брауни"},
    {"id": "tiramisu",    "label": "🥮 Тирамису"},
    {"id": "muffin",      "label": "🧁 Маффин"},
    {"id": "croissant",   "label": "🥐 Круассан"},
    {"id": "other_sweet", "label": "🍮 Другое"},
]


def get_dessert_catalog(db, only_active=True):
    """Aktif tatlıların listesi: [{id,label,icon,price,sort_order,active}]"""
    q = "SELECT id,label,icon,price,sort_order,active FROM desserts_catalog"
    if only_active:
        q += " WHERE active=1"
    q += " ORDER BY sort_order, label"
    return [dict(r) for r in db.execute(q).fetchall()]


def get_dessert_prices(db):
    """Hızlı erişim: {id: price}"""
    return {r["id"]: int(r["price"] or 0) for r in db.execute(
        "SELECT id, price FROM desserts_catalog").fetchall()}


def calc_dessert_bonus(desserts, prices_map=None):
    """desserts={cookie:5,cheesecake:2,...} → toplam tatlı bonusu (DB fiyatına göre)."""
    total = 0
    pmap = prices_map or {}
    for k, v in (desserts or {}).items():
        price = pmap.get(k, DESSERT_RATE)
        total += int(price) * int(v or 0)
    return total

FINE_PRESETS = {
    "clean": {"label": "🧹 Чистота", "amount": 30000},
    "insp_70": {"label": "🏢 Проверка 70-80%", "amount": 1000000},
    "insp_60": {"label": "🏢 Проверка 60-70%", "amount": 2000000},
    "insp_50": {"label": "🏢 Проверка 50-60%", "amount": 3000000},
    "foreign": {"label": "🚫 Посторонняя продукция", "amount": 4000000},
}


def fmt_sum(n):
    return f"{int(n):,}".replace(",", ".")


def current_period():
    return datetime.now(TZ).strftime("%Y-%m")


def upsert_user(db, user_id, name, username=None, chat_id=None):
    db.execute("""INSERT INTO users (user_id, name, username, chat_id, role, created_at)
                  VALUES (?,?,?,?,'barista',?)
                  ON CONFLICT(user_id) DO UPDATE SET
                  name=excluded.name,
                  username=COALESCE(excluded.username, users.username),
                  chat_id=COALESCE(excluded.chat_id, users.chat_id)""",
               (user_id, name, username, chat_id, datetime.now(TZ).isoformat()))
    db.commit()


def get_role(db, user_id):
    row = db.execute("SELECT role FROM users WHERE user_id=?", (user_id,)).fetchone()
    return row["role"] if row else "barista"


def auth_required(db):
    """
    Auth gerekiyor mu?
    - Eğer DB'de en az bir owner varsa → her bariastanın kendi şifresi gerekir
      (owner her zaman authorized).
    - Owner yoksa ve ACCESS_CODE da boşsa → açık (herkes giriş yapabilir, ilk kullanıcı owner olur).
    """
    # En az bir owner kayıtlıysa parola sistemi aktif
    try:
        if has_owner(db):
            return True
    except Exception:
        pass
    # Eski sistem: ACCESS_CODE varsa zorunlu
    return bool(ACCESS_CODE)


def is_authorized(db, user_id):
    """
    Yetki:
    - Owner her zaman yetkili.
    - Arşivli kullanıcı asla erişemez.
    - Auth kapalıysa: herkes yetkili.
    - Auth açıksa: users.authorized=1 VEYA şifresi atanmış olanlar yetkili.
      (Şifre atananlar webapp'ta passcode ile giriyor — server bunu güvenir.)
    """
    row = db.execute("SELECT role, authorized, archived, password FROM users WHERE user_id=?", (user_id,)).fetchone()
    if row and (row["role"] or "") == "owner":
        return True
    if row and (row["archived"] or 0):
        return False
    if not auth_required(db):
        return True
    if not row:
        return False
    if (row["password"] or "").strip():
        return True
    return bool(row["authorized"])


async def require_auth(update, context):
    """Yetkisizse uyarı gönder ve False döndür."""
    db = get_db()
    user = update.effective_user
    upsert_user(db, user.id, user.first_name, user.username, update.effective_chat.id)
    if is_authorized(db, user.id):
        return True
    await update.message.reply_text(
        "🔒 *Доступ ограничен*\n\n"
        "Этот бот — только для сотрудников Caffelito.\n"
        "Введите ваш личный пароль (получите у владельца):\n"
        "`/login ВАШ_ПАРОЛЬ`",
        parse_mode="Markdown")
    return False


def has_owner(db):
    row = db.execute("SELECT COUNT(*) as c FROM users WHERE role='owner'").fetchone()
    return (row["c"] or 0) > 0


async def send_reopen_button(update, context, db, user):
    """
    Mini App aksiyondan sonra otomatik kapanır (Telegram limitasyonu —
    bot tarafında engellenemez). Bu helper TAM onaydan hemen sonra
    "🚀 Продолжить в приложении" inline butonu gönderir.
    Kullanıcı tek dokunuşla uygulamaya geri döner — /start basmaya gerek yok.
    URL her seferinde taze build edilir, böylece state güncel olur.
    """
    try:
        if not WEBAPP_URL:
            return
        if update.effective_chat.type != "private":
            return
        url = build_webapp_url(WEBAPP_URL, user.id, user.first_name, db)
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("🚀 Продолжить в приложении", web_app=WebAppInfo(url=url))
        ]])
        await update.message.reply_text("👆 Одно касание — и вы снова в Caffelito", reply_markup=kb)
    except Exception as e:
        logger.warning(f"send_reopen_button failed: {e}")


async def refresh_webapp_keyboard(update, context, db, user, text="🔄 Приложение обновлено 👇"):
    """
    Vardiya başla/bit gibi state değişikliklerinden sonra ReplyKeyboard'u
    TAZE URL ile yeniden gönder. Yoksa Telegram eski hash'i tutar ve mini app
    eski veriyle açılır (örn. 'Начать смену' butonu hâlâ görünür).
    """
    try:
        if not WEBAPP_URL:
            return
        if update.effective_chat.type != "private":
            return
        url = build_webapp_url(WEBAPP_URL, user.id, user.first_name, db)
        kb = ReplyKeyboardMarkup(
            [[KeyboardButton("☕ Открыть Caffelito", web_app=WebAppInfo(url=url))]],
            resize_keyboard=True
        )
        await update.message.reply_text(text, reply_markup=kb)
    except Exception as e:
        logger.warning(f"refresh_webapp_keyboard failed: {e}")


def find_user(db, handle):
    """Find user by @username, name, or numeric id"""
    h = str(handle).lstrip("@").strip()
    if not h:
        return None
    if h.isdigit():
        row = db.execute("SELECT * FROM users WHERE user_id=?", (int(h),)).fetchone()
        if row:
            return row
    row = db.execute("SELECT * FROM users WHERE username=? COLLATE NOCASE", (h,)).fetchone()
    if row:
        return row
    return db.execute("SELECT * FROM users WHERE name=? COLLATE NOCASE", (h,)).fetchone()


def calc_bonus(drinks, prices=None):
    """drinks={ml100:5,...}; prices ondan kullanılır, yoksa default."""
    total = 0
    p = prices or BONUS_RATES
    for k, v in (drinks or {}).items():
        total += int(p.get(k, BONUS_RATES.get(k, 0))) * int(v or 0)
    return total


# ─── Display name (Owner tarafından özel atanmış isim) ───
def display_name_for(db, user_id, fallback=None):
    row = db.execute("SELECT display_name, name FROM users WHERE user_id=?", (user_id,)).fetchone()
    if row and (row["display_name"] or "").strip():
        return row["display_name"].strip()
    if row and row["name"]:
        return row["name"]
    return fallback or "Бариста"


# ─── Bardak fiyatları (override + default) ───
def get_prices(db):
    """Returns {drink_id: amount}; DB override > default BONUS_RATES."""
    out = dict(BONUS_RATES)
    try:
        for r in db.execute("SELECT drink_id, amount FROM prices").fetchall():
            out[r["drink_id"]] = int(r["amount"])
    except Exception:
        pass
    return out


# ─── Audit log ───
def log_action(db, action, actor_id, actor_name, target_id=None, target_name=None, details=None):
    db.execute(
        "INSERT INTO logs (action, actor_id, actor_name, target_id, target_name, details, created_at) "
        "VALUES (?,?,?,?,?,?,?)",
        (action, actor_id, actor_name or "", target_id, target_name or "",
         json.dumps(details or {}, ensure_ascii=False),
         datetime.now(TZ).isoformat()))
    db.commit()


def calc_summary(db, user_id, period=None):
    period = period or current_period()
    # Aktif olmayan (bitmiş) vardiyaları topla
    shifts = db.execute(
        "SELECT * FROM shifts WHERE user_id=? AND period=? AND (end_time IS NOT NULL OR start_time IS NULL) ORDER BY created_at",
        (user_id, period)).fetchall()
    fines = db.execute(
        "SELECT * FROM fines WHERE user_id=? AND period=? ORDER BY created_at",
        (user_id, period)).fetchall()
    paid_row = db.execute(
        "SELECT COALESCE(SUM(amount),0) as s FROM payments WHERE user_id=? AND period=?",
        (user_id, period)).fetchone()
    tips = db.execute(
        "SELECT * FROM tips WHERE user_id=? AND period=? ORDER BY created_at",
        (user_id, period)).fetchall()
    active = get_active_shift(db, user_id)

    hours = sum(s["hours"] or 0 for s in shifts)
    bonus = sum(s["bonus"] or 0 for s in shifts)
    hourly = sum(s["hourly_pay"] or 0 for s in shifts)
    fine_total = sum(f["amount"] for f in fines)
    paid_total = paid_row["s"] or 0
    tips_total = sum(t["amount"] for t in tips)
    gross = hourly + bonus + tips_total
    net = gross - fine_total - paid_total

    return {
        "period": period,
        "hours": hours,
        "bonus": bonus,
        "hourly": hourly,
        "fines": fine_total,
        "paid": paid_total,
        "tips": tips_total,
        "tips_count": len(tips),
        "tips_list": [dict(t) for t in tips],
        "gross": gross,
        "net": net,
        "shifts_count": len(shifts),
        "fines_count": len(fines),
        "shifts": [dict(s) for s in shifts],
        "fines_list": [dict(f) for f in fines],
        "active": dict(active) if active else None,
    }


# ─── Vardiya başlat / bitir ───
def get_active_shift(db, user_id):
    return db.execute(
        "SELECT * FROM shifts WHERE user_id=? AND start_time IS NOT NULL AND end_time IS NULL "
        "ORDER BY id DESC LIMIT 1", (user_id,)).fetchone()


def _parse_user_time(s):
    """
    HTML'den gelen zamanı parse et. Kabul edilenler:
      - ISO: '2026-04-19T12:32:00...' (tam tarih+saat)
      - 'HH:MM' (sadece saat → BUGÜN için)
    Geçersizse None döner.
    """
    if not s:
        return None
    s = str(s).strip()
    try:
        # Tam ISO
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(TZ).replace(tzinfo=None) if "T" in s else None
    except Exception:
        pass
    # HH:MM formatı → bugün
    try:
        if ":" in s and len(s) <= 5:
            hh, mm = s.split(":")
            now = datetime.now(TZ)
            return now.replace(hour=int(hh), minute=int(mm), second=0, microsecond=0).replace(tzinfo=None)
    except Exception:
        pass
    return None


def start_shift(db, user_id, custom_start=None):
    """
    Aktif vardiya yoksa yeni başlat. Varsa onu döner.
    custom_start: ISO string veya 'HH:MM' (telefon kapanmışsa geriye dönük başlatma).
    """
    existing = get_active_shift(db, user_id)
    if existing:
        return existing
    now = datetime.now(TZ).replace(tzinfo=None)
    start_dt = _parse_user_time(custom_start) or now
    # Geleceğe izin verme — küçük tolerans
    if start_dt > now + timedelta(minutes=2):
        start_dt = now
    period = start_dt.strftime("%Y-%m")
    cur = db.execute(
        "INSERT INTO shifts (user_id, hours, drinks, bonus, hourly_pay, total, date, period, created_at, start_time, end_time, note) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (user_id, 0.0, json.dumps({}), 0, 0, 0,
         start_dt.strftime("%Y-%m-%d"), period, now.isoformat(),
         start_dt.isoformat(), None, ""))
    db.commit()
    return db.execute("SELECT * FROM shifts WHERE id=?", (cur.lastrowid,)).fetchone()


def end_shift(db, user_id, drinks, note="", desserts=None, custom_end=None):
    """
    Aktif vardiyayı sonlandır.
    desserts: {cookie:N, cheesecake:N, ...} — her biri 500 сум.
    custom_end: ISO/'HH:MM' — barista uygulamayı geç açtıysa gerçek bitiş saati.
    """
    active = get_active_shift(db, user_id)
    if not active:
        return None
    now = datetime.now(TZ).replace(tzinfo=None)
    end_dt = _parse_user_time(custom_end) or now
    try:
        start = datetime.fromisoformat(active["start_time"])
    except Exception:
        start = end_dt
    # Bitiş başlangıçtan önce olamaz
    if end_dt < start:
        end_dt = now
    delta_h = (end_dt - start).total_seconds() / 3600.0
    hours = round(max(0.0, delta_h), 2)
    drinks_bonus = calc_bonus(drinks, get_prices(db))
    dessert_bonus = calc_dessert_bonus(desserts, get_dessert_prices(db))
    bonus = drinks_bonus + dessert_bonus
    hourly_pay = int(hours * HOURLY_RATE)
    total = hourly_pay + bonus
    db.execute(
        "UPDATE shifts SET end_time=?, hours=?, drinks=?, bonus=?, hourly_pay=?, total=?, note=?, "
        "desserts=?, dessert_bonus=? WHERE id=?",
        (end_dt.isoformat(), hours, json.dumps(drinks or {}, ensure_ascii=False),
         bonus, hourly_pay, total, note or "",
         json.dumps(desserts or {}, ensure_ascii=False), dessert_bonus, active["id"]))
    db.commit()
    return db.execute("SELECT * FROM shifts WHERE id=?", (active["id"],)).fetchone()


def build_webapp_url(base_url, user_id, name, db):
    """Build WebApp URL with user, role, summary, active shift and (for owner) baristas embedded in hash."""
    from urllib.parse import quote
    upsert_user(db, user_id, name, None, None)
    role = get_role(db, user_id)
    s = calc_summary(db, user_id)
    # Owner tarafından atanan display_name varsa onu kullan
    show_name = display_name_for(db, user_id, fallback=name)
    prices = get_prices(db)
    summary = {
        "hours": s["hours"], "bonus": s["bonus"], "hourly": s["hourly"],
        "fines": s["fines"], "paid": s["paid"], "net": s["net"],
        "tips": s["tips"], "tips_count": s["tips_count"],
        "tips_list": s["tips_list"][-5:],
        "period": s["period"], "shifts_count": s["shifts_count"],
        "fines_count": s["fines_count"],
        "shifts": s["shifts"][-5:],
        "fines_list": s["fines_list"][-5:],
        "active": s["active"],
    }
    # Tatlı kataloğu — owner hepsini görsün (yönetim için), barista sadece aktifleri
    desserts_cat = get_dessert_catalog(db, only_active=(role != "owner"))
    # Resmi sınav daveti — beklemede mi?
    pending_invite = db.execute(
        "SELECT id, owner_name, created_at FROM rt_exam_invites "
        "WHERE barista_id=? AND status IN ('pending','active') ORDER BY id DESC LIMIT 1",
        (user_id,)).fetchone()
    pending_exam = (
        {"id": pending_invite["id"], "by": pending_invite["owner_name"] or "Шеф", "at": pending_invite["created_at"]}
        if pending_invite else None
    )
    # Recipe trainer — bu kullanıcının progress'ı
    rt_row = db.execute("SELECT * FROM rt_progress WHERE user_id=?", (user_id,)).fetchone()
    rt_self = {
        "level": rt_row["level"] if rt_row else 1,
        "maxLevel": rt_row["max_level"] if rt_row else 1,
        "xp": rt_row["xp"] if rt_row else 0,
        "bestStreak": rt_row["best_streak"] if rt_row else 0,
        "totalSessions": rt_row["total_sessions"] if rt_row else 0,
        "totalCorrect": rt_row["total_correct"] if rt_row else 0,
        "totalQuestions": rt_row["total_questions"] if rt_row else 0,
        "lastPlayed": rt_row["last_played_at"] if rt_row else None,
    }
    import hashlib
    ts = int(datetime.now(TZ).timestamp())
    pwd_row = db.execute("SELECT password FROM users WHERE user_id=?", (user_id,)).fetchone()
    pwd_raw = ((pwd_row["password"] if pwd_row else "") or "").strip()
    pwh = hashlib.sha256(pwd_raw.encode('utf-8')).hexdigest() if pwd_raw else ""
    parts = [
        f"uid={user_id}",
        f"role={role}",
        f"name={quote(show_name or '')}",
        f"pwh={pwh}",
        f"summary={quote(json.dumps(summary, ensure_ascii=False))}",
        f"prices={quote(json.dumps(prices, ensure_ascii=False))}",
        f"desserts={quote(json.dumps(desserts_cat, ensure_ascii=False))}",
        f"rt={quote(json.dumps(rt_self, ensure_ascii=False))}",
        f"exam={quote(json.dumps(pending_exam, ensure_ascii=False) if pending_exam else '')}",
        f"ts={ts}",
    ]
    if role == "owner":
        rows = db.execute(
            "SELECT user_id, name, username, role, display_name, password, authorized, "
            "COALESCE(archived,0) AS archived, archived_at "
            "FROM users ORDER BY COALESCE(archived,0), COALESCE(display_name,name)").fetchall()
        baristas = []
        for b in rows:
            bs = calc_summary(db, b["user_id"])
            real_name = (b["display_name"] or b["name"] or "?").strip()
            # Recipe trainer progress for this barista
            rtp = db.execute("SELECT * FROM rt_progress WHERE user_id=?", (b["user_id"],)).fetchone()
            rt_sess = db.execute(
                "SELECT level, correct, total, passed, played_at FROM rt_sessions "
                "WHERE user_id=? ORDER BY id DESC LIMIT 5", (b["user_id"],)).fetchall()
            rt_exam = db.execute(
                "SELECT score, passed, taken_at FROM rt_exams "
                "WHERE user_id=? ORDER BY id DESC LIMIT 1", (b["user_id"],)).fetchone()
            rt_data = {
                "lvl": rtp["level"] if rtp else 1,
                "max": rtp["max_level"] if rtp else 1,
                "xp": rtp["xp"] if rtp else 0,
                "bs": rtp["best_streak"] if rtp else 0,
                "ts": rtp["total_sessions"] if rtp else 0,
                "tc": rtp["total_correct"] if rtp else 0,
                "tq": rtp["total_questions"] if rtp else 0,
                "lp": rtp["last_played_at"] if rtp else None,
                "rec": [{"l":r["level"],"c":r["correct"],"t":r["total"],"p":r["passed"],"d":r["played_at"]} for r in rt_sess],
                "exam": ({"s":rt_exam["score"],"p":rt_exam["passed"],"d":rt_exam["taken_at"]} if rt_exam else None),
            }
            baristas.append({
                "id": b["user_id"], "n": real_name,
                "rn": b["name"] or "",
                "dn": b["display_name"] or "",
                "u": b["username"] or "",
                "r": b["role"], "h": bs["hours"], "b": bs["bonus"],
                "hp": bs["hourly"], "f": bs["fines"],
                "paid": bs["paid"], "net": bs["net"],
                "tips": bs["tips"],
                "sc": bs["shifts_count"], "fc": bs["fines_count"],
                "active": bs["active"],
                "recent": [],
                "rt": rt_data,
                "pw": 1 if (b["password"] or "").strip() else 0,
                "auth": 1 if (b["authorized"] or 0) else 0,
                "arch": 1 if (b["archived"] or 0) else 0,
                "arch_at": b["archived_at"] or "",
            })
        parts.append(f"baristas={quote(json.dumps(baristas, ensure_ascii=False))}")
    sep = "&" if "?" in base_url else "?"
    return base_url + f"{sep}v={ts}" + "#" + "&".join(parts)

# ═══════════════════════════════════════
#  ПРОДУКЦИЯ СКЛАДА (Sipariş Listesi)
# ═══════════════════════════════════════
PRODUCTS = {
    "☕ Кофе": [
        {"id": "espresso_mix",   "name": "— Кофе эспрессо смесь (1 кг) :"},
        {"id": "columbia_250",   "name": "Кофе Колумбия (250 гр)"},
        {"id": "ethiopia_250",   "name": "Кофе Эфиопия (250 гр)"},
        {"id": "brazil_250",     "name": "Кофе Бразилия (250 гр)"},
        {"id": "espresso_crema", "name": "Кофе эспрессо крема (250 гр)"},
        {"id": "decaf_250",      "name": "Кофе Декаф (250 гр)"},
        {"id": "drip_columbia",  "name": "Кофе дрип Колумбия (5 шт.)"},
        {"id": "drip_ethiopia",  "name": "Кофе дрип Эфиопия (5 шт.)"},
    ],
    "🥛 Молоко и сливки": [
        {"id": "milk_32",       "name": "Молоко 3.2% (1 уп. 12 л)"},
        {"id": "milk_almond",   "name": "Молоко миндальное (1 л)"},
        {"id": "milk_coconut",  "name": "Молоко кокосовое (1 л)"},
        {"id": "milk_lactfree", "name": "Молоко безлактозное (1 л)"},
        {"id": "cream_10",      "name": "Сливки 10% (200 мл)"},
        {"id": "cream_33",      "name": "Сливки 33% (1 л)"},
    ],
    "🍯 Сиропы и топпинги": [
        {"id": "syrup_banana",    "name": "Сироп банановый (1 л)"},
        {"id": "syrup_vanilla",   "name": "Сироп ванильный (1 л)"},
        {"id": "syrup_caramel",   "name": "Сироп карамельный (1 л)"},
        {"id": "syrup_strawberry","name": "Сироп клубничный (1 л)"},
        {"id": "syrup_coconut",   "name": "Сироп кокосовый (1 л)"},
        {"id": "syrup_lavender",  "name": "Сироп лаванды (1 л)"},
        {"id": "syrup_almond",    "name": "Сироп миндальный (1 л)"},
        {"id": "syrup_mint",      "name": "Сироп мятный (1 л)"},
        {"id": "syrup_hazelnut",  "name": "Сироп лесной орех (1 л)"},
        {"id": "syrup_saltcaramel","name":"Сироп солёная карамель (1 л)"},
        {"id": "syrup_pistachio", "name": "Сироп фисташки (1 л)"},
        {"id": "syrup_chocolate", "name": "Сироп шоколадный (1 л)"},
        {"id": "topping_choco",   "name": "Топпинг шоколадный (1 л)"},
        {"id": "puree_strawberry","name": "Пюре клубничное (850 мл)"},
    ],
    "🍊 Для заготовок": [
        {"id": "mint",          "name": "Мята (100 гр)"},
        {"id": "sea_buckthorn", "name": "Облепиха с/м (0.5 кг)"},
        {"id": "honey",         "name": "Мёд (1 кг)"},
        {"id": "ginger",        "name": "Имбирь (0.5 кг)"},
        {"id": "lemon",         "name": "Лимон (1 шт)"},
        {"id": "currant",       "name": "Смородина красная с/м (0.5 кг)"},
        {"id": "ice_cream_18",  "name": "Мороженое (1.8 кг)"},
        {"id": "juice_orange",  "name": "Сок апельсиновый (200 мл)"},
    ],
    "🥤 Упаковка": [
        {"id": "cup_100",       "name": "Стакан 100 (рукав 100 шт.)"},
        {"id": "cup_200",       "name": "Стакан 200 (рукав 37 шт.)"},
        {"id": "cup_300",       "name": "Стакан 300 (рукав 40 шт.)"},
        {"id": "cup_400",       "name": "Стакан 400 (рукав 25 шт.)"},
        {"id": "cup_dome_400",  "name": "Стакан купол 400 (рукав 20 шт.)"},
        {"id": "cup_500",       "name": "Стакан 500 (рукав 100 шт.)"},
        {"id": "lid_200",       "name": "Крышка 200 (рукав 100 шт.)"},
        {"id": "lid_dome_400",  "name": "Крышка купол 400 (рукав)"},
        {"id": "lid_300_500",   "name": "Крышка 300-500 (рукав 100 шт.)"},
        {"id": "holder_2",      "name": "Подстаканник на 2 (1 шт.)"},
        {"id": "holder_4",      "name": "Подстаканник на 4 (1 шт.)"},
        {"id": "bag_tshirt",    "name": "Пакет майка (упак)"},
        {"id": "bag_kraft",     "name": "Крафтовый пакет (50 шт.)"},
        {"id": "bag_brand",     "name": "Фирменный пакет (50 шт.)"},
        {"id": "marking_tape",  "name": "Маркировочная лента (1 шт.)"},
    ],
    "🧻 Расходники": [
        {"id": "napkins",       "name": "Салфетки (1 пачка)"},
        {"id": "straws_corrug", "name": "Трубочки гофрир. (500 шт.)"},
        {"id": "straws_flat",   "name": "Трубочки плоские (4000 шт.)"},
        {"id": "filter_cold",   "name": "Фильтры для холод. напитков (400)"},
        {"id": "receipt_tape",  "name": "Чековая лента (1 шт.)"},
        {"id": "trash_bags",    "name": "Мусорный пакет (10 шт.)"},
        {"id": "filter_batch",  "name": "Фильтры для батч бро (100 шт.)"},
        {"id": "cloth_clean",   "name": "Тряпка для уборки (1 шт.)"},
        {"id": "cloth_dolphin", "name": "Тряпка дельфин (1 шт.)"},
        {"id": "soap_hands",    "name": "Гель мыло для рук (1 л)"},
        {"id": "towels_hands",  "name": "Полотенца для рук (1 пачка)"},
        {"id": "chem_equip",    "name": "Химия для оборудования (1 кг)"},
        {"id": "gloves",        "name": "Перчатки (1 уп. 100 шт.)"},
        {"id": "nitrogen",      "name": "Баллон с азотом (10 шт.)"},
    ],
    "🍦 Штучные позиции": [
        {"id": "ice_plombir",   "name": "Мороженое пломбир (1 шт.)"},
        {"id": "ice_choco",     "name": "Мороженое шоколад (1 шт.)"},
        {"id": "ice_saltcar",   "name": "Мороженое солёная карамель (1 шт.)"},
        {"id": "ice_strawberry","name": "Мороженое клубника (1 шт.)"},
        {"id": "cookie_classic","name": "Кукис классик (1 шт.)"},
        {"id": "shoko_balls",   "name": "Shoko balls (1 шт.)"},
    ],
    "🏪 Бакалея": [
        {"id": "sugar",         "name": "Сахар (1 кг)"},
        {"id": "cacao",         "name": "Какао (500 гр)"},
        {"id": "matcha",        "name": "Матча (100 гр)"},
        {"id": "cinnamon",      "name": "Корица (100 гр)"},
        {"id": "halva",         "name": "Халва (500 гр)"},
        {"id": "flour_pistachio","name":"Мука фисташки (500 гр)"},
        {"id": "sweetener",     "name": "Сахарозаменитель (1 шт.)"},
    ],
    "💧 Вода": [
        {"id": "water_gas",     "name": "Вода с газом (уп. 12 шт.)"},
        {"id": "water_still",   "name": "Вода без газа (уп. 12 шт.)"},
    ],
}

# Flat list for lookups
ALL_PRODUCTS = []
for cat, items in PRODUCTS.items():
    for item in items:
        item["category"] = cat
        ALL_PRODUCTS.append(item)

# ═══════════════════════════════════════
#  ЗАДАЧИ (Görev Listesi)
# ═══════════════════════════════════════
TASKS = {
    "gorev": {
        "🌅 Открытие": [
            "Машина включена и прогрета",
            "Гриндер настроен (помол 23-28 сек)",
            "Молочный холодильник проверен",
            "Стаканы/крышки в наличии",
            "Касса открыта, деньги пересчитаны",
            "Барная стойка протёрта",
            "Бойлер включен (94°C)",
            "Заготовки проверены/промаркированы",
        ],
        "☀️ В течение дня": [
            "Стоки отмечены (что заканчивается)",
            "Мусор вынесен",
            "Барная стойка в порядке",
            "Зона гостя в чистоте",
            "Холдеры промыты (каждые 3 часа)",
            "Форсунки чистые после каждого использования",
        ],
        "🌙 Закрытие": [
            "Бэкфлеш кофемашины (слепой фильтр)",
            "Холдеры замочены в растворе (15 мин)",
            "Гриндер почищен (щётка)",
            "Колба для зерна помыта",
            "Молочный холодильник проверен",
            "Барная стойка и пол вымыты",
            "Мусор вынесен",
            "Касса закрыта, деньги пересчитаны",
            "Электричество и дверь проверены",
        ],
    },
    "temizlik": {
        "🧹 Ежедневная уборка": [
            "Steam wand (форсунки) — тряпка после каждого",
            "Гриндер — щётка",
            "Барная стойка — протирка и дезинфекция",
            "Пол — подмести и протереть",
            "Мусорные вёдра — опустошить",
            "Холодильник — снаружи протереть",
            "Раковина — помыть",
            "Столы гостей — протереть",
            "Витрина — протереть стекло",
            "Отбойник для кофе — опустошить",
            "Поддон под группами — промыть",
            "Тряпки замочить в дез. растворе",
        ],
        "🧽 Еженедельная уборка": [
            "Бэкфлеш кофемашины (химия)",
            "Гриндер — глубокая чистка",
            "Холодильник — внутри помыть",
            "Полки и шкафы — уборка",
            "Стены и углы — протереть",
            "Оборудование — общий осмотр",
            "Бойлер кипятка — помыть с лимонной кислотой",
        ],
        "✨ Ежемесячная уборка": [
            "Декальцинация кофемашины",
            "Водяной фильтр — проверка/замена",
            "Глубокая уборка пола",
            "Вентиляция — чистка",
            "Склад — генеральная уборка",
        ],
    },
    "okk": {
        "⚙️ Оборудование": [
            "Кофемашина: панели, мармит, поддон целые",
            "Сетки рассекателей — без деформаций",
            "Холдеры: наличие, ручки, сетки, пружины, носики",
            "Паровые форсунки — не протекают, покрытие",
            "Жернова гриндера (менять каждые 400 кг)",
            "Гриндер: корпус, колба, поддон целые",
            "Давление бойлера 1-1.3 атм",
            "Давление воды 8-9 атм при заварке",
            "Помол 23-28 сек экстракции",
            "Бойлер кипятка — 94°C, без сколов",
            "Журнал ведётся правильно",
            "Кисточка — в наличии, ворс прямой",
            "Эспрессо-питчеры — мин 2 шт на мармите",
            "Молочные питчеры — 0.3 / 0.6 / 1 л",
            "Темпер — правильный диаметр, ровный",
            "Зона TO GO — мешалки, трубочки, ложки",
            "Раковина — целая, горячая вода есть",
            "Мыло — жидкое, для рук и посуды отдельно",
            "Диспенсер Z-салфеток — целый",
            "Доска для нарезки — без трещин",
            "Батч брю и гриндер для альтернативы",
            "Касса — рабочее состояние",
            "Кремер/Сифон — без сколов, насадка",
            "Холодильники — 2-4°C, резинки целые",
            "Морозильник — -15..-18°C, стекло целое",
            "Блендер — кнопки, колба, крышка",
            "Весы — заряжены >45%, резинка",
            "Планшет/терминал — заряжен >45%, Poster",
            "Витрина круассанов — закрывается, целая",
            "Диспенсер для соуса — помпы, крышки",
            "Фильтры/умягчитель — сроки в журнале",
            "Папка бара со стандартами — актуальная",
        ],
        "🧼 Чистота (ОКК)": [
            "3 вида тряпок используются правильно",
            "Бойлер кипятка — чистый, без накипи",
            "Кофемашина: все панели, мармит, поддон чистые",
            "Форсунки — чистые, без налёта молока",
            "Рабочие группы — без кофейного нагара",
            "Холдеры — чистые, замочены 1 раз/сутки",
            "Фильтры — шкаф чистый, без пыли",
            "Гриндер — поверхность без пыли",
            "Колба для зерна — без масел, крышка чистая",
            "Отбойник — опустошен, чистый",
            "Кисточка — чистая, сухая",
            "Питчеры — чистые после каждого исп.",
            "Сиропы — помпы чистые, промаркированы",
            "Топпинг/пюре/мёд — чистые, промаркированы",
            "Касса — зона чистая, смена соотв. дате",
            "Витрина — без крошек, без разводов",
            "Холодильники — чисто внутри и снаружи",
            "Морозильник — стекло чистое, без шубы",
            "Блендер — колба чистая, без запаха",
            "Раковина — без остатков продуктов",
            "Весы — чистые, без капель",
            "Резиновый коврик — чистый",
            "Ножи/ложки/совок — чистые, отдельно",
            "График уборок соблюдается",
        ],
        "📋 Процедуры: Продукты": [
            "Зерно хранится правильно, ротация, мин 2 кг",
            "Зерно в гриндере — маркировка, макс 48 ч",
            "Молоко 3.2%: маркировка, холодильник, 48 ч",
            "Заготовки — в контейнерах, маркированы",
            "ВСЕ вскрытое промаркировано (ДДММЧЧ)",
            "Нет хранения в транспортировочной таре",
            "Альт. молоко — маркировка, мин 2 л каждого",
            "Сыпучка — закрытая тара, выше 50 см от пола",
        ],
        "👨‍🍳 Навыки бариста": [
            "Очистка холдера перед дозировкой",
            "Правильная дозировка (весы!)",
            "Формирование таблетки (угол, сила, упор)",
            "Стравливание воды из группы",
            "Очистка обода холдера от молотого кофе",
            "Быстрое нажатие (1-3 сек после установки)",
            "Форсунка: стравить → взбить → протереть",
            "Пена капучино ≥ 1.5 см, однородная",
            "Температура молока 60-70°C",
            "Контроль качества напитков",
            "Знание стандартов (тест 10 вопросов)",
        ],
        "👔 Внешний вид": [
            "Футболка/толстовка Caffelito",
            "Фартук надет",
            "Бейджик с именем",
            "Тёмные штаны без рисунков",
            "Закрытая сменная обувь",
            "Форма не носится вне кофейни",
            "Ногти коротко подстрижены",
            "Волосы чистые/собраны",
            "Украшения — только религ./венчальные",
        ],
        "🤝 Сервис": [
            "Приветствие гостя + предложение новинок",
            "Принятие заказа — upsell (большие порции)",
            "Предложение доп. блюда (сэндвич/выпечка)",
            "Повтор заказа гостю",
            "Расчёт — сумма, способ оплаты",
            "Выполнение — штучное → холодное → горячее",
            "Прощание — хорошего дня, приходите ещё",
        ],
        "📢 Маркетинг и ассортимент": [
            "Фасад чистый, наклейки целые",
            "Территория вокруг кофейни чистая",
            "Реклама актуальная, меню актуальное",
            "Фасад/вывеска/подсветка исправны",
            "Персонал знает все акции",
            "Все ингредиенты для напитков в наличии",
            "Сэндвичи в наличии (мин 1 вид)",
            "Выпечка в наличии, промаркирована",
            "Все снеки/мороженое в наличии",
            "Зерновой кофе для продажи — все виды",
            "Дрип-пакеты — мин 2 уп. каждого вида",
            "Нет посторонней продукции",
        ],
    },
}

# ═══════════════════════════════════════
#  ITEMS_PER_PAGE for order
# ═══════════════════════════════════════
ITEMS_PER_PAGE = 5

# ═══════════════════════════════════════
#  COMMANDS
# ═══════════════════════════════════════

async def cmd_login(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Şifre doğrulama: /login PAROLA
    Sırayla denenir:
      1) Bariastanın kendi şifresi (owner tarafından atanmış users.password)
      2) Eski global ACCESS_CODE (eğer set edilmişse — geri uyumluluk için)
    Owner her zaman parolasız erişebilir.
    """
    db = get_db()
    user = update.effective_user
    upsert_user(db, user.id, user.first_name, user.username, update.effective_chat.id)

    # Owner ise zaten yetkili
    if get_role(db, user.id) == "owner":
        db.execute("UPDATE users SET authorized=1 WHERE user_id=?", (user.id,))
        db.commit()
        await update.message.reply_text("👑 Вы — владелец. Доступ открыт.\n/menu чтобы открыть приложение.")
        return

    # Auth gerekmiyorsa (henüz owner yok ve ACCESS_CODE de boş)
    if not auth_required(db):
        db.execute("UPDATE users SET authorized=1 WHERE user_id=?", (user.id,))
        db.commit()
        await update.message.reply_text("ℹ️ Пароль пока не настроен — доступ открыт.\n/menu чтобы открыть приложение.")
        return

    args = context.args or []
    if not args:
        await update.message.reply_text(
            "🔑 Использование:\n`/login ВАШ_ПАРОЛЬ`\n\n"
            "Пароль выдаёт владелец кофейни.",
            parse_mode="Markdown")
        return

    given = " ".join(args).strip()

    # 1) Kendi şifresi
    row = db.execute("SELECT password FROM users WHERE user_id=?", (user.id,)).fetchone()
    own_pwd = (row["password"] if row else None) or ""
    own_pwd = own_pwd.strip()

    ok = False
    if own_pwd and given == own_pwd:
        ok = True
    elif ACCESS_CODE and given == ACCESS_CODE:
        # Eski global kod — fallback
        ok = True

    if ok:
        db.execute("UPDATE users SET authorized=1 WHERE user_id=?", (user.id,))
        db.commit()
        log_action(db, "login_ok", user.id, user.first_name, user.id, user.first_name, {})
        await update.message.reply_text(
            "✅ *Доступ открыт!*\n\nНажмите /menu чтобы открыть приложение.",
            parse_mode="Markdown")
    else:
        log_action(db, "login_fail", user.id, user.first_name, user.id, user.first_name, {})
        await update.message.reply_text(
            "❌ Неверный пароль. Попросите владельца выдать вам новый.\n`/login ВАШ_ПАРОЛЬ`",
            parse_mode="Markdown")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = get_db()
    db.execute("INSERT OR IGNORE INTO shops (chat_id) VALUES (?)",
               (update.effective_chat.id,))
    db.commit()
    user = update.effective_user
    upsert_user(db, user.id, user.first_name, user.username, update.effective_chat.id)

    # 👑 İlk yetkili kullanıcı otomatik owner olur
    auto_owner = False
    if not has_owner(db):
        db.execute("UPDATE users SET role='owner' WHERE user_id=?", (user.id,))
        db.commit()
        auto_owner = True

    # Role'e göre komut listesi + menu butonunu her start'ta senkronla
    await sync_user_ui(context.bot, db, user.id)

    chat_type = update.effective_chat.type  # 'private', 'group', 'supergroup', 'channel'

    # Grupta web_app çalışmaz — DM'ye yönlendiren inline buton gönder
    if chat_type != "private":
        bot_user = await context.bot.get_me()
        deep = f"https://t.me/{bot_user.username}?start=menu"
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("☕ Открыть Caffelito (в личке)", url=deep)]])
        await update.message.reply_text(
            "☕ *CAFFELITO*\n\nЭто приложение работает только в личных сообщениях.\nНажмите кнопку, чтобы открыть бота 👇",
            reply_markup=kb, parse_mode="Markdown")
        return

    # DM — web_app butonu (ReplyKeyboard)
    if WEBAPP_URL:
        url = build_webapp_url(WEBAPP_URL, user.id, user.first_name, db)
        reply_kb = ReplyKeyboardMarkup(
            [[KeyboardButton("☕ Открыть Caffelito", web_app=WebAppInfo(url=url))]],
            resize_keyboard=True
        )
    else:
        reply_kb = None

    role_now = get_role(db, user.id)
    role_note = ""
    if auto_owner:
        role_note = "\n\n👑 *Вы автоматически назначены владельцем* (первый пользователь)."
    elif role_now == "owner":
        role_note = "\n\n👑 Вы — *владелец*."
    else:
        role_note = "\n\n👤 Вы — *бариста*. Чтобы стать владельцем — /setowner (если ещё нет владельца) или попросите владельца /grantowner."

    try:
        await update.message.reply_text(
            "☕ *CAFFELITO BOT*\n\n"
            "Нажмите кнопку ниже чтобы открыть приложение 👇" + role_note,
            reply_markup=reply_kb,
            parse_mode="Markdown")
    except Exception as e:
        logger.error(f"start reply with keyboard failed: {e}")
        await update.message.reply_text(
            "☕ *CAFFELITO BOT*\n\n"
            "⚠️ Слишком много данных для кнопки. Откройте приложение через меню (≡) внизу." + role_note,
            parse_mode="Markdown")


async def cmd_order(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["order"] = {}
    await show_order_categories(update.message, context)

async def cmd_gorev(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_task_menu(update.message, "gorev")

async def cmd_temizlik(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_task_menu(update.message, "temizlik")

async def cmd_okk(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_task_menu(update.message, "okk")

async def cmd_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_report(update.message, update.effective_chat.id)


# ═══════════════════════════════════════
#  ЗАРПЛАТА (КОМАНДЫ)
# ═══════════════════════════════════════

async def cmd_setowner(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = get_db()
    user = update.effective_user
    upsert_user(db, user.id, user.first_name, user.username, update.effective_chat.id)
    if has_owner(db):
        if get_role(db, user.id) == "owner":
            await update.message.reply_text("✅ Вы уже владелец.")
        else:
            await update.message.reply_text(
                "❌ Владелец уже назначен.\n\n"
                "Попросите его выдать вам права через /grantowner @username.")
        return
    db.execute("UPDATE users SET role='owner' WHERE user_id=?", (user.id,))
    db.commit()
    await sync_user_ui(context.bot, db, user.id)
    await update.message.reply_text(
        f"👑 *Вы — владелец!*\n\n"
        f"Имя: {user.first_name}\n"
        f"ID: `{user.id}`\n\n"
        f"Откройте приложение — теперь вам доступна панель управления зарплатой.\n"
        f"Команды: /zarplata, /baristas, /shtraf, /paid, /grantowner",
        parse_mode="Markdown")


async def cmd_maosh(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = get_db()
    user = update.effective_user
    upsert_user(db, user.id, user.first_name, user.username, update.effective_chat.id)
    s = calc_summary(db, user.id)
    text = (f"💰 *Моя зарплата — {s['period']}*\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"⏱️ Часы: *{s['hours']:g}* × 12.000 = *{fmt_sum(s['hourly'])}* сум\n"
            f"🥤 Бонус ({s['shifts_count']} смен): *{fmt_sum(s['bonus'])}* сум\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"💵 Брутто: *{fmt_sum(s['gross'])}* сум\n"
            f"⚠️ Штрафы ({s['fines_count']}): *-{fmt_sum(s['fines'])}* сум\n")
    if s['paid'] > 0:
        text += f"✅ Уже выплачено: *-{fmt_sum(s['paid'])}* сум\n"
    text += (f"━━━━━━━━━━━━━━━━━━\n"
             f"💎 *ИТОГО: {fmt_sum(s['net'])} сум*")
    await update.message.reply_text(text, parse_mode="Markdown")


async def cmd_baristalar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = get_db()
    user = update.effective_user
    if get_role(db, user.id) != "owner":
        await update.message.reply_text("❌ Команда только для владельца.")
        return
    rows = db.execute("SELECT * FROM users ORDER BY role DESC, name").fetchall()
    if not rows:
        await update.message.reply_text("Нет пользователей.")
        return
    period = current_period()
    text = f"👥 *Все сотрудники — {period}*\n━━━━━━━━━━━━━━━━━━"
    for u in rows:
        s = calc_summary(db, u["user_id"], period)
        crown = "👑" if u["role"] == "owner" else "👤"
        text += (f"\n\n{crown} *{u['name']}* (`{u['user_id']}`)\n"
                 f"  Часы: {s['hours']:g}h | Бонус: {fmt_sum(s['bonus'])}\n"
                 f"  Штр: -{fmt_sum(s['fines'])} | Выпл: -{fmt_sum(s['paid'])}\n"
                 f"  💎 *= {fmt_sum(s['net'])} сум*")
    await update.message.reply_text(text, parse_mode="Markdown")


async def cmd_grantowner(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = get_db()
    user = update.effective_user
    if get_role(db, user.id) != "owner":
        await update.message.reply_text("❌ Только владелец может выдавать роли.")
        return
    if not context.args:
        await update.message.reply_text("Использование: /grantowner @username\nили /grantowner <user_id>")
        return
    target = find_user(db, context.args[0])
    if not target:
        await update.message.reply_text(
            f"❌ Не найден: {context.args[0]}\n\n"
            f"Этот человек должен сначала написать боту /start.")
        return
    db.execute("UPDATE users SET role='owner' WHERE user_id=?", (target["user_id"],))
    db.commit()
    await sync_user_ui(context.bot, db, target["user_id"])
    await update.message.reply_text(f"👑 {target['name']} теперь *владелец*.", parse_mode="Markdown")
    try:
        await context.bot.send_message(
            target["user_id"],
            f"👑 Вам выдали роль *владельца*!\n\nОт: {user.first_name}\n"
            f"Откройте приложение через /menu чтобы увидеть панель управления.",
            parse_mode="Markdown")
    except Exception as e:
        logger.warning(f"Notify owner failed: {e}")


async def cmd_addbarista(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = get_db()
    user = update.effective_user
    if get_role(db, user.id) != "owner":
        await update.message.reply_text("❌ Только владелец.")
        return
    if not context.args:
        await update.message.reply_text("Использование: /addbarista @username")
        return
    target = find_user(db, context.args[0])
    if not target:
        await update.message.reply_text(
            f"❌ Не найден: {context.args[0]}\n\n"
            f"Этот человек должен сначала написать боту /start.")
        return
    db.execute("UPDATE users SET role='barista' WHERE user_id=?", (target["user_id"],))
    db.commit()
    await sync_user_ui(context.bot, db, target["user_id"])
    await update.message.reply_text(f"✅ {target['name']} — теперь бариста.")


async def cmd_revoke(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = get_db()
    user = update.effective_user
    if get_role(db, user.id) != "owner":
        await update.message.reply_text("❌ Только владелец.")
        return
    if not context.args:
        await update.message.reply_text("Использование: /revoke @username — снять роль владельца")
        return
    target = find_user(db, context.args[0])
    if not target:
        await update.message.reply_text(f"❌ Не найден: {context.args[0]}")
        return
    if target["user_id"] == user.id:
        owner_count = db.execute("SELECT COUNT(*) as c FROM users WHERE role='owner'").fetchone()["c"]
        if owner_count <= 1:
            await update.message.reply_text("❌ Вы единственный владелец — нельзя снять роль.")
            return
    db.execute("UPDATE users SET role='barista' WHERE user_id=?", (target["user_id"],))
    db.commit()
    await sync_user_ui(context.bot, db, target["user_id"])
    await update.message.reply_text(f"✅ {target['name']}: роль владельца снята.")


async def cmd_ceza(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = get_db()
    user = update.effective_user
    if get_role(db, user.id) != "owner":
        await update.message.reply_text("❌ Только владелец может выписывать штрафы.")
        return
    if len(context.args) < 2:
        await update.message.reply_text(
            "Использование: /ceza @username 30000 [причина]\n\n"
            "Примеры:\n"
            "/ceza @ahmet 30000 Чистота\n"
            "/ceza ahmet 1000000 Проверка 75%")
        return
    target = find_user(db, context.args[0])
    if not target:
        await update.message.reply_text(f"❌ Не найден: {context.args[0]}")
        return
    try:
        amount = int(context.args[1])
    except ValueError:
        await update.message.reply_text("❌ Сумма должна быть числом.")
        return
    reason = " ".join(context.args[2:]) or "Без причины"
    period = current_period()
    db.execute(
        "INSERT INTO fines (user_id, amount, reason, type, period, added_by, added_by_name, created_at) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (target["user_id"], amount, reason, "manual", period,
         user.id, user.first_name, datetime.now(TZ).isoformat()))
    db.commit()
    await update.message.reply_text(
        f"⚠️ Штраф выписан\n\n"
        f"Кому: {target['name']}\n"
        f"Сумма: -{fmt_sum(amount)} сум\n"
        f"Причина: {reason}")
    try:
        await context.bot.send_message(
            target["user_id"],
            f"⚠️ *Вам начислен штраф*\n\n"
            f"Сумма: *-{fmt_sum(amount)}* сум\n"
            f"Причина: {reason}\n"
            f"От: {user.first_name}\n\n"
            f"Текущий баланс: /zarplata",
            parse_mode="Markdown")
    except Exception as e:
        logger.warning(f"Notify fine failed: {e}")


async def cmd_setname(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Owner: /setname @user Yeni Display İsim"""
    db = get_db()
    user = update.effective_user
    if get_role(db, user.id) != "owner":
        await update.message.reply_text("❌ Только владелец.")
        return
    if len(context.args) < 2:
        await update.message.reply_text(
            "Использование: /setname @username Новое Имя\n\n"
            "Пример: /setname @ahmet Ахмет К.")
        return
    target = find_user(db, context.args[0])
    if not target:
        await update.message.reply_text(f"❌ Не найден: {context.args[0]}\n\nПопросите написать /start.")
        return
    new_name = " ".join(context.args[1:]).strip()
    db.execute("UPDATE users SET display_name=? WHERE user_id=?", (new_name or None, target["user_id"]))
    db.commit()
    log_action(db, "rename", user.id, user.first_name, target["user_id"], new_name,
               {"old": target["name"], "new": new_name})
    await update.message.reply_text(
        f"✏️ Имя обновлено\n@{target['username'] or target['user_id']} → *{new_name}*",
        parse_mode="Markdown")


async def cmd_setprice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Owner: /setprice ml200 800"""
    db = get_db()
    user = update.effective_user
    if get_role(db, user.id) != "owner":
        await update.message.reply_text("❌ Только владелец.")
        return
    if len(context.args) < 2:
        prices = get_prices(db)
        text = "💰 *Текущие цены за стакан:*\n━━━━━━━━━━━━━━━━━━\n"
        for k, v in prices.items():
            text += f"`{k}` — *{fmt_sum(v)}* сум\n"
        text += "\nИспользование: /setprice <id> <сум>\nПример: /setprice ml200 800"
        await update.message.reply_text(text, parse_mode="Markdown")
        return
    drink_id = context.args[0].strip()
    try:
        amount = int(context.args[1])
    except ValueError:
        await update.message.reply_text("❌ Сумма должна быть числом.")
        return
    old = db.execute("SELECT amount FROM prices WHERE drink_id=?", (drink_id,)).fetchone()
    old_amt = (old["amount"] if old else BONUS_RATES.get(drink_id, 0))
    db.execute(
        "INSERT INTO prices (drink_id, amount, updated_by, updated_by_name, updated_at) VALUES (?,?,?,?,?) "
        "ON CONFLICT(drink_id) DO UPDATE SET amount=excluded.amount, updated_by=excluded.updated_by, "
        "updated_by_name=excluded.updated_by_name, updated_at=excluded.updated_at",
        (drink_id, amount, user.id, user.first_name, datetime.now(TZ).isoformat()))
    db.commit()
    log_action(db, "price_update", user.id, user.first_name, None, None,
               {"drink_id": drink_id, "old": old_amt, "new": amount})
    await update.message.reply_text(
        f"💰 *{drink_id}*: {fmt_sum(old_amt)} → *{fmt_sum(amount)}* сум", parse_mode="Markdown")


async def cmd_tip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Owner: /tip @user 50000 [açıklama]"""
    db = get_db()
    user = update.effective_user
    if get_role(db, user.id) != "owner":
        await update.message.reply_text("❌ Только владелец.")
        return
    if len(context.args) < 2:
        await update.message.reply_text(
            "Использование: /tip @username 50000 [заметка]\n\n"
            "Для распределения по нескольким сразу — откройте Mini App.")
        return
    target = find_user(db, context.args[0])
    if not target:
        await update.message.reply_text(f"❌ Не найден: {context.args[0]}")
        return
    try:
        amount = int(context.args[1])
    except ValueError:
        await update.message.reply_text("❌ Сумма должна быть числом.")
        return
    note = " ".join(context.args[2:]).strip()
    period = current_period()
    db.execute(
        "INSERT INTO tips (user_id, amount, period, note, added_by, added_by_name, created_at) "
        "VALUES (?,?,?,?,?,?,?)",
        (target["user_id"], amount, period, note, user.id, user.first_name,
         datetime.now(TZ).isoformat()))
    db.commit()
    log_action(db, "tip_add", user.id, user.first_name, target["user_id"],
               display_name_for(db, target["user_id"]),
               {"amount": amount, "period": period, "note": note})
    await update.message.reply_text(
        f"💝 Чаевые: +{fmt_sum(amount)} сум → {display_name_for(db, target['user_id'])}")
    try:
        await context.bot.send_message(
            target["user_id"],
            f"💝 *Вам начислены чаевые!*\n\nСумма: *+{fmt_sum(amount)}* сум\n" +
            (f"📝 {note}\n" if note else "") +
            f"От: {user.first_name}\n\nБаланс: /zarplata", parse_mode="Markdown")
    except Exception as e:
        logger.warning(f"Notify tip failed: {e}")


async def cmd_logs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Owner: son 20 işlem"""
    db = get_db()
    user = update.effective_user
    if get_role(db, user.id) != "owner":
        await update.message.reply_text("❌ Только владелец.")
        return
    rows = db.execute("SELECT * FROM logs ORDER BY id DESC LIMIT 20").fetchall()
    if not rows:
        await update.message.reply_text("📜 Логов нет.")
        return
    icons = {"fine_add": "⚠️", "pay": "✅", "tip_add": "💝", "price_update": "💰",
             "rename": "✏️", "role_change": "🔑"}
    text = "📜 *Последние 20 действий*\n━━━━━━━━━━━━━━━━━━"
    for r in rows:
        try:
            dt = datetime.fromisoformat(r["created_at"]).strftime("%d.%m %H:%M")
        except Exception:
            dt = "?"
        ic = icons.get(r["action"], "•")
        actor = r["actor_name"] or "?"
        target = r["target_name"] or ""
        try:
            d = json.loads(r["details"] or "{}")
        except Exception:
            d = {}
        extra = ""
        if "amount" in d:
            extra = f" · {fmt_sum(d['amount'])} сум"
        elif "new" in d:
            extra = f" · {d['new']}"
        line = f"{ic} *{dt}* — {actor} → {target}{extra}"
        if d.get("reason"):
            line += f"\n  💬 _{d['reason']}_"
        text += "\n" + line
    await update.message.reply_text(text, parse_mode="Markdown")


async def cmd_odendi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = get_db()
    user = update.effective_user
    if get_role(db, user.id) != "owner":
        await update.message.reply_text("❌ Только владелец.")
        return
    if not context.args:
        await update.message.reply_text(
            "Использование: /odendi @username [месяц 2026-04]\n\n"
            "Отметит текущий баланс как выплаченный.")
        return
    target = find_user(db, context.args[0])
    if not target:
        await update.message.reply_text(f"❌ Не найден: {context.args[0]}")
        return
    period = context.args[1] if len(context.args) > 1 else current_period()
    s = calc_summary(db, target["user_id"], period)
    if s["net"] <= 0:
        await update.message.reply_text(f"❌ Нет средств для выплаты ({fmt_sum(s['net'])} сум).")
        return
    db.execute(
        "INSERT INTO payments (user_id, amount, period, paid_by, paid_by_name, paid_at) "
        "VALUES (?,?,?,?,?,?)",
        (target["user_id"], s["net"], period, user.id, user.first_name,
         datetime.now(TZ).isoformat()))
    db.commit()
    await update.message.reply_text(
        f"✅ Выплата записана\n\n"
        f"Кому: {target['name']}\n"
        f"Период: {period}\n"
        f"Сумма: {fmt_sum(s['net'])} сум")
    try:
        await context.bot.send_message(
            target["user_id"],
            f"💵 *Зарплата выплачена!*\n\n"
            f"Период: {period}\n"
            f"Сумма: *{fmt_sum(s['net'])}* сум\n"
            f"От: {user.first_name}",
            parse_mode="Markdown")
    except Exception as e:
        logger.warning(f"Notify pay failed: {e}")


# ═══════════════════════════════════════
#  ORDER SYSTEM
# ═══════════════════════════════════════

async def show_order_categories(message, context, edit=False):
    keyboard = []
    order = context.user_data.get("order", {})
    for cat_name in PRODUCTS.keys():
        # Count items in this category
        cat_items = PRODUCTS[cat_name]
        cat_count = sum(order.get(p["id"], 0) for p in cat_items)
        badge = f" ({cat_count})" if cat_count > 0 else ""
        keyboard.append([InlineKeyboardButton(
            f"{cat_name}{badge}",
            callback_data=f"ocat_{cat_name[:20]}")])

    total = sum(order.values())
    keyboard.append([InlineKeyboardButton(
        f"🛒 Корзина ({total} шт.)" if total > 0 else "🛒 Корзина пуста",
        callback_data="ord_basket")])
    if total > 0:
        keyboard.append([InlineKeyboardButton(
            "✅ ОТПРАВИТЬ ЗАКАЗ", callback_data="ord_submit")])
    keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="ord_cancel")])

    text = "📦 *ЗАКАЗ*\nВыберите категорию:"
    if edit:
        await message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard),
                                parse_mode="Markdown")
    else:
        await message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard),
                                 parse_mode="Markdown")


async def show_order_category(message, context, cat_name, edit=False):
    order = context.user_data.get("order", {})
    items = PRODUCTS.get(cat_name, [])
    keyboard = []
    for p in items:
        qty = order.get(p["id"], 0)
        qty_text = f" → {qty}" if qty > 0 else ""
        short_name = p["name"][:30]
        keyboard.append([
            InlineKeyboardButton("➖", callback_data=f"om_{p['id']}_{cat_name[:20]}"),
            InlineKeyboardButton(f"{short_name}{qty_text}", callback_data="noop"),
            InlineKeyboardButton("➕", callback_data=f"op_{p['id']}_{cat_name[:20]}"),
        ])
    keyboard.append([InlineKeyboardButton("⬅️ Назад к категориям",
                     callback_data="ord_back_cats")])

    text = f"📦 *{cat_name}*"
    if edit:
        await message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard),
                                parse_mode="Markdown")
    else:
        await message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard),
                                 parse_mode="Markdown")


# ═══════════════════════════════════════
#  TASK / CHECKLIST SYSTEM
# ═══════════════════════════════════════

async def show_task_menu(message, category, edit=False):
    source = TASKS[category]
    titles = {"gorev": "📋 ЗАДАЧИ СМЕНЫ", "temizlik": "🧹 УБОРКА",
              "okk": "✅ ПРОВЕРКА ОКК"}
    keyboard = []
    for key in source:
        keyboard.append([InlineKeyboardButton(key,
                         callback_data=f"tcat_{category}_{key[:25]}")])

    text = f"*{titles.get(category, category)}*\nВыберите раздел:"
    if edit:
        await message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard),
                                parse_mode="Markdown")
    else:
        await message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard),
                                 parse_mode="Markdown")


async def show_task_list(message, context, category, sub_key, edit=False):
    source = TASKS[category]
    items = source.get(sub_key, [])
    state_key = f"{category}_{sub_key}"
    if state_key not in context.chat_data:
        context.chat_data[state_key] = [False] * len(items)
    states = context.chat_data[state_key]

    # Ensure length matches
    if len(states) != len(items):
        context.chat_data[state_key] = [False] * len(items)
        states = context.chat_data[state_key]

    keyboard = []
    for i, item in enumerate(items):
        status = "✅" if states[i] else "⬜"
        short_item = item[:45]
        keyboard.append([InlineKeyboardButton(
            f"{status} {short_item}",
            callback_data=f"ttog_{category}_{sub_key[:25]}_{i}")])

    done = sum(states)
    total = len(states)
    keyboard.append([InlineKeyboardButton(
        f"📊 {done}/{total}", callback_data="noop")])
    if done == total and total > 0:
        keyboard.append([InlineKeyboardButton(
            "🎉 ВСЁ ГОТОВО — Сохранить!",
            callback_data=f"tsave_{category}_{sub_key[:25]}")])
    keyboard.append([
        InlineKeyboardButton("🔄 Сброс",
                             callback_data=f"treset_{category}_{sub_key[:25]}"),
        InlineKeyboardButton("⬅️ Назад",
                             callback_data=f"tback_{category}"),
    ])

    now = datetime.now(TZ).strftime("%d.%m.%Y %H:%M")
    text = f"*{sub_key}*\n📅 {now}\n\nНажмите чтобы отметить:"

    if edit:
        await message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard),
                                parse_mode="Markdown")
    else:
        await message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard),
                                 parse_mode="Markdown")


# ═══════════════════════════════════════
#  REPORT
# ═══════════════════════════════════════

async def show_report(message, chat_id):
    db = get_db()
    today = datetime.now(TZ).strftime("%Y-%m-%d")
    orders = db.execute(
        "SELECT * FROM orders WHERE chat_id=? AND date(created_at)=? ORDER BY created_at DESC LIMIT 5",
        (chat_id, today)).fetchall()
    tasks = db.execute(
        "SELECT * FROM tasks WHERE chat_id=? AND date=? ORDER BY created_at DESC",
        (chat_id, today)).fetchall()

    text = f"📊 *ОТЧЁТ — {datetime.now(TZ).strftime('%d.%m.%Y')}*\n\n"

    if orders:
        text += "📦 *ЗАКАЗЫ:*\n"
        for o in orders:
            items = json.loads(o["items"])
            time_str = o["created_at"].split("T")[1][:5] if "T" in o["created_at"] else ""
            text += f"  🕐 {time_str} — {o['user_name']}\n"
            for pid, qty in items.items():
                p = next((x for x in ALL_PRODUCTS if x["id"] == pid), None)
                name = p["name"] if p else pid
                text += f"    • {name}: {qty}\n"
        text += "\n"
    else:
        text += "📦 Заказов нет\n\n"

    if tasks:
        text += "📋 *ВЫПОЛНЕННЫЕ ЗАДАЧИ:*\n"
        for t in tasks:
            tl = json.loads(t["tasks"])
            time_str = t["created_at"].split("T")[1][:5] if "T" in t["created_at"] else ""
            text += f"  🕐 {time_str} — {t['user_name']} — {t['category']}\n"
            for item in tl[:5]:
                text += f"    ✅ {item}\n"
            if len(tl) > 5:
                text += f"    ... и ещё {len(tl)-5}\n"
    else:
        text += "📋 Задач нет\n"

    await message.reply_text(text, parse_mode="Markdown")


# ═══════════════════════════════════════
#  CALLBACK HANDLER
# ═══════════════════════════════════════

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "noop":
        return

    # ─── Main Menu ───
    if data == "menu_order":
        context.user_data["order"] = {}
        await show_order_categories(query.message, context)
        return
    if data == "menu_gorev":
        await show_task_menu(query.message, "gorev")
        return
    if data == "menu_temizlik":
        await show_task_menu(query.message, "temizlik")
        return
    if data == "menu_okk":
        await show_task_menu(query.message, "okk")
        return
    if data == "menu_report":
        await show_report(query.message, update.effective_chat.id)
        return

    # ─── Order: Category select ───
    if data.startswith("ocat_"):
        cat_prefix = data[5:]
        for cat_name in PRODUCTS:
            if cat_name[:20] == cat_prefix:
                await show_order_category(query.message, context, cat_name, edit=True)
                return
        return

    # ─── Order: Plus/Minus ───
    if data.startswith("op_") or data.startswith("om_"):
        parts = data.split("_", 2)
        action = parts[0]  # op or om
        pid = parts[1]
        cat_prefix = parts[2] if len(parts) > 2 else ""
        order = context.user_data.setdefault("order", {})

        if action == "op":
            order[pid] = order.get(pid, 0) + 1
        elif action == "om" and order.get(pid, 0) > 0:
            order[pid] = order[pid] - 1
            if order[pid] == 0:
                del order[pid]

        for cat_name in PRODUCTS:
            if cat_name[:20] == cat_prefix:
                await show_order_category(query.message, context, cat_name, edit=True)
                return
        return

    # ─── Order: Back to categories ───
    if data == "ord_back_cats":
        await show_order_categories(query.message, context, edit=True)
        return

    # ─── Order: Basket ───
    if data == "ord_basket":
        order = context.user_data.get("order", {})
        if not order:
            await query.answer("Корзина пуста!", show_alert=True)
            return
        text = "🛒 *КОРЗИНА:*\n\n"
        for pid, qty in order.items():
            p = next((x for x in ALL_PRODUCTS if x["id"] == pid), None)
            name = p["name"] if p else pid
            text += f"  {name}: *{qty}*\n"
        await query.message.reply_text(text, parse_mode="Markdown")
        return

    # ─── Order: Submit ───
    if data == "ord_submit":
        order = context.user_data.get("order", {})
        if not order:
            await query.answer("Корзина пуста!", show_alert=True)
            return
        user = update.effective_user
        now = datetime.now(TZ)
        db = get_db()
        db.execute(
            "INSERT INTO orders (chat_id, user_id, user_name, items, created_at) VALUES (?,?,?,?,?)",
            (update.effective_chat.id, user.id, user.first_name,
             json.dumps(order), now.isoformat()))
        db.commit()

        text = f"✅ *ЗАКАЗ ОТПРАВЛЕН!*\n👤 {user.first_name}\n📅 {now.strftime('%d.%m.%Y %H:%M')}\n\n"
        for pid, qty in order.items():
            p = next((x for x in ALL_PRODUCTS if x["id"] == pid), None)
            name = p["name"] if p else pid
            text += f"  {name}: *{qty}*\n"
        context.user_data["order"] = {}
        await query.message.edit_text(text, parse_mode="Markdown")
        return

    if data == "ord_cancel":
        context.user_data["order"] = {}
        await query.message.edit_text("❌ Заказ отменён.")
        return

    # ─── Tasks: Category ───
    if data.startswith("tcat_"):
        rest = data[5:]
        cat = rest.split("_", 1)[0]
        sub_prefix = rest.split("_", 1)[1] if "_" in rest else ""
        source = TASKS.get(cat, {})
        for key in source:
            if key[:25] == sub_prefix:
                await show_task_list(query.message, context, cat, key)
                return
        return

    # ─── Tasks: Toggle ───
    if data.startswith("ttog_"):
        rest = data[5:]
        parts = rest.rsplit("_", 1)
        idx = int(parts[1])
        cat_sub = parts[0]
        cat = cat_sub.split("_", 1)[0]
        sub_prefix = cat_sub.split("_", 1)[1] if "_" in cat_sub else ""

        source = TASKS.get(cat, {})
        for key in source:
            if key[:25] == sub_prefix:
                state_key = f"{cat}_{key}"
                if state_key not in context.chat_data:
                    context.chat_data[state_key] = [False] * len(source[key])
                states = context.chat_data[state_key]
                if idx < len(states):
                    states[idx] = not states[idx]
                await show_task_list(query.message, context, cat, key, edit=True)
                return
        return

    # ─── Tasks: Save ───
    if data.startswith("tsave_"):
        rest = data[6:]
        cat = rest.split("_", 1)[0]
        sub_prefix = rest.split("_", 1)[1] if "_" in rest else ""
        source = TASKS.get(cat, {})
        for key in source:
            if key[:25] == sub_prefix:
                state_key = f"{cat}_{key}"
                states = context.chat_data.get(state_key, [])
                completed = [source[key][i] for i, d in enumerate(states) if d]
                user = update.effective_user
                now = datetime.now(TZ)
                db = get_db()
                db.execute(
                    "INSERT INTO tasks (chat_id, user_id, user_name, category, tasks, date, created_at) "
                    "VALUES (?,?,?,?,?,?,?)",
                    (update.effective_chat.id, user.id, user.first_name,
                     f"{cat}: {key}", json.dumps(completed, ensure_ascii=False),
                     now.strftime("%Y-%m-%d"), now.isoformat()))
                db.commit()
                context.chat_data[state_key] = [False] * len(source[key])
                await query.message.edit_text(
                    f"🎉 *{key}*\n\nВсё выполнено и сохранено!\n"
                    f"👤 {user.first_name}\n📅 {now.strftime('%d.%m.%Y %H:%M')}",
                    parse_mode="Markdown")
                return
        return

    # ─── Tasks: Reset ───
    if data.startswith("treset_"):
        rest = data[7:]
        cat = rest.split("_", 1)[0]
        sub_prefix = rest.split("_", 1)[1] if "_" in rest else ""
        source = TASKS.get(cat, {})
        for key in source:
            if key[:25] == sub_prefix:
                context.chat_data[f"{cat}_{key}"] = [False] * len(source[key])
                await show_task_list(query.message, context, cat, key, edit=True)
                return
        return

    # ─── Tasks: Back ───
    if data.startswith("tback_"):
        cat = data[6:]
        await show_task_menu(query.message, cat)
        return


# ═══════════════════════════════════════
#  WEBAPP DATA
# ═══════════════════════════════════════

async def handle_webapp_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Mini App'ten gelen veriyi işle ve gruba ilet"""
    logger.info("=== WEBAPP DATA RECEIVED ===")
    db = get_db()
    user = update.effective_user
    upsert_user(db, user.id, user.first_name, user.username, update.effective_chat.id)
    if not is_authorized(db, user.id):
        await update.message.reply_text(
            "🔒 У вас нет доступа. Попросите владельца назначить пароль.",
            parse_mode="Markdown")
        return

    # Mini App ID → güzel isim
    NAMES = {
        "esp":"Кофе эспрессо смесь (1кг)","col":"Кофе Колумбия (250г)","eth":"Кофе Эфиопия (250г)",
        "brz":"Кофе Бразилия (250г)","crm":"Кофе эспрессо крема (250г)","dcf":"Кофе Декаф (250г)",
        "dc":"Дрип Колумбия (5шт)","de":"Дрип Эфиопия (5шт)",
        "m32":"Молоко 3.2% (12л)","mal":"Молоко миндальное (1л)","mco":"Молоко кокосовое (1л)",
        "mlf":"Молоко безлактозное (1л)","c10":"Сливки 10% (200мл)","c33":"Сливки 33% (1л)",
        "sb":"Сироп банановый (1л)","sv":"Сироп ванильный (1л)","sk":"Сироп карамельный (1л)",
        "ss":"Сироп клубничный (1л)","sco":"Сироп кокосовый (1л)","sl":"Сироп лаванды (1л)",
        "sa":"Сироп миндальный (1л)","sm":"Сироп мятный (1л)","sh":"Сироп лесной орех (1л)",
        "ssc":"Сироп солёная карамель (1л)","sp":"Сироп фисташки (1л)","sch":"Сироп шоколадный (1л)",
        "tc":"Топпинг шоколадный (1л)","pk":"Пюре клубничное (850мл)",
        "mnt":"Мята (100г)","obl":"Облепиха с/м (0.5кг)","med":"Мёд (1кг)",
        "imb":"Имбирь (0.5кг)","lim":"Лимон (1шт)","smr":"Смородина кр. (0.5кг)",
        "mr":"Мороженое (1.8кг)","sok":"Сок апельсиновый (200мл)",
        "k1":"Стакан (100мл)","k2":"Стакан (200мл)","k3":"Стакан (300мл)",
        "k4":"Стакан (400мл)","kd":"Купол (400мл)","k5":"Стакан (500мл)",
        "l2":"Крышка (200)","ld":"Крышка купол (400)","l3":"Крышка (300-500)",
        "h2":"Подстаканник (на 2)","h4":"Подстаканник (на 4)",
        "pt":"Пакет майка (уп.)","kr":"Крафт пакет (50шт)","fr":"Фирм. пакет (50шт)","ml":"Марк. лента (1шт)",
        "slv":"Салфетки (пачка)","tg2":"Трубочки гофр. (500шт)","tf":"Трубочки плоские (4000шт)",
        "fh":"Фильтры холод. (400шт)","ch":"Чековая лента (1шт)","ms":"Мусорный пакет (10шт)",
        "fb":"Фильтры батч (100шт)","tu":"Тряпка уборки (1шт)","td":"Тряпка дельфин (1шт)",
        "gm":"Гель мыло рук (1л)","pr":"Полотенца рук (пачка)","xo":"Химия оборуд. (1кг)",
        "pe":"Перчатки (100шт)","ba":"Баллон азот (10шт)",
        "ip":"Мороженое пломбир (1шт)","ic":"Мороженое шоколад (1шт)",
        "is":"Мороженое сол.карам. (1шт)","ik":"Мороженое клубника (1шт)",
        "cu":"Кукис классик (1шт)","sb2":"Shoko balls (1шт)",
        "sug":"Сахар (1кг)","cac":"Какао (500г)","mat":"Матча (100г)","cin":"Корица (100г)",
        "hal":"Халва (500г)","fpi":"Мука фисташки (500г)","szm":"Сахарозаменитель (1шт)",
        "wg":"Вода с газом (уп.)","ws":"Вода без газа (уп.)",
    }

    try:
        raw = update.effective_message.web_app_data.data
        logger.info(f"Raw data: {raw[:200]}")
        data = json.loads(raw)
        # Hem eski hem yeni format desteği
        action = data.get("action") or data.get("a")
        if action == "o":
            action = "order"
        user = update.effective_user
        now = datetime.now(TZ)

        group_id = GROUP_CHAT_ID or context.bot_data.get("group_id")

        if action == "order":
            from html import escape as esc_html
            total = data.get("c", 0)
            groups = data.get("g", [])

            # HTML mesaj oluştur
            text = f"<b>ЗАКАЗ — CAFFELITO</b>\n"
            text += f"━━━━━━━━━━━━━━━━━━━━\n"
            text += f"<b>{esc_html(user.first_name)}</b>\n"
            text += f"<b>{now.strftime('%d.%m.%Y  %H:%M')}</b>\n"
            text += f"━━━━━━━━━━━━━━━━━━━━\n"

            if groups:
                # Yeni kompakt format: ["Кофе|Эспрессо:1|Колумбия:3", ...]
                for group_str in groups:
                    parts = group_str.split('|')
                    cat_name = parts[0] if parts else "Прочее"
                    text += f"\n<b>{esc_html(cat_name)}:</b>\n"
                    for item_str in parts[1:]:
                        if ':' in item_str:
                            iname, iqty = item_str.rsplit(':', 1)
                            text += f"<b>  — {esc_html(iname)}:  {iqty}x</b>\n"
                        else:
                            text += f"<b>  — {esc_html(item_str)}</b>\n"
            else:
                # Eski format desteği
                items = data.get("items") or data.get("i", {})
                names_from_app = data.get("names") or data.get("n", {})
                total = sum(items.values()) if items else total
                for pid, qty in items.items():
                    name = names_from_app.get(pid) or NAMES.get(pid, pid)
                    text += f"<b>  — {esc_html(name)}:  {qty}x</b>\n"

            text += f"\n━━━━━━━━━━━━━━━━━━━━\n"
            text += f"<b>Итого: {total} позиций</b>"

            await update.message.reply_text("Заказ принят!")

            if group_id:
                try:
                    if len(text.encode('utf-8')) <= 4096:
                        await context.bot.send_message(chat_id=int(group_id), text=text, parse_mode="HTML")
                    else:
                        lines_all = text.split('\n')
                        chunk = ""
                        for line in lines_all:
                            test = chunk + line + "\n"
                            if len(test.encode('utf-8')) > 3900:
                                if chunk.strip():
                                    await context.bot.send_message(chat_id=int(group_id), text=chunk, parse_mode="HTML")
                                chunk = line + "\n"
                            else:
                                chunk = test
                        if chunk.strip():
                            await context.bot.send_message(chat_id=int(group_id), text=chunk, parse_mode="HTML")
                    logger.info("Order forwarded to group OK")
                except Exception as e:
                    logger.error(f"GROUP FORWARD FAILED: {e}")
                    await update.message.reply_text(f"Ошибка: {e}")

        elif action == "tasks":
            completed = data.get("completed", [])
            category = data.get("category", "")

            await update.message.reply_text("Задачи сохранены!")

            if group_id:
                try:
                    from html import escape as esc_html
                    text = f"<b>ЗАДАЧИ ВЫПОЛНЕНЫ</b>\n"
                    text += f"━━━━━━━━━━━━━━━━━━━━\n"
                    text += f"<b>{esc_html(user.first_name)}</b>\n"
                    text += f"<b>{now.strftime('%d.%m.%Y  %H:%M')}</b>\n"
                    text += f"<i>{esc_html(category)}</i>\n"
                    text += f"━━━━━━━━━━━━━━━━━━━━\n\n"
                    for item in completed:
                        text += f"  — {esc_html(item)}\n"
                    await context.bot.send_message(chat_id=int(group_id), text=text, parse_mode="HTML")
                except Exception as e:
                    logger.error(f"GROUP FORWARD FAILED: {e}")

        elif action == "shift_start":
            # Vardiya başlat (geliş zamanını kaydet)
            db = get_db()
            upsert_user(db, user.id, user.first_name, user.username, update.effective_chat.id)
            existing = get_active_shift(db, user.id)
            if existing:
                start_dt = datetime.fromisoformat(existing["start_time"])
                await update.message.reply_text(
                    f"ℹ️ У вас уже есть открытая смена с {start_dt.strftime('%H:%M')}.")
                return
            # Opsiyonel: barista geçmiş bir saat girdiyse (telefon kapanmıştı vs.)
            custom_start = data.get("start_time") or data.get("custom_start")
            sh = start_shift(db, user.id, custom_start=custom_start)
            start_dt = datetime.fromisoformat(sh["start_time"])
            note_back = ""
            if custom_start:
                note_back = f"\n_(время указано вручную)_"
            await update.message.reply_text(
                f"🟢 *Смена началась!*\n\n"
                f"📅 {start_dt.strftime('%d.%m.%Y')}\n"
                f"⏰ Пришли в *{start_dt.strftime('%H:%M')}*{note_back}\n\n"
                f"Когда закончите — нажмите «Завершить смену» в приложении.",
                parse_mode="Markdown")
            # Klavye butonunu taze URL ile yenile (yoksa tekrar açınca eski state görünür)
            await refresh_webapp_keyboard(update, context, db, user,
                "🔄 Откройте приложение — теперь видна активная смена 👇")
            if group_id:
                try:
                    from html import escape as esc_html
                    gtext = (f"🟢 <b>{esc_html(user.first_name)}</b> начал(а) смену\n"
                             f"⏰ {start_dt.strftime('%d.%m.%Y %H:%M')}")
                    await context.bot.send_message(chat_id=int(group_id), text=gtext, parse_mode="HTML")
                except Exception as e:
                    logger.error(f"GROUP FORWARD FAILED: {e}")

        elif action == "shift_end":
            # Vardiyayı bitir (gidiş + bardak sayıları + bonus)
            db = get_db()
            upsert_user(db, user.id, user.first_name, user.username, update.effective_chat.id)
            active = get_active_shift(db, user.id)
            if not active:
                await update.message.reply_text("❌ Нет активной смены. Сначала нажмите «Начать смену».")
                return
            drinks = data.get("drinks", {}) or {}
            desserts = data.get("desserts", {}) or {}
            note = (data.get("note") or "").strip()
            custom_end = data.get("end_time") or data.get("custom_end")
            sh = end_shift(db, user.id, drinks, note, desserts=desserts, custom_end=custom_end)
            if not sh:
                await update.message.reply_text("❌ Не удалось закрыть смену.")
                return
            start_dt = datetime.fromisoformat(sh["start_time"])
            end_dt = datetime.fromisoformat(sh["end_time"])
            hours = sh["hours"] or 0
            hourly_pay = sh["hourly_pay"] or 0
            bonus = sh["bonus"] or 0
            dessert_bonus = sh["dessert_bonus"] or 0
            drinks_bonus = max(0, bonus - dessert_bonus)
            total = sh["total"] or 0
            cups = sum(int(v or 0) for v in drinks.values())
            sweets = sum(int(v or 0) for v in desserts.values())
            period = sh["period"]
            s = calc_summary(db, user.id, period)
            # DM cevabı: kişisel — saatlik dahil net
            text = (f"🔴 *Смена закрыта!*\n"
                    f"━━━━━━━━━━━━━━━━━━\n"
                    f"⏰ {start_dt.strftime('%H:%M')} → {end_dt.strftime('%H:%M')}  ({hours:g}h)\n"
                    f"🥤 Напитков: *{cups}* шт · 💰 {fmt_sum(drinks_bonus)} сум\n")
            if sweets:
                text += f"🍰 Десерты: *{sweets}* шт · 💰 {fmt_sum(dessert_bonus)} сум\n"
            text += (f"💵 Часы (12.000): {fmt_sum(hourly_pay)} _(в конце месяца)_\n"
                     f"💎 За смену: *{fmt_sum(total)}* сум\n"
                     f"━━━━━━━━━━━━━━━━━━\n"
                     f"📊 *Месяц {period}:*\n"
                     f"Часы: {s['hours']:g}h | Смен: {s['shifts_count']}\n"
                     f"💎 *НЕТТО: {fmt_sum(s['net'])} сум*")
            if note:
                text += f"\n📝 {note}"
            await update.message.reply_text(text, parse_mode="Markdown")
            # Klavye butonunu taze URL ile yenile (active=null, yeni vardiya başlatılabilsin)
            await refresh_webapp_keyboard(update, context, db, user,
                "🔄 Смена закрыта. Готово к следующей смене 👇")
            if group_id:
                try:
                    from html import escape as esc_html
                    # Grup mesajı: SAATLIK GIZLI (ay sonu hesabı). Sadece satış sayıları + satış bonusu.
                    sales_bonus = drinks_bonus + dessert_bonus
                    gtext = (f"🔴 <b>{esc_html(user.first_name)}</b> закрыл(а) смену\n"
                             f"━━━━━━━━━━━━━━━━━━━━\n"
                             f"⏰ {start_dt.strftime('%H:%M')} → {end_dt.strftime('%H:%M')}  ({hours:g}h)\n"
                             f"🥤 Напитки: <b>{cups}</b> шт")
                    if sweets:
                        gtext += f"\n🍰 Десерты: <b>{sweets}</b> шт"
                    gtext += f"\n💰 Продажи: <b>{fmt_sum(sales_bonus)} сум</b>"
                    if note:
                        gtext += f"\n📝 {esc_html(note)}"
                    await context.bot.send_message(chat_id=int(group_id), text=gtext, parse_mode="HTML")
                except Exception as e:
                    logger.error(f"GROUP FORWARD FAILED: {e}")
            # Sahiplere bildir (TAM detay — owner zarplata için görür)
            try:
                owners = db.execute("SELECT user_id FROM users WHERE role='owner' AND user_id != ?", (user.id,)).fetchall()
                for o in owners:
                    try:
                        otext = (f"📢 *{user.first_name}* закрыл(а) смену\n"
                                 f"⏰ {start_dt.strftime('%H:%M')} → {end_dt.strftime('%H:%M')} ({hours:g}h)\n"
                                 f"🥤 {cups} шт · 🍰 {sweets} шт\n"
                                 f"💰 Продажи: {fmt_sum(drinks_bonus + dessert_bonus)}\n"
                                 f"💵 Часы: {fmt_sum(hourly_pay)}\n"
                                 f"💎 Итого: *{fmt_sum(total)} сум*")
                        await context.bot.send_message(o["user_id"], otext, parse_mode="Markdown")
                    except Exception:
                        pass
            except Exception as e:
                logger.warning(f"Notify owners failed: {e}")

        elif action == "shift":
            # Geriye dönük manuel kayıt (eski akış)
            db = get_db()
            upsert_user(db, user.id, user.first_name, user.username, update.effective_chat.id)
            hours = float(data.get("hours", 0) or 0)
            drinks = data.get("drinks", {}) or {}
            note = data.get("note", "")
            bonus = calc_bonus(drinks)
            hourly_pay = int(hours * HOURLY_RATE)
            total = hourly_pay + bonus
            period = current_period()
            db.execute(
                "INSERT INTO shifts (user_id, hours, drinks, bonus, hourly_pay, total, date, period, created_at, start_time, end_time, note) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (user.id, hours, json.dumps(drinks, ensure_ascii=False),
                 bonus, hourly_pay, total,
                 now.strftime("%Y-%m-%d"), period, now.isoformat(),
                 None, now.isoformat(), note))
            db.commit()
            s = calc_summary(db, user.id, period)
            text = (f"✅ *Смена записана!*\n"
                    f"━━━━━━━━━━━━━━━━━━\n"
                    f"⏱️ {hours:g}h × 12.000 = *{fmt_sum(hourly_pay)}* сум\n"
                    f"🥤 Бонус: *{fmt_sum(bonus)}* сум\n"
                    f"💵 За смену: *{fmt_sum(total)}* сум\n"
                    f"━━━━━━━━━━━━━━━━━━\n"
                    f"📊 *Месяц {period}:*\n"
                    f"Часы: {s['hours']:g}h | Смен: {s['shifts_count']}\n"
                    f"Брутто: {fmt_sum(s['gross'])} | Штраф: -{fmt_sum(s['fines'])}\n"
                    f"💎 *НЕТТО: {fmt_sum(s['net'])} сум*")
            await update.message.reply_text(text, parse_mode="Markdown")

            if group_id:
                try:
                    from html import escape as esc_html
                    gtext = (f"<b>СМЕНА — {esc_html(user.first_name)}</b>\n"
                             f"━━━━━━━━━━━━━━━━━━━━\n"
                             f"📅 {now.strftime('%d.%m.%Y %H:%M')}\n"
                             f"⏱️ {hours:g}h | 🥤 {sum(int(v or 0) for v in drinks.values())} шт\n"
                             f"💵 За смену: <b>{fmt_sum(total)} сум</b>")
                    if note:
                        gtext += f"\n📝 {esc_html(note)}"
                    await context.bot.send_message(chat_id=int(group_id), text=gtext, parse_mode="HTML")
                except Exception as e:
                    logger.error(f"GROUP FORWARD FAILED: {e}")

        elif action == "fine":
            # Tek veya çoklu (denetim — split) ceza
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец может выписывать штрафы.")
                return
            # Sebep ZORUNLU
            reason = (data.get("reason") or "").strip()
            if not reason:
                await update.message.reply_text("❌ Причина обязательна.")
                return
            amount = int(data.get("amount", 0) or 0)
            ftype = data.get("type", "manual")
            # targets liste olabilir (split) veya tek hedef olabilir
            targets_raw = data.get("targets") or ([data.get("target")] if data.get("target") else [])
            targets = [int(t) for t in targets_raw if t]
            if not targets or amount <= 0:
                await update.message.reply_text("❌ Неверные данные штрафа.")
                return
            split = bool(data.get("split"))
            chef_share = bool(data.get("chef_share"))  # Şef %50 öder
            # Şef paylaşırsa toplam tutarın yarısı baristalara dağılır
            barista_pool = amount // 2 if chef_share else amount
            chef_amount = amount - barista_pool if chef_share else 0
            per_target = (barista_pool // len(targets)) if split and len(targets) > 1 else barista_pool
            period = current_period()
            sent_to = []
            for tid in targets:
                trow = db.execute("SELECT * FROM users WHERE user_id=?", (tid,)).fetchone()
                if not trow:
                    continue
                final_reason = reason + (f" (раздел.: {len(targets)})" if split and len(targets) > 1 else "")
                db.execute(
                    "INSERT INTO fines (user_id, amount, reason, type, period, added_by, added_by_name, created_at) "
                    "VALUES (?,?,?,?,?,?,?,?)",
                    (tid, per_target, final_reason, ftype, period,
                     user.id, user.first_name, now.isoformat()))
                sent_to.append(trow)
                # Log
                log_action(db, "fine_add", user.id, user.first_name,
                           tid, display_name_for(db, tid),
                           {"amount": per_target, "reason": final_reason, "type": ftype, "split": split})
                # Bildir
                try:
                    await context.bot.send_message(
                        tid,
                        f"⚠️ *Вам начислен штраф*\n\n"
                        f"Сумма: *-{fmt_sum(per_target)}* сум\n"
                        f"Причина: {final_reason}\n"
                        f"От: {user.first_name}\n\n"
                        f"Баланс: /zarplata",
                        parse_mode="Markdown")
                except Exception as e:
                    logger.warning(f"Notify fine failed: {e}")
            # Şef payı — owner kendi üstüne %50 ceza yazar (iyilik kuralı)
            if chef_share and chef_amount > 0:
                chef_reason = reason + " (50% шефа)"
                db.execute(
                    "INSERT INTO fines (user_id, amount, reason, type, period, added_by, added_by_name, created_at) "
                    "VALUES (?,?,?,?,?,?,?,?)",
                    (user.id, chef_amount, chef_reason, ftype, period,
                     user.id, user.first_name, now.isoformat()))
                log_action(db, "fine_add", user.id, user.first_name,
                           user.id, user.first_name,
                           {"amount": chef_amount, "reason": chef_reason, "type": ftype, "chef_share": True})
            db.commit()
            tail = (f"\n🍴 Шеф взял на себя: -{fmt_sum(chef_amount)} сум" if chef_share and chef_amount > 0 else "")
            if split and len(sent_to) > 1:
                await update.message.reply_text(
                    f"⚠️ Штраф разделён на {len(sent_to)} человек\n"
                    f"По {fmt_sum(per_target)} сум каждому\n"
                    f"Причина: {reason}" + tail)
            elif sent_to:
                await update.message.reply_text(
                    f"⚠️ Штраф добавлен\n\n"
                    f"Кому: {display_name_for(db, sent_to[0]['user_id'])}\n"
                    f"Сумма: -{fmt_sum(per_target)} сум\n"
                    f"Причина: {reason}" + tail)

        elif action == "pay":
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец.")
                return
            target_id = int(data.get("target", 0) or 0)
            period = data.get("period") or current_period()
            target_row = db.execute("SELECT * FROM users WHERE user_id=?", (target_id,)).fetchone()
            if not target_row:
                await update.message.reply_text("❌ Бариста не найден.")
                return
            s = calc_summary(db, target_id, period)
            if s["net"] <= 0:
                await update.message.reply_text(f"❌ Нет средств: {fmt_sum(s['net'])} сум")
                return
            db.execute(
                "INSERT INTO payments (user_id, amount, period, paid_by, paid_by_name, paid_at) "
                "VALUES (?,?,?,?,?,?)",
                (target_id, s["net"], period, user.id, user.first_name, now.isoformat()))
            db.commit()
            log_action(db, "pay", user.id, user.first_name, target_id,
                       display_name_for(db, target_id),
                       {"amount": s["net"], "period": period})
            await update.message.reply_text(
                f"✅ Выплата записана\n\n"
                f"Кому: {display_name_for(db, target_id)}\n"
                f"Период: {period}\n"
                f"Сумма: {fmt_sum(s['net'])} сум")
            try:
                await context.bot.send_message(
                    target_id,
                    f"💵 *Зарплата выплачена!*\n\n"
                    f"Период: {period}\n"
                    f"Сумма: *{fmt_sum(s['net'])}* сум\n"
                    f"От: {user.first_name}",
                    parse_mode="Markdown")
            except Exception as e:
                logger.warning(f"Notify pay failed: {e}")

        elif action == "grant":
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец.")
                return
            target_id = int(data.get("target", 0) or 0)
            new_role = data.get("role", "barista")
            if new_role not in ("owner", "barista"):
                await update.message.reply_text("❌ Неверная роль.")
                return
            target_row = db.execute("SELECT * FROM users WHERE user_id=?", (target_id,)).fetchone()
            if not target_row:
                await update.message.reply_text("❌ Пользователь не найден.")
                return
            if new_role == "barista" and target_id == user.id:
                owner_count = db.execute("SELECT COUNT(*) as c FROM users WHERE role='owner'").fetchone()["c"]
                if owner_count <= 1:
                    await update.message.reply_text("❌ Вы единственный владелец.")
                    return
            db.execute("UPDATE users SET role=? WHERE user_id=?", (new_role, target_id))
            db.commit()
            await sync_user_ui(context.bot, db, target_id)
            log_action(db, "role_change", user.id, user.first_name, target_id,
                       display_name_for(db, target_id), {"new_role": new_role})
            await update.message.reply_text(f"✅ {display_name_for(db, target_id)}: роль → {new_role}")
            try:
                if new_role == "owner":
                    msg = f"👑 Вам выдали роль *владельца*!\nОт: {user.first_name}"
                else:
                    msg = f"ℹ️ Ваша роль изменена на *бариста*.\nОт: {user.first_name}"
                await context.bot.send_message(target_id, msg, parse_mode="Markdown")
            except Exception as e:
                logger.warning(f"Notify role failed: {e}")

        # ─── Yeni: Bahşiş dağıtımı ───
        elif action == "tip_distribute":
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец может раздавать чаевые.")
                return
            # distributions: [{target:uid, amount:int}, ...]
            distributions = data.get("distributions") or []
            note = (data.get("note") or "").strip()
            if not distributions:
                await update.message.reply_text("❌ Список получателей пуст.")
                return
            period = current_period()
            total_dist = 0
            recipients = []
            for d in distributions:
                tid = int(d.get("target", 0) or 0)
                amt = int(d.get("amount", 0) or 0)
                if tid <= 0 or amt <= 0:
                    continue
                trow = db.execute("SELECT * FROM users WHERE user_id=?", (tid,)).fetchone()
                if not trow:
                    continue
                db.execute(
                    "INSERT INTO tips (user_id, amount, period, note, added_by, added_by_name, created_at) "
                    "VALUES (?,?,?,?,?,?,?)",
                    (tid, amt, period, note, user.id, user.first_name, now.isoformat()))
                total_dist += amt
                recipients.append((tid, amt))
                log_action(db, "tip_add", user.id, user.first_name, tid,
                           display_name_for(db, tid),
                           {"amount": amt, "period": period, "note": note})
                try:
                    await context.bot.send_message(
                        tid,
                        f"💝 *Вам начислены чаевые!*\n\n"
                        f"Сумма: *+{fmt_sum(amt)}* сум\n" +
                        (f"📝 {note}\n" if note else "") +
                        f"От: {user.first_name}\n\nБаланс: /zarplata",
                        parse_mode="Markdown")
                except Exception as e:
                    logger.warning(f"Notify tip failed: {e}")
            db.commit()
            await update.message.reply_text(
                f"💝 Чаевые распределены\n\n"
                f"Всего: {fmt_sum(total_dist)} сум · Получателей: {len(recipients)}" +
                (f"\n📝 {note}" if note else ""))

        # ─── Yeni: Bardak fiyatı güncelle ───
        elif action == "price_update":
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец.")
                return
            drink_id = (data.get("drink_id") or "").strip()
            try:
                amount = int(data.get("amount", 0) or 0)
            except Exception:
                amount = 0
            if not drink_id or amount < 0:
                await update.message.reply_text("❌ Неверные данные цены.")
                return
            old = db.execute("SELECT amount FROM prices WHERE drink_id=?", (drink_id,)).fetchone()
            old_amt = (old["amount"] if old else BONUS_RATES.get(drink_id, 0))
            db.execute(
                "INSERT INTO prices (drink_id, amount, updated_by, updated_by_name, updated_at) "
                "VALUES (?,?,?,?,?) "
                "ON CONFLICT(drink_id) DO UPDATE SET amount=excluded.amount, "
                "updated_by=excluded.updated_by, updated_by_name=excluded.updated_by_name, "
                "updated_at=excluded.updated_at",
                (drink_id, amount, user.id, user.first_name, now.isoformat()))
            db.commit()
            log_action(db, "price_update", user.id, user.first_name, None, None,
                       {"drink_id": drink_id, "old": old_amt, "new": amount})
            await update.message.reply_text(
                f"💰 Цена обновлена\n\n"
                f"{drink_id}: {fmt_sum(old_amt)} → *{fmt_sum(amount)}* сум",
                parse_mode="Markdown")

        # ─── Tatlı kataloğu yönetimi (owner) ───
        elif action == "dessert_save":
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец.")
                return
            did = (data.get("id") or "").strip().lower()
            label = (data.get("label") or "").strip()
            icon = (data.get("icon") or "🍮").strip()[:4]
            try:
                price = max(0, int(data.get("price", 500) or 0))
            except Exception:
                price = 500
            try:
                sort_order = int(data.get("sort_order", 50) or 50)
            except Exception:
                sort_order = 50
            active = 1 if data.get("active", 1) else 0
            if not did or not label:
                await update.message.reply_text("❌ ID и название обязательны.")
                return
            # ID'de sadece harf/rakam/_
            import re
            if not re.match(r"^[a-z0-9_]+$", did):
                await update.message.reply_text("❌ ID: только латинские буквы, цифры, _ (например: tiramisu)")
                return
            now_iso = datetime.now(TZ).isoformat()
            db.execute(
                "INSERT INTO desserts_catalog (id,label,icon,price,sort_order,active,updated_by,updated_by_name,updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(id) DO UPDATE SET label=excluded.label, icon=excluded.icon, "
                "price=excluded.price, sort_order=excluded.sort_order, active=excluded.active, "
                "updated_by=excluded.updated_by, updated_by_name=excluded.updated_by_name, "
                "updated_at=excluded.updated_at",
                (did, label, icon, price, sort_order, active, user.id, user.first_name, now_iso))
            db.commit()
            log_action(db, "dessert_save", user.id, user.first_name, None, None,
                       {"id": did, "label": label, "price": price, "active": active})
            await update.message.reply_text(
                f"🍰 Десерт сохранён: *{icon} {label}* — {fmt_sum(price)} сум",
                parse_mode="Markdown")
            await refresh_webapp_keyboard(update, context, db, user,
                "🔄 Каталог десертов обновлён 👇")

        elif action == "dessert_delete":
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец.")
                return
            did = (data.get("id") or "").strip().lower()
            if not did:
                await update.message.reply_text("❌ ID обязателен.")
                return
            row = db.execute("SELECT label FROM desserts_catalog WHERE id=?", (did,)).fetchone()
            if not row:
                await update.message.reply_text("❌ Десерт не найден.")
                return
            # Tam silme — kataloğdan kaldırılır. Eski vardiyalar JSON içinde
            # snapshot tuttuğu için geçmiş veriler bozulmaz.
            db.execute("DELETE FROM desserts_catalog WHERE id=?", (did,))
            db.commit()
            log_action(db, "dessert_delete", user.id, user.first_name, None, None, {"id": did, "label": row["label"]})
            await update.message.reply_text(f"🗑 Десерт «{row['label']}» удалён из каталога.")
            await refresh_webapp_keyboard(update, context, db, user,
                "🔄 Каталог десертов обновлён 👇")

        # ─── Şifre yönetimi (owner-only) ───
        elif action == "set_password":
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец может менять пароли.")
                return
            target_id = int(data.get("target", 0) or 0)
            new_pwd = (data.get("password") or "").strip()
            if not target_id or not new_pwd:
                await update.message.reply_text("❌ Укажите бариста и новый пароль.")
                return
            target_row = db.execute("SELECT * FROM users WHERE user_id=?", (target_id,)).fetchone()
            if not target_row:
                await update.message.reply_text("❌ Пользователь не найден.")
                return
            # Yeni şifre atanınca eski authorized=0 olur — barista yeni şifreyle tekrar girmek zorunda
            db.execute("UPDATE users SET password=?, authorized=0 WHERE user_id=?",
                       (new_pwd, target_id))
            db.commit()
            log_action(db, "set_password", user.id, user.first_name, target_id,
                       target_row["display_name"] or target_row["name"], {})
            shown = display_name_for(db, target_id, fallback=target_row["name"])
            await update.message.reply_text(
                f"🔐 Пароль для *{md_safe(shown)}* установлен.\n\n"
                f"Передайте бариста: он(а) должен(а) написать боту:\n`/login {new_pwd}`",
                parse_mode="Markdown")

        elif action == "clear_password":
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец.")
                return
            target_id = int(data.get("target", 0) or 0)
            target_row = db.execute("SELECT * FROM users WHERE user_id=?", (target_id,)).fetchone()
            if not target_row:
                await update.message.reply_text("❌ Пользователь не найден.")
                return
            # Şifre silinir + erişim kapatılır
            db.execute("UPDATE users SET password=NULL, authorized=0 WHERE user_id=?",
                       (target_id,))
            db.commit()
            log_action(db, "clear_password", user.id, user.first_name, target_id,
                       target_row["display_name"] or target_row["name"], {})
            shown = display_name_for(db, target_id, fallback=target_row["name"])
            await update.message.reply_text(
                f"🗑 Пароль для *{md_safe(shown)}* удалён. Доступ закрыт.",
                parse_mode="Markdown")

        # ─── Kullanıcı arşivleme (owner-only) ───
        # Arşivlenince: kullanıcı bottan giriş yapamaz, aktif listede gözükmez
        # ama TÜM geçmişi (vardiya/ceza/ödeme/bahşiş/loglar) korunur.
        elif action == "archive_user":
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец.")
                return
            target_id = int(data.get("target", 0) or 0)
            if not target_id or target_id == user.id:
                await update.message.reply_text("❌ Неверный пользователь.")
                return
            target_row = db.execute("SELECT * FROM users WHERE user_id=?", (target_id,)).fetchone()
            if not target_row:
                await update.message.reply_text("❌ Пользователь не найден.")
                return
            if (target_row["role"] or "") == "owner":
                await update.message.reply_text("❌ Нельзя архивировать владельца.")
                return
            shown = target_row["display_name"] or target_row["name"] or "?"
            now = datetime.now(TZ).isoformat()
            db.execute(
                "UPDATE users SET archived=1, archived_at=?, authorized=0 WHERE user_id=?",
                (now, target_id))
            db.commit()
            log_action(db, "archive_user", user.id, user.first_name, target_id, shown, {})
            await update.message.reply_text(
                f"📦 *{md_safe(shown)}* перенесён в архив.\n"
                f"Доступ закрыт, но вся история сохранена.",
                parse_mode="Markdown")

        elif action == "unarchive_user":
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец.")
                return
            target_id = int(data.get("target", 0) or 0)
            target_row = db.execute("SELECT * FROM users WHERE user_id=?", (target_id,)).fetchone()
            if not target_row:
                await update.message.reply_text("❌ Пользователь не найден.")
                return
            shown = target_row["display_name"] or target_row["name"] or "?"
            db.execute("UPDATE users SET archived=0, archived_at=NULL WHERE user_id=?", (target_id,))
            db.commit()
            log_action(db, "unarchive_user", user.id, user.first_name, target_id, shown, {})
            await update.message.reply_text(
                f"♻️ *{md_safe(shown)}* возвращён из архива.\n"
                f"Не забудьте задать пароль, если нужен доступ.",
                parse_mode="Markdown")

        # ─── Kullanıcı tamamen silme (owner-only, GERİ DÖNÜŞSÜZ) ───
        # İki mod:
        #   1) Veri yoksa → direkt sil
        #   2) Veri varsa → confirm_with_data=1 flag'i şart
        elif action == "delete_user":
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец может удалять пользователей.")
                return
            target_id = int(data.get("target", 0) or 0)
            confirm_data = bool(data.get("confirm_with_data"))
            if not target_id:
                await update.message.reply_text("❌ Укажите пользователя.")
                return
            if target_id == user.id:
                await update.message.reply_text("❌ Нельзя удалить самого себя.")
                return
            target_row = db.execute("SELECT * FROM users WHERE user_id=?", (target_id,)).fetchone()
            if not target_row:
                await update.message.reply_text("❌ Пользователь не найден.")
                return
            if (target_row["role"] or "") == "owner":
                await update.message.reply_text("❌ Нельзя удалить владельца.")
                return
            shown = target_row["display_name"] or target_row["name"] or "?"
            # İlişkili veri sayımı
            sc = db.execute("SELECT COUNT(*) AS c FROM shifts WHERE user_id=?", (target_id,)).fetchone()["c"]
            fc = db.execute("SELECT COUNT(*) AS c FROM fines WHERE user_id=?", (target_id,)).fetchone()["c"]
            pc = db.execute("SELECT COUNT(*) AS c FROM payments WHERE user_id=?", (target_id,)).fetchone()["c"]
            try:
                tc = db.execute("SELECT COUNT(*) AS c FROM tips WHERE user_id=?", (target_id,)).fetchone()["c"]
            except Exception:
                tc = 0
            has_data = (sc + fc + pc + tc) > 0
            if has_data and not confirm_data:
                await update.message.reply_text(
                    f"⚠️ У *{md_safe(shown)}* есть данные:\n"
                    f"• Смен: {sc}\n• Штрафов: {fc}\n• Выплат: {pc}\n• Чаевых: {tc}\n\n"
                    f"Подтвердите удаление в приложении ещё раз.",
                    parse_mode="Markdown")
                return
            # Tüm ilişkili verileri sil
            db.execute("DELETE FROM shifts WHERE user_id=?", (target_id,))
            db.execute("DELETE FROM fines WHERE user_id=?", (target_id,))
            db.execute("DELETE FROM payments WHERE user_id=?", (target_id,))
            try:
                db.execute("DELETE FROM tips WHERE user_id=?", (target_id,))
            except Exception:
                pass
            db.execute("DELETE FROM users WHERE user_id=?", (target_id,))
            db.commit()
            log_action(db, "delete_user", user.id, user.first_name, target_id,
                       shown, {"shifts": sc, "fines": fc, "payments": pc, "tips": tc})
            await update.message.reply_text(
                f"🗑 Пользователь *{shown}* полностью удалён.\n"
                f"_Удалено: смен {sc}, штрафов {fc}, выплат {pc}, чаевых {tc}._",
                parse_mode="Markdown")

        # ─── Yeni: Display name ata ───
        elif action == "rename_user":
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец.")
                return
            target_id = int(data.get("target", 0) or 0)
            new_name = (data.get("display_name") or data.get("name") or "").strip()
            target_row = db.execute("SELECT * FROM users WHERE user_id=?", (target_id,)).fetchone()
            if not target_row:
                await update.message.reply_text("❌ Пользователь не найден.")
                return
            old_name = display_name_for(db, target_id)
            db.execute(
                "UPDATE users SET display_name=? WHERE user_id=?",
                (new_name or None, target_id))
            db.commit()
            log_action(db, "rename", user.id, user.first_name, target_id, new_name or target_row["name"],
                       {"old": old_name, "new": new_name})
            shown = new_name or target_row["name"] or "?"
            uname = target_row["username"] or str(target_id)
            await update.message.reply_text(
                f"✏️ Имя обновлено\n\n"
                f"@{md_safe(uname)} → *{md_safe(shown)}*",
                parse_mode="Markdown")

        # ─── Owner: Resmi sınav daveti (uzaktan) ───
        elif action == "exam_invite":
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец может назначать зачёт.")
                return
            target_id = int(data.get("target", 0) or 0)
            if not target_id:
                await update.message.reply_text("❌ Укажите бариста.")
                return
            target_row = db.execute("SELECT * FROM users WHERE user_id=?", (target_id,)).fetchone()
            if not target_row:
                await update.message.reply_text("❌ Бариста не найден.")
                return
            # Aktif daveti varsa engelle
            existing = db.execute(
                "SELECT id FROM rt_exam_invites WHERE barista_id=? AND status IN ('pending','active')",
                (target_id,)).fetchone()
            if existing:
                await update.message.reply_text("⚠️ У этого бариста уже есть активная сессия.")
                return
            now = datetime.now(TZ).isoformat()
            cur = db.execute(
                "INSERT INTO rt_exam_invites (barista_id, owner_id, owner_name, status, created_at) "
                "VALUES (?,?,?,'pending',?)",
                (target_id, user.id, user.first_name, now))
            db.commit()
            invite_id = cur.lastrowid
            log_action(db, "exam_invite", user.id, user.first_name, target_id,
                       display_name_for(db, target_id), {"invite_id": invite_id})
            # Baristaya bildirim + taze web_app butonu (kolay erişim)
            try:
                shown = display_name_for(db, target_id, fallback=target_row["name"])
                await context.bot.send_message(
                    target_id,
                    f"🎓 *ОФИЦИАЛЬНЫЙ ЗАЧЁТ*\n\n"
                    f"От: *{md_safe(user.first_name)}*\n\n"
                    f"⚠️ Перед началом убедитесь:\n"
                    f"🔋 Заряд телефона ≥ 50%\n"
                    f"📷 Камера работает\n"
                    f"🟢 Вы в смене\n\n"
                    f"После начала экран нельзя закрыть до окончания.\n\n"
                    f"Нажмите *☕ Открыть Caffelito* ниже, и зачёт запустится автоматически.",
                    parse_mode="Markdown")
                # Taze webapp butonu — barista tek tuşla girsin
                if WEBAPP_URL:
                    fresh_url = build_webapp_url(WEBAPP_URL, target_id, target_row["name"] or "Бариста", db)
                    kb = ReplyKeyboardMarkup(
                        [[KeyboardButton("☕ Открыть Caffelito", web_app=WebAppInfo(url=fresh_url))]],
                        resize_keyboard=True)
                    await context.bot.send_message(target_id, "👇 Откройте, чтобы начать зачёт", reply_markup=kb)
            except Exception as e:
                logger.warning(f"exam invite notify failed: {e}")
                await update.message.reply_text("⚠️ Не удалось отправить уведомление баристе.")
                return
            await update.message.reply_text(
                f"✅ Зачёт назначен\n\nКому: {display_name_for(db, target_id)}\n"
                f"Уведомление отправлено. Ожидание начала…")

        # ─── Owner: davet iptali ───
        elif action == "exam_invite_cancel":
            db = get_db()
            if get_role(db, user.id) != "owner":
                return
            invite_id = int(data.get("invite_id", 0) or 0)
            if not invite_id:
                return
            db.execute("UPDATE rt_exam_invites SET status='cancelled' WHERE id=? AND status IN ('pending','active')",
                       (invite_id,))
            db.commit()
            await update.message.reply_text("🚫 Зачёт отменён.")

        # ─── Barista: zaten resmi sınav cevabı (her sorudan sonra opsiyonel canlı log) ───
        elif action == "exam_progress":
            db = get_db()
            invite_id = int(data.get("invite_id", 0) or 0)
            inv = db.execute("SELECT * FROM rt_exam_invites WHERE id=? AND barista_id=?",
                             (invite_id, user.id)).fetchone()
            if not inv:
                return
            db.execute("UPDATE rt_exam_invites SET status='active' WHERE id=?", (invite_id,))
            db.commit()
            # Owner'a canlı bildirim (opsiyonel — Phase 2'de daha detaylı)
            try:
                shown = display_name_for(db, user.id)
                idx = int(data.get("idx", 0))
                total = int(data.get("total", 0))
                kind = data.get("kind", "")
                ok = data.get("ok")
                emoji = "✅" if ok else ("📷" if kind == "photo" else "❌")
                await context.bot.send_message(
                    inv["owner_id"],
                    f"{emoji} {md_safe(shown)} · {idx}/{total} · {kind}",
                    parse_mode="Markdown")
            except Exception:
                pass

        # ─── Barista: resmi sınavı bitirir ───
        elif action == "exam_finish":
            db = get_db()
            invite_id = int(data.get("invite_id", 0) or 0)
            correct = int(data.get("correct", 0) or 0)
            total = int(data.get("total", 0) or 0)
            score = int(data.get("score", 0) or 0)
            passed = 1 if data.get("passed") else 0
            now = datetime.now(TZ).isoformat()
            inv = db.execute("SELECT * FROM rt_exam_invites WHERE id=? AND barista_id=?",
                             (invite_id, user.id)).fetchone()
            if not inv:
                # Davet yoksa basit kayıt
                db.execute("INSERT INTO rt_exams (user_id, correct, total, score, passed, taken_at) "
                           "VALUES (?,?,?,?,?,?)",
                           (user.id, correct, total, score, passed, now))
                db.commit()
                return
            db.execute("UPDATE rt_exam_invites SET status='done', score=?, correct=?, total=?, finished_at=? WHERE id=?",
                       (score, correct, total, now, invite_id))
            db.execute("INSERT INTO rt_exams (user_id, correct, total, score, passed, taken_at) "
                       "VALUES (?,?,?,?,?,?)",
                       (user.id, correct, total, score, passed, now))
            db.commit()
            log_action(db, "exam_finish", user.id, user.first_name, user.id, user.first_name,
                       {"score": score, "passed": passed, "invite_id": invite_id})
            # Owner'a sonuç bildirimi
            try:
                shown = display_name_for(db, user.id, fallback=user.first_name)
                msg = (
                    f"🎓 *Зачёт завершён*\n\n"
                    f"Бариста: *{md_safe(shown)}*\n"
                    f"Результат: *{score}%* ({correct}/{total})\n"
                    f"Статус: {'🏆 Сдан' if passed else '❌ Не сдан'}"
                )
                await context.bot.send_message(inv["owner_id"], msg, parse_mode="Markdown")
            except Exception as e:
                logger.warning(f"exam finish notify failed: {e}")
            # Baristaya tebrik
            try:
                if passed:
                    await update.message.reply_text(
                        f"🏆 *Зачёт сдан!* {score}%\n\nМолодец!", parse_mode="Markdown")
                else:
                    await update.message.reply_text(f"💪 Не сдан · {score}%\n\nПродолжайте тренироваться.")
            except Exception:
                pass

        # ─── Resmi Sınav (Зачёт) — sertifika kaydı + owner bildirimi ───
        elif action == "rt_exam":
            db = get_db()
            correct = int(data.get("correct", 0) or 0)
            total = int(data.get("total", 0) or 0)
            score = int(data.get("score", 0) or 0)
            passed = 1 if data.get("passed") else 0
            now = datetime.now(TZ).isoformat()
            db.execute("INSERT INTO rt_exams (user_id, correct, total, score, passed, taken_at) "
                       "VALUES (?,?,?,?,?,?)",
                       (user.id, correct, total, score, passed, now))
            db.commit()
            log_action(db, "exam_taken", user.id, user.first_name, user.id, user.first_name,
                       {"score": score, "passed": passed})
            # Owner'lara bildirim — kim, ne zaman, sonuç
            try:
                shown = display_name_for(db, user.id, fallback=user.first_name)
                owners = db.execute("SELECT user_id FROM users WHERE role='owner' AND user_id != ?", (user.id,)).fetchall()
                msg_owner = (
                    f"🎓 *Зачёт пройден!*\n\n"
                    f"Бариста: *{md_safe(shown)}*\n"
                    f"Результат: *{score}%* ({correct}/{total})\n"
                    f"Статус: {'🏆 Сдан' if passed else '❌ Не сдан'}"
                )
                for o in owners:
                    try:
                        await context.bot.send_message(o["user_id"], msg_owner, parse_mode="Markdown")
                    except Exception:
                        pass
            except Exception as e:
                logger.warning(f"exam notify owners failed: {e}")
            # Baristaya tebrik/teselli mesajı
            try:
                if passed:
                    await update.message.reply_text(
                        f"🏆 *Зачёт сдан!*\n\n"
                        f"Результат: *{score}%* ({correct}/{total})\n"
                        f"Молодец — рецептура освоена!",
                        parse_mode="Markdown")
                else:
                    await update.message.reply_text(
                        f"💪 Зачёт не пройден\n\n"
                        f"Результат: {score}% ({correct}/{total})\n"
                        f"Нужно 90%. Тренируйтесь и приходите завтра.")
            except Exception:
                pass

        # ─── Recipe Trainer — session bitince progress kaydı ───
        elif action == "rt_session":
            db = get_db()
            lvl = int(data.get("level", 1) or 1)
            correct = int(data.get("correct", 0) or 0)
            total = int(data.get("total", 0) or 0)
            xp = int(data.get("xp", 0) or 0)
            max_streak = int(data.get("maxStreak", 0) or 0)
            passed = 1 if data.get("passed") else 0
            now = datetime.now(TZ).isoformat()
            db.execute("""INSERT INTO rt_sessions
                (user_id, level, correct, total, xp_earned, max_streak, passed, played_at)
                VALUES (?,?,?,?,?,?,?,?)""",
                (user.id, lvl, correct, total, xp, max_streak, passed, now))
            row = db.execute("SELECT * FROM rt_progress WHERE user_id=?", (user.id,)).fetchone()
            if row:
                new_level = row["level"]
                new_max = row["max_level"]
                if passed and lvl >= row["level"]:
                    new_level = min(5, lvl + 1)
                    new_max = max(row["max_level"], new_level)
                db.execute("""UPDATE rt_progress SET
                    level=?, max_level=?, xp=xp+?, best_streak=MAX(best_streak,?),
                    total_sessions=total_sessions+1, total_correct=total_correct+?,
                    total_questions=total_questions+?, last_played_at=?
                    WHERE user_id=?""",
                    (new_level, new_max, xp, max_streak, correct, total, now, user.id))
            else:
                new_level = min(5, lvl + 1) if passed else lvl
                new_max = max(1, new_level if passed else lvl)
                db.execute("""INSERT INTO rt_progress
                    (user_id, level, max_level, xp, best_streak, total_sessions,
                     total_correct, total_questions, last_played_at)
                    VALUES (?,?,?,?,?,1,?,?,?)""",
                    (user.id, new_level, new_max, xp, max_streak, correct, total, now))
            db.commit()

    except Exception as e:
        logger.error(f"WEBAPP DATA ERROR: {e}")
        try:
            await update.message.reply_text(f"❌ Ошибка: {e}")
        except:
            pass
    # Her aksiyondan sonra otomatik /start — karşılama mesajı + taze klavye butonu gönderir.
    # (Telegram bot kullanıcı yerine /start yazamaz; aynı fonksiyonu kendisi çağırır.)
    if update.effective_chat.type == "private":
        try:
            await start(update, context)
        except Exception as _e:
            logger.warning(f"auto /start failed: {_e}")


async def cmd_setgroup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Grubu kaydet — grupta /setgroup yaz"""
    chat = update.effective_chat
    if chat.type in ['group', 'supergroup']:
        context.bot_data["group_id"] = str(chat.id)
        await update.message.reply_text(
            f"✅ Группа привязана!\n\n"
            f"ID: `{chat.id}`\n\n"
            f"Теперь заказы и задачи из Mini App будут приходить сюда.\n\n"
            f"💡 Для Railway: добавьте переменную\n"
            f"`GROUP_CHAT_ID = {chat.id}`",
            parse_mode="Markdown")
    else:
        await update.message.reply_text(
            "❌ Эту команду нужно использовать в группе, не в личном чате.")


async def cmd_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Grupta veya özelde webapp butonu göster"""
    webapp_url = WEBAPP_URL
    if not webapp_url:
        await update.message.reply_text("❌ WEBAPP_URL не настроен.")
        return

    db = get_db()
    user = update.effective_user
    upsert_user(db, user.id, user.first_name, user.username, update.effective_chat.id)

    # 🔒 Auth check
    if not await require_auth(update, context):
        return

    # 👑 İlk yetkili kullanıcı otomatik owner olur
    if not has_owner(db):
        db.execute("UPDATE users SET role='owner' WHERE user_id=?", (user.id,))
        db.commit()

    chat_type = update.effective_chat.type

    # Grupta web_app çalışmaz — inline buton ile DM'ye yönlendir
    if chat_type != "private":
        bot_user = await context.bot.get_me()
        deep = f"https://t.me/{bot_user.username}?start=menu"
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("☕ Открыть Caffelito (в личке)", url=deep)]])
        await update.message.reply_text(
            "☕ *CAFFELITO*\n\nПриложение открывается только в личных сообщениях бота.\nНажмите кнопку 👇",
            reply_markup=kb, parse_mode="Markdown")
        return

    url = build_webapp_url(webapp_url, user.id, user.first_name, db)
    role = get_role(db, user.id)

    # Kalıcı buton — klavyenin üstünde her zaman görünür
    reply_kb = ReplyKeyboardMarkup(
        [[KeyboardButton("☕ Открыть Caffelito", web_app=WebAppInfo(url=url))]],
        resize_keyboard=True
    )
    role_line = "👑 *Владелец · Бухгалтерия*" if role == "owner" else "👤 *Бариста*"
    hint = ""
    if role != "owner":
        hint = "\n\n_Чтобы стать владельцем — /setowner (если ещё не назначен)._"
    await update.message.reply_text(
        f"☕ *CAFFELITO*\n\n"
        f"{role_line}\n\n"
        f"Кнопка приложения обновлена 👇\n"
        f"Нажмите «☕ Открыть Caffelito» внизу экрана.{hint}",
        reply_markup=reply_kb,
        parse_mode="Markdown")


async def cmd_app(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Inline buton ile mini app aç — gruba sabitlenebilir.
    ÖNEMLİ: tg.sendData() sadece DM'deki KeyboardButton'dan çalışır.
    Bu yüzden grupta button → DM'ye deep-link açar, DM'de otomatik keyboard button gelir."""
    # Grupta auth zorlamıyoruz (zaten DM'ye yönlendiriyor); DM'deyse auth iste
    if update.effective_chat.type == "private":
        if not await require_auth(update, context):
            return
    bot_user = await context.bot.get_me()
    dm_url = f"https://t.me/{bot_user.username}?start=app"  # DM'ye git, /start app tetiklenir
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("☕ Открыть Caffelito", url=dm_url)]
    ])
    msg = await update.message.reply_text(
        "☕ *CAFFELITO — Мини-приложение*\n\n"
        "Нажмите кнопку — откроется чат с ботом, где приложение запустится автоматически.\n"
        "Заказы, смены, зарплата и отчёты — всё внутри.\n\n"
        "_Закрепите это сообщение, чтобы быстро открывать приложение._",
        reply_markup=kb,
        parse_mode="Markdown")
    # Grupta otomatik sabitlemeyi dene
    if update.effective_chat.type in ("group", "supergroup"):
        try:
            await context.bot.pin_chat_message(
                chat_id=update.effective_chat.id,
                message_id=msg.message_id,
                disable_notification=True)
        except Exception as e:
            logger.info(f"Pin failed (need admin rights): {e}")


async def cmd_whoami(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Debug: текущий user ID + role"""
    db = get_db()
    user = update.effective_user
    upsert_user(db, user.id, user.first_name, user.username, update.effective_chat.id)
    role = get_role(db, user.id)
    dn = display_name_for(db, user.id, fallback=user.first_name)
    has_o = has_owner(db)
    owner_count = db.execute("SELECT COUNT(*) as c FROM users WHERE role='owner'").fetchone()["c"]
    await update.message.reply_text(
        f"ℹ️ *Кто я:*\n\n"
        f"ID: `{user.id}`\n"
        f"Имя в TG: {user.first_name}\n"
        f"Имя для UI: {dn}\n"
        f"@username: {user.username or '—'}\n"
        f"Роль: *{('👑 Владелец' if role=='owner' else '👤 Бариста')}*\n"
        f"Всего владельцев: {owner_count}\n\n"
        + ("" if has_o else "_Владелец ещё не назначен — /setowner._"),
        parse_mode="Markdown")


async def cmd_chatid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Debug: chat ID göster"""
    chat = update.effective_chat
    group_id = GROUP_CHAT_ID or context.bot_data.get("group_id", "не задан")
    await update.message.reply_text(
        f"ℹ️ *Информация:*\n\n"
        f"Этот чат ID: `{chat.id}`\n"
        f"Тип: {chat.type}\n"
        f"GROUP\\_CHAT\\_ID: `{group_id}`",
        parse_mode="Markdown")


async def cmd_test(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Test: gruba mesaj göndermeyi dene"""
    group_id = GROUP_CHAT_ID or context.bot_data.get("group_id")
    if not group_id:
        await update.message.reply_text(
            "❌ GROUP_CHAT_ID не задан.\n\n"
            "1. Добавьте бота в группу\n"
            "2. В группе напишите /setgroup\n"
            "3. Или добавьте GROUP_CHAT_ID в Railway Variables")
        return

    try:
        await context.bot.send_message(
            chat_id=int(group_id),
            text=f"✅ *Тест успешен!*\nБот может отправлять сообщения в эту группу.\n\n"
                 f"👤 Отправил: {update.effective_user.first_name}",
            parse_mode="Markdown")
        await update.message.reply_text("✅ Тестовое сообщение отправлено в группу!")
    except Exception as e:
        await update.message.reply_text(
            f"❌ Ошибка: `{e}`\n\n"
            f"Убедитесь что:\n"
            f"1. Бот добавлен в группу\n"
            f"2. Бот — администратор группы\n"
            f"3. GROUP_CHAT_ID правильный: `{group_id}`",
            parse_mode="Markdown")


# ═══════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════

BARISTA_COMMANDS = [
    BotCommand("start",    "🚀 Открыть приложение"),
    BotCommand("zarplata", "💰 Моя зарплата"),
    BotCommand("whoami",   "🪪 Кто я"),
]

OWNER_COMMANDS = [
    BotCommand("start",      "🚀 Открыть приложение / обновить"),
    BotCommand("menu",       "☕ Главное меню"),
    BotCommand("app",        "📱 Открыть мини-приложение"),
    BotCommand("zakaz",      "📋 Сделать заказ"),
    BotCommand("zadachi",    "✅ Задачи"),
    BotCommand("uborka",     "🧹 Уборка"),
    BotCommand("okk",        "🔍 ОКК контроль"),
    BotCommand("otchet",     "📊 Отчёт"),
    BotCommand("zarplata",   "💰 Моя зарплата"),
    BotCommand("baristalar", "👥 Список бариста"),
    BotCommand("ceza",       "⚠️ Штраф"),
    BotCommand("odendi",     "✅ Отметить оплату"),
    BotCommand("tip",        "💵 Чаевые"),
    BotCommand("logs",       "📜 Логи"),
    BotCommand("whoami",     "🪪 Кто я"),
    BotCommand("chatid",     "🆔 ID чата"),
    BotCommand("setowner",   "👑 Стать владельцем"),
    BotCommand("grantowner", "👑 Назначить владельца"),
    BotCommand("addbarista", "➕ Добавить бариста"),
    BotCommand("revoke",     "🚫 Отозвать доступ"),
    BotCommand("setname",    "✏️ Изменить имя"),
    BotCommand("setprice",   "💲 Изменить цену"),
    BotCommand("setgroup",   "📢 Привязать группу"),
]


async def sync_user_ui(bot, db, user_id: int):
    """Her kullanıcının rolüne göre komut listesi + menu butonu ayarla.
    Baristalar admin komutlarını hiç görmesin, menu butonu WebApp olsun."""
    try:
        row = db.execute("SELECT role FROM users WHERE user_id=?", (user_id,)).fetchone()
        is_owner_user = row and row["role"] == "owner"
        cmds = OWNER_COMMANDS if is_owner_user else BARISTA_COMMANDS
        await bot.set_my_commands(cmds, scope=BotCommandScopeChat(chat_id=user_id))
        # Menu butonu: owner için komut listesi, barista için direkt WebApp
        if is_owner_user:
            await bot.set_chat_menu_button(chat_id=user_id, menu_button=MenuButtonCommands())
        elif WEBAPP_URL:
            await bot.set_chat_menu_button(
                chat_id=user_id,
                menu_button=MenuButtonWebApp(text="☕ Caffelito", web_app=WebAppInfo(url=WEBAPP_URL))
            )
    except Exception as e:
        logger.warning(f"sync_user_ui failed for {user_id}: {e}")


async def setup_commands(app):
    """Default komut listesi — barista minimali. Owner'lar per-chat override alır."""
    await app.bot.set_my_commands(BARISTA_COMMANDS, scope=BotCommandScopeDefault())


def main():
    app = Application.builder().token(BOT_TOKEN).post_init(setup_commands).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("zakaz", cmd_order))
    app.add_handler(CommandHandler("zadachi", cmd_gorev))
    app.add_handler(CommandHandler("uborka", cmd_temizlik))
    app.add_handler(CommandHandler("okk", cmd_okk))
    app.add_handler(CommandHandler("otchet", cmd_report))
    app.add_handler(CommandHandler("setgroup", cmd_setgroup))
    app.add_handler(CommandHandler("menu", cmd_menu))
    app.add_handler(CommandHandler("app", cmd_app))
    app.add_handler(CommandHandler("login", cmd_login))
    app.add_handler(CommandHandler("whoami", cmd_whoami))
    app.add_handler(CommandHandler("chatid", cmd_chatid))
    app.add_handler(CommandHandler("test", cmd_test))
    # ─── Зарплата (Salary) команды ───
    app.add_handler(CommandHandler("setowner", cmd_setowner))
    app.add_handler(CommandHandler("zarplata", cmd_maosh))
    app.add_handler(CommandHandler("salary", cmd_maosh))
    app.add_handler(CommandHandler("maosh", cmd_maosh))
    app.add_handler(CommandHandler("baristalar", cmd_baristalar))
    app.add_handler(CommandHandler("baristas", cmd_baristalar))
    app.add_handler(CommandHandler("grantowner", cmd_grantowner))
    app.add_handler(CommandHandler("addbarista", cmd_addbarista))
    app.add_handler(CommandHandler("revoke", cmd_revoke))
    app.add_handler(CommandHandler("ceza", cmd_ceza))
    app.add_handler(CommandHandler("shtraf", cmd_ceza))
    app.add_handler(CommandHandler("fine", cmd_ceza))
    app.add_handler(CommandHandler("odendi", cmd_odendi))
    app.add_handler(CommandHandler("paid", cmd_odendi))
    app.add_handler(CommandHandler("setname", cmd_setname))
    app.add_handler(CommandHandler("setprice", cmd_setprice))
    app.add_handler(CommandHandler("tip", cmd_tip))
    app.add_handler(CommandHandler("logs", cmd_logs))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.StatusUpdate.WEB_APP_DATA, handle_webapp_data))
    print("☕ Caffelito Bot запущен!")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
