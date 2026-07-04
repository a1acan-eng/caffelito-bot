"""
CAFFELITO TELEGRAM BOT ☕
Заказ, Задачи, Уборка и ОКК контроль
"""

import json, os, logging, sqlite3, hmac, hashlib, asyncio, re, io, base64
from datetime import datetime, timezone, timedelta
from urllib.parse import parse_qsl
from aiohttp import web  # Yol B: Mini App'i + API'yi sunan HTTP sunucusu
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    WebAppInfo, BotCommand, BotCommandScopeChat, BotCommandScopeDefault,
    MenuButtonCommands, MenuButtonWebApp, MenuButtonDefault,
    KeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove,
)
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    ContextTypes, MessageHandler, ChatMemberHandler, filters
)

BOT_TOKEN = os.getenv("BOT_TOKEN", "BURAYA_BOT_TOKEN_YAZ")
WEBAPP_URL = os.getenv("WEBAPP_URL", "")
# Railway otomatik public domain'i tercih et: WEBAPP_URL boşsa VEYA eski github.io'yu
# gösteriyorsa, uygulama+backend AYNI origin'den (Railway) gelsin. github.io'da backend
# yok → /api/state oradan 405 döner ve rol "barista" sanılır (passcode bug'ı).
_RW_DOMAIN = os.getenv("RAILWAY_PUBLIC_DOMAIN", "")
if _RW_DOMAIN and (not WEBAPP_URL or "github.io" in WEBAPP_URL):
    WEBAPP_URL = f"https://{_RW_DOMAIN}/"
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
logger.info(f"WEBAPP_URL (etkin) = {WEBAPP_URL or '(bos)'}")

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
    # approved: owner kabul etti mi? Yeni /start yapanlar approved=0 (onay bekler, 'Все баристы'de görünmez).
    # İlk migration'da MEVCUT kullanıcıların hepsi onaylı sayılır (kaybolmasınlar).
    try:
        db.execute("ALTER TABLE users ADD COLUMN approved INTEGER DEFAULT 0")
        db.execute("UPDATE users SET approved=1")  # sadece ilk migration'da çalışır
    except sqlite3.OperationalError:
        pass
    # Owner her zaman onaylı
    try:
        db.execute("UPDATE users SET approved=1 WHERE role='owner'")
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
    # payments — sonradan eklenen sütunlar (avans/aванс kaydı kind/note kullanıyor)
    for _pc, _pt in (("kind", "TEXT"), ("note", "TEXT")):
        try:
            db.execute(f"ALTER TABLE payments ADD COLUMN {_pc} {_pt}")
        except sqlite3.OperationalError:
            pass
    # ─── Çaевые (Bahşiş) ───
    db.execute("""CREATE TABLE IF NOT EXISTS tips (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER, amount INTEGER, period TEXT,
        note TEXT,
        added_by INTEGER, added_by_name TEXT,
        created_at TEXT)""")
    # ─── Click/Payme ödeme akışı (gruptan yakalanan bildirimler) ───
    db.execute("""CREATE TABLE IF NOT EXISTS pay_feed (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        provider TEXT, amount INTEGER, ok INTEGER,
        txid TEXT, pay_at TEXT, chat_id INTEGER, chat_title TEXT,
        raw TEXT, created_at TEXT,
        UNIQUE(provider, txid))""")
    # ─── Kasa / Сменный отчёт (vardiya raporu) ───
    db.execute("""CREATE TABLE IF NOT EXISTS cashreports (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER, user_name TEXT,
        date TEXT, period TEXT, created_at TEXT,
        bylo TEXT, restock TEXT, ostalos TEXT, sold TEXT, cups_total INTEGER,
        itogo INTEGER, click INTEGER, payme INTEGER, karta INTEGER, terminal INTEGER,
        cashless INTEGER, schitano INTEGER, vyshlo INTEGER, na_sdachi INTEGER, kassa INTEGER,
        expenses TEXT, expenses_total INTEGER, note TEXT)""")
    # cashreports — sonradan eklenen sütunlar (eski DB'ler için)
    for _col, _typ in (("daily_pay", "INTEGER"), ("hours", "REAL"),
                       ("start_time", "TEXT"), ("end_time", "TEXT"),
                       ("coffee_kg", "REAL")):
        try:
            db.execute(f"ALTER TABLE cashreports ADD COLUMN {_col} {_typ}")
        except sqlite3.OperationalError:
            pass
    # ─── Meta (key-value: ödeme hatırlatması vb.) ───
    db.execute("""CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, val TEXT)""")
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
    # Сэндвич'i kataloğa BİR KEZ ekle (mevcut/özelleştirilmiş DB'de yoksa). Meta bayrağı →
    # owner sonradan silerse her restart'ta geri gelmesin.
    try:
        if not db.execute("SELECT 1 FROM meta WHERE k='seed_sandwich'").fetchone():
            db.execute("INSERT OR IGNORE INTO desserts_catalog (id,label,icon,price,sort_order,active,updated_at) VALUES (?,?,?,?,?,1,?)",
                       ("sandwich", "Сэндвич", "🥪", 500, 7, datetime.now(TZ).isoformat()))
            db.execute("INSERT OR REPLACE INTO meta (k,val) VALUES ('seed_sandwich', ?)", (datetime.now(TZ).isoformat(),))
    except sqlite3.OperationalError:
        pass
    # ─── Ступени обслуживания — günlük ознакомление (kim hangi gün okudu/onayladı) ───
    db.execute("""CREATE TABLE IF NOT EXISTS std_acks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER, user_name TEXT, date TEXT, created_at TEXT,
        UNIQUE(user_id, date))""")
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
    # Borç (avans) talepleri — barista şeften ister
    db.execute("""CREATE TABLE IF NOT EXISTS loans (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        barista_id INTEGER,
        amount INTEGER,
        reason TEXT,
        status TEXT DEFAULT 'pending',
        decided_by INTEGER,
        decided_at TEXT,
        decision_note TEXT,
        created_at TEXT,
        repaid INTEGER DEFAULT 0)""")
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
    # ─── Zamanlı siparişler (gelecekte gruba gönderilecek) ───
    db.execute("""CREATE TABLE IF NOT EXISTS scheduled_orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER, user_name TEXT,
        group_id TEXT, branch_id INTEGER,
        body TEXT, total INTEGER, items TEXT,
        send_at TEXT, created_at TEXT,
        sent INTEGER DEFAULT 0, canceled INTEGER DEFAULT 0)""")
    # ─── Филиалы (şubeler) — çok şube desteği ───
    db.execute("""CREATE TABLE IF NOT EXISTS branches (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        group_chat_id TEXT,
        sort_order INTEGER DEFAULT 0,
        active INTEGER DEFAULT 1,
        created_at TEXT)""")
    # Şubelenen tablolara branch_id kolonu (eski DB'lerde yoksa ekle; hepsi ana şubeye = 1).
    # SQLite ALTER ADD COLUMN ... DEFAULT 1 mevcut satırları da 1 yapar.
    for _bt in ("users", "shifts", "cashreports", "orders", "std_acks"):
        try:
            db.execute(f"ALTER TABLE {_bt} ADD COLUMN branch_id INTEGER DEFAULT 1")
        except sqlite3.OperationalError:
            pass
    # İlk kurulum / migration: hiç şube yoksa ana şube "C5" (id=1). group_chat_id →
    # daha önce /setgroup ile kaydedilmiş active_group (varsa) atanır, yoksa boş.
    try:
        _bc = db.execute("SELECT COUNT(*) AS c FROM branches").fetchone()
        if (_bc["c"] or 0) == 0:
            _ag = db.execute("SELECT val FROM meta WHERE k='active_group'").fetchone()
            _ag_val = (_ag["val"] if _ag else None) or (GROUP_CHAT_ID or None)
            db.execute(
                "INSERT INTO branches (id, name, group_chat_id, sort_order, active, created_at) "
                "VALUES (1, ?, ?, 0, 1, ?)",
                ("C5", _ag_val, datetime.now(TZ).isoformat()))
            # Mevcut satırlarda branch_id NULL kalmışsa (kolon zaten vardı ama boşsa) 1 yap.
            for _bt in ("users", "shifts", "cashreports", "orders", "std_acks"):
                try:
                    db.execute(f"UPDATE {_bt} SET branch_id=1 WHERE branch_id IS NULL")
                except sqlite3.OperationalError:
                    pass
    except sqlite3.OperationalError:
        pass
    db.commit()
    return db


# ═══════════════════════════════════════
#  ФИЛИАЛЫ (ŞUBELER)
# ═══════════════════════════════════════
DEFAULT_BRANCH_ID = 1
# Telegram "анонимный администратор" grup adına yazınca from.id bu olur (GroupAnonymousBot).
ANON_ADMIN_ID = 1087968824


def get_branches(db, only_active=True):
    """Şubeler listesi: [{id,name,group_chat_id,sort_order,active}]."""
    q = "SELECT id, name, group_chat_id, sort_order, active FROM branches"
    if only_active:
        q += " WHERE COALESCE(active,1)=1"
    q += " ORDER BY sort_order, id"
    return [dict(r) for r in db.execute(q).fetchall()]


def get_branch(db, branch_id):
    """Tek şube (dict) veya None."""
    if not branch_id:
        return None
    r = db.execute(
        "SELECT id, name, group_chat_id, sort_order, active FROM branches WHERE id=?",
        (int(branch_id),)).fetchone()
    return dict(r) if r else None


def user_branch_id(db, user_id):
    """Kullanıcının atandığı (ev) şube — yoksa ana şube."""
    r = db.execute("SELECT branch_id FROM users WHERE user_id=?", (user_id,)).fetchone()
    return int(r["branch_id"]) if (r and r["branch_id"]) else DEFAULT_BRANCH_ID


def branch_group_id(db, branch_id):
    """Bir şubenin rapor grubu (group_chat_id) — yoksa None."""
    b = get_branch(db, branch_id)
    if b and b.get("group_chat_id"):
        return str(b["group_chat_id"])
    return None


def acting_branch_id(db, user_id):
    """Bir aksiyonun ait olduğu şube: 1) AÇIK vardiyanın şubesi →
    2) girişte seçilen oturum şubesi (cur_branch) → 3) ev şubesi.
    (Sipariş/kasa/grup yönlendirmesi için.)"""
    try:
        act = get_active_shift(db, user_id)
        if act is not None and act["branch_id"]:
            return int(act["branch_id"])
    except Exception:
        pass
    try:
        r = db.execute("SELECT val FROM meta WHERE k=?", (f"cur_branch_{user_id}",)).fetchone()
        if r and r["val"]:
            b = int(r["val"])
            if get_branch(db, b):
                return b
    except Exception:
        pass
    return user_branch_id(db, user_id)


def resolve_group_id(db, user_id, context=None, branch_id=None):
    """Bir aksiyonun raporunun gideceği Telegram grubu. Öncelik:
    1) verilen branch_id → 2) kullanıcının AÇIK vardiyasının şubesi →
    3) kullanıcının ev şubesi. Şubede grup tanımsızsa eski tekil gruba düşer
    (böylece tek şubede davranış hiç değişmez)."""
    bid = branch_id or acting_branch_id(db, user_id)
    g = branch_group_id(db, bid)
    if g:
        return g
    # Fallback: eski tekil grup (active_group / env)
    if context is not None:
        return context.bot_data.get("group_id") or GROUP_CHAT_ID or None
    return GROUP_CHAT_ID or None


# ═══════════════════════════════════════
#  ЗАРПЛАТА (MAAŞ SİSTEMİ)
# ═══════════════════════════════════════
HOURLY_RATE = 12000  # сум за час

# ─── Çalışma saati / ödeme yapılandırması (owner ayarlar, meta'da tutulur) ───
PAY_DEFAULTS = {"open": 7, "close": 3, "max": 20, "unpaid": 1}


def get_pay_cfg(db):
    """{open, close, max, unpaid} — açılış/kapanış saati, max vardiya (saat), kapalı
    pencere düşümü açık mı. Owner Настройки'den değiştirir."""
    cfg = dict(PAY_DEFAULTS)
    try:
        rows = db.execute(
            "SELECT k,val FROM meta WHERE k IN ('pay_open','pay_close','pay_max','pay_unpaid')").fetchall()
        for r in rows:
            key = r["k"].split("_", 1)[1]
            try:
                cfg[key] = int(r["val"])
            except Exception:
                pass
    except Exception:
        pass
    return cfg


def paid_hours(start_dt, end_dt, cfg):
    """Ödenecek saat: ham süreden KAPALI pencereye (kapanış→açılış, ör. 03:00–07:00)
    denk gelen kısım düşülür; sonuç max vardiya saatiyle sınırlanır (unutulan vardiya
    koruması). start_dt/end_dt naive datetime."""
    if not end_dt or not start_dt or end_dt <= start_dt:
        return 0.0
    raw = (end_dt - start_dt).total_seconds() / 3600.0
    unpaid = 0.0
    if cfg.get("unpaid", 1):
        oh = int(cfg.get("open", 7)); ch = int(cfg.get("close", 3))
        if 0 <= ch < oh <= 24:
            day = start_dt.replace(hour=0, minute=0, second=0, microsecond=0)
            end_day = end_dt.replace(hour=0, minute=0, second=0, microsecond=0)
            guard = 0
            while day <= end_day and guard < 400:
                w0 = day.replace(hour=ch); w1 = day.replace(hour=oh)
                ov = (min(end_dt, w1) - max(start_dt, w0)).total_seconds()
                if ov > 0:
                    unpaid += ov / 3600.0
                day = day + timedelta(days=1); guard += 1
    paid = max(0.0, raw - unpaid)
    mx = int(cfg.get("max", 20) or 0)
    if mx and paid > mx:
        paid = float(mx)
    return round(paid, 2)


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


def fmt_hm(h):
    """Ondalık saati okunabilir göster: 0.65 → '39м', 1.5 → '1ч 30м', 8 → '8ч'."""
    n = float(h or 0)
    if n <= 0:
        return "0м"
    total_min = round(n * 60)
    hh, mm = divmod(total_min, 60)
    if hh == 0:
        return f"{mm}м"
    if mm == 0:
        return f"{hh}ч"
    return f"{hh}ч {mm}м"


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
    ARTIK NO-OP. Eskiden her aksiyondan sonra "🔄 ... 👇" mesajı + reply klavye
    gönderiyordu (DM kalabalığı). Tazelik artık /api/ver oto-yenileme ile sağlanıyor;
    kullanıcı sade DM istedi → hiçbir şey gönderme.
    """
    return
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
def _valid_name(s):
    # En az 2 karakter VE en az bir harf/rakam olmalı — "•", ".", "-" gibi tek anlamsız ad geçersiz
    s = (s or "").strip()
    return s if (len(s) >= 2 and any(c.isalnum() for c in s)) else ""

def display_name_for(db, user_id, fallback=None):
    row = db.execute("SELECT display_name, name FROM users WHERE user_id=?", (user_id,)).fetchone()
    if row and _valid_name(row["display_name"]):
        return _valid_name(row["display_name"])
    if row and _valid_name(row["name"]):
        return _valid_name(row["name"])
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


def start_shift(db, user_id, custom_start=None, branch_id=None):
    """
    Aktif vardiya yoksa yeni başlat. Varsa onu döner.
    custom_start: ISO string veya 'HH:MM' (telefon kapanmışsa geriye dönük başlatma).
    branch_id: baristanın seçtiği şube (yoksa ev şubesi).
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
    # Seçilen şube geçerli mi? Değilse ev şubesine düş.
    bid = None
    try:
        if branch_id and get_branch(db, branch_id):
            bid = int(branch_id)
    except Exception:
        bid = None
    if not bid:
        bid = user_branch_id(db, user_id)
    cur = db.execute(
        "INSERT INTO shifts (user_id, hours, drinks, bonus, hourly_pay, total, date, period, created_at, start_time, end_time, note, branch_id) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (user_id, 0.0, json.dumps({}), 0, 0, 0,
         start_dt.strftime("%Y-%m-%d"), period, now.isoformat(),
         start_dt.isoformat(), None, "", bid))
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
    # Ödenecek saat: kapalı pencere (03:00–07:00) düşülür + max ile sınırlı.
    hours = paid_hours(start, end_dt, get_pay_cfg(db))
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


def build_reports(db, role, user_id):
    """Отчёт odaları için liste verisi. Owner → tüm baristalar; barista → sadece kendi
    (siparişler + mesai). Her sorgu defansif (kolon yoksa boş döner, state patlamasın)."""
    def Q(sql, params=()):
        try:
            return [dict(r) for r in db.execute(sql, params).fetchall()]
        except Exception as e:
            logger.warning(f"build_reports query failed: {e}")
            return []
    rep = {}
    if role == "owner":
        rep["tips"] = Q("SELECT t.amount AS amount, t.note AS note, t.created_at AS at, "
                        "COALESCE(u.display_name,u.name,'?') AS nm FROM tips t "
                        "LEFT JOIN users u ON u.user_id=t.user_id ORDER BY t.id DESC LIMIT 40")
        rep["pays"] = Q("SELECT p.amount AS amount, p.kind AS kind, p.note AS note, p.paid_at AS at, "
                        "COALESCE(u.display_name,u.name,'?') AS nm FROM payments p "
                        "LEFT JOIN users u ON u.user_id=p.user_id ORDER BY p.id DESC LIMIT 40")
        rep["fines"] = Q("SELECT f.amount AS amount, f.reason AS reason, f.created_at AS at, "
                         "COALESCE(u.display_name,u.name,'?') AS nm FROM fines f "
                         "LEFT JOIN users u ON u.user_id=f.user_id ORDER BY f.id DESC LIMIT 40")
        rep["shifts"] = Q("SELECT s.hours AS hours, s.total AS total, s.start_time AS start_time, "
                          "s.end_time AS end_time, s.created_at AS at, "
                          "COALESCE(u.display_name,u.name,'?') AS nm FROM shifts s "
                          "LEFT JOIN users u ON u.user_id=s.user_id "
                          "WHERE (s.end_time IS NOT NULL OR s.start_time IS NULL) ORDER BY s.id DESC LIMIT 40")
        rep["orders"] = Q("SELECT items, created_at AS at, user_name AS nm FROM orders ORDER BY id DESC LIMIT 40")
        rep["loans"] = Q("SELECT l.amount AS amount, l.reason AS reason, l.status AS status, "
                         "l.created_at AS at, COALESCE(u.display_name,u.name,'?') AS nm FROM loans l "
                         "LEFT JOIN users u ON u.user_id=l.barista_id ORDER BY l.id DESC LIMIT 40")
    else:
        rep["orders"] = Q("SELECT items, created_at AS at, user_name AS nm FROM orders "
                          "WHERE user_id=? ORDER BY id DESC LIMIT 40", (user_id,))
        rep["shifts"] = Q("SELECT hours, total, start_time, end_time, created_at AS at FROM shifts "
                          "WHERE user_id=? AND (end_time IS NOT NULL OR start_time IS NULL) "
                          "ORDER BY id DESC LIMIT 40", (user_id,))
    return rep


def build_hash_payload(db, user_id, name):
    """URL-hash payload string'ini (uid=...&role=...&summary=...) üretir.
    Hem klavye-butonu URL'i (build_webapp_url) hem de /api/state HTTP ucu (Yol B)
    bu aynı payload'ı kullanır — böylece ana ekrandan açınca da aynı veri gelir."""
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
    # Avans talepleri — barista kendisininkiler, owner pending olanların hepsi
    if role == "owner":
        loan_rows = db.execute(
            "SELECT l.*, u.name as bn, u.display_name as bdn FROM loans l "
            "LEFT JOIN users u ON u.user_id = l.barista_id "
            "WHERE l.status='pending' ORDER BY l.id DESC LIMIT 20").fetchall()
        loans_data = [{
            "id": r["id"], "uid": r["barista_id"],
            "name": (r["bdn"] or r["bn"] or "?"),
            "amount": r["amount"], "reason": r["reason"] or "",
            "status": r["status"], "at": r["created_at"]
        } for r in loan_rows]
    else:
        loan_rows = db.execute(
            "SELECT * FROM loans WHERE barista_id=? ORDER BY id DESC LIMIT 10",
            (user_id,)).fetchall()
        loans_data = [{
            "id": r["id"], "amount": r["amount"], "reason": r["reason"] or "",
            "status": r["status"], "at": r["created_at"],
            "decision_note": r["decision_note"] or ""
        } for r in loan_rows]
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
    # Kasa: son raporun "Осталось"u → yeni "Было" ön-doldurma için (en son hangi vardiya kapatıldıysa)
    try:
        cr = db.execute("SELECT ostalos FROM cashreports ORDER BY id DESC LIMIT 1").fetchone()
        kasa_last = json.loads(cr["ostalos"]) if (cr and cr["ostalos"]) else {}
    except Exception:
        kasa_last = {}
    # Kasa raporları listesi: owner hepsini, barista kendininkini görür
    try:
        _crcols = ("SELECT id,user_name,date,created_at,cups_total,itogo,click,payme,karta,terminal,"
                   "cashless,schitano,vyshlo,kassa,sold,expenses,daily_pay,hours,start_time,end_time,note,branch_id FROM cashreports ")
        if role == "owner":
            crs = db.execute(_crcols + "ORDER BY id DESC LIMIT 15").fetchall()
        else:
            crs = db.execute(_crcols + "WHERE user_id=? ORDER BY id DESC LIMIT 10", (user_id,)).fetchall()
        kasa_reports = [dict(r) for r in crs]
    except Exception:
        kasa_reports = []
    # Ступени обслуживания — bu kullanıcı bugün onayladı mı?
    today_str = datetime.now(TZ).strftime("%Y-%m-%d")
    std_acked = bool(db.execute("SELECT 1 FROM std_acks WHERE user_id=? AND date=?", (user_id, today_str)).fetchone())
    # ── Филиалы (şubeler): owner hepsini (grup+aktiflik yönetim için), barista sadece aktif id+ad ──
    try:
        if role == "owner":
            branches_out = [{"id": b["id"], "name": b["name"], "group": b["group_chat_id"] or "",
                             "active": int(b["active"] or 0), "sort": b["sort_order"] or 0}
                            for b in get_branches(db, only_active=False)]
        else:
            branches_out = [{"id": b["id"], "name": b["name"]} for b in get_branches(db, only_active=True)]
    except Exception:
        branches_out = []
    my_branch = user_branch_id(db, user_id)
    # ── Haftalık график (bu haftanın Pazartesi'si) ──
    _today_dt = datetime.now(TZ)
    _monday = (_today_dt - timedelta(days=_today_dt.weekday())).strftime("%Y-%m-%d")
    _sched = {}
    try:
        _sr = db.execute("SELECT val FROM meta WHERE k=?", (f"sched_{_monday}",)).fetchone()
        if _sr and _sr["val"]:
            _sched = json.loads(_sr["val"])
    except Exception:
        _sched = {}
    # ── Zamanlı siparişler (bekleyen): barista kendininki, owner hepsi ──
    try:
        if role == "owner":
            _srows = db.execute(
                "SELECT id,user_name,total,send_at,branch_id FROM scheduled_orders "
                "WHERE COALESCE(sent,0)=0 AND COALESCE(canceled,0)=0 ORDER BY send_at LIMIT 40").fetchall()
        else:
            _srows = db.execute(
                "SELECT id,user_name,total,send_at,branch_id FROM scheduled_orders "
                "WHERE user_id=? AND COALESCE(sent,0)=0 AND COALESCE(canceled,0)=0 ORDER BY send_at LIMIT 20",
                (user_id,)).fetchall()
        scheduled_out = [{"id": r["id"], "nm": r["user_name"] or "?", "total": r["total"] or 0,
                          "at": r["send_at"], "bid": r["branch_id"] or 1} for r in _srows]
    except Exception:
        scheduled_out = []
    parts = [
        f"uid={user_id}",
        f"role={role}",
        f"name={quote(show_name or '')}",
        f"pwh={pwh}",
        f"std_ack={1 if std_acked else 0}",
        f"summary={quote(json.dumps(summary, ensure_ascii=False))}",
        f"prices={quote(json.dumps(prices, ensure_ascii=False))}",
        f"desserts={quote(json.dumps(desserts_cat, ensure_ascii=False))}",
        f"rt={quote(json.dumps(rt_self, ensure_ascii=False))}",
        f"exam={quote(json.dumps(pending_exam, ensure_ascii=False) if pending_exam else '')}",
        f"loans={quote(json.dumps(loans_data, ensure_ascii=False))}",
        f"kasa_last={quote(json.dumps(kasa_last, ensure_ascii=False))}",
        f"kasa_reports={quote(json.dumps(kasa_reports, ensure_ascii=False))}",
        f"rep={quote(json.dumps(build_reports(db, role, user_id), ensure_ascii=False))}",
        f"branches={quote(json.dumps(branches_out, ensure_ascii=False))}",
        f"my_branch={my_branch}",
        f"scheduled={quote(json.dumps(scheduled_out, ensure_ascii=False))}",
        f"pay_cfg={quote(json.dumps(get_pay_cfg(db) if role=='owner' else {}, ensure_ascii=False))}",
        f"sched_week={_monday}",
        f"sched={quote(json.dumps(_sched, ensure_ascii=False))}",
        f"ts={ts}",
    ]
    # ── Отчёт odaları için kayıtlar (owner: hepsi · barista: sadece kendi vardiya+sipariş) ──
    def _repq(sql, params=(), n=120):
        try:
            return db.execute(sql + f" LIMIT {n}", params).fetchall()
        except Exception:
            return []
    if role == "owner":
        _sh = _repq("SELECT s.start_time, s.end_time, s.hours, s.total, s.branch_id AS bid, COALESCE(u.display_name,u.name) AS nm "
                    "FROM shifts s LEFT JOIN users u ON u.user_id=s.user_id "
                    "WHERE s.start_time IS NOT NULL ORDER BY s.start_time DESC", (), 150)
        _or = _repq("SELECT user_name AS nm, items, created_at, branch_id AS bid FROM orders ORDER BY id DESC", (), 60)
        _ti = _repq("SELECT t.amount, t.note, t.created_at, COALESCE(u.display_name,u.name) AS nm "
                    "FROM tips t LEFT JOIN users u ON u.user_id=t.user_id ORDER BY t.id DESC", (), 60)
        _pa = _repq("SELECT p.amount, p.kind, p.note, p.paid_at, COALESCE(u.display_name,u.name) AS nm "
                    "FROM payments p LEFT JOIN users u ON u.user_id=p.user_id "
                    "WHERE p.paid_by!=p.user_id ORDER BY p.id DESC", (), 60)
        _fi = _repq("SELECT f.amount, f.reason, f.created_at, COALESCE(u.display_name,u.name) AS nm "
                    "FROM fines f LEFT JOIN users u ON u.user_id=f.user_id ORDER BY f.id DESC", (), 60)
        _lo = _repq("SELECT l.amount, l.reason, l.status, l.created_at, COALESCE(u.display_name,u.name) AS nm "
                    "FROM loans l LEFT JOIN users u ON u.user_id=l.barista_id ORDER BY l.id DESC", (), 60)
    else:
        _sh = _repq("SELECT s.start_time, s.end_time, s.hours, s.total, s.branch_id AS bid, ? AS nm FROM shifts s "
                    "WHERE s.user_id=? AND s.start_time IS NOT NULL ORDER BY s.start_time DESC",
                    (show_name, user_id), 90)
        _or = _repq("SELECT user_name AS nm, items, created_at, branch_id AS bid FROM orders WHERE user_id=? ORDER BY id DESC",
                    (user_id,), 50)
        _ti = _pa = _fi = _lo = []
    rep = {
        "shifts": [{"nm": r["nm"] or "?", "start_time": r["start_time"], "end_time": r["end_time"], "hours": r["hours"] or 0, "total": r["total"] or 0, "bid": r["bid"] or 1} for r in _sh],
        "orders": [{"nm": r["nm"] or "?", "items": r["items"] or "", "at": r["created_at"], "bid": r["bid"] or 1} for r in _or],
        "tips": [{"nm": r["nm"] or "?", "amount": r["amount"] or 0, "note": r["note"] or "", "at": r["created_at"]} for r in _ti],
        "pays": [{"nm": r["nm"] or "?", "amount": r["amount"] or 0, "kind": r["kind"] or "", "note": r["note"] or "", "at": r["paid_at"]} for r in _pa],
        "fines": [{"nm": r["nm"] or "?", "amount": r["amount"] or 0, "reason": r["reason"] or "", "at": r["created_at"]} for r in _fi],
        "loans": [{"nm": r["nm"] or "?", "amount": r["amount"] or 0, "reason": r["reason"] or "", "status": r["status"] or "", "at": r["created_at"]} for r in _lo],
    }
    parts.append(f"rep={quote(json.dumps(rep, ensure_ascii=False))}")
    if role == "owner":
        rows = db.execute(
            "SELECT user_id, name, username, role, display_name, password, authorized, "
            "COALESCE(archived,0) AS archived, archived_at, COALESCE(branch_id,1) AS branch_id "
            "FROM users WHERE COALESCE(approved,0)=1 "
            "ORDER BY COALESCE(archived,0), COALESCE(display_name,name)").fetchall()
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
                "bid": b["branch_id"] or 1,
                "recent": [],
                "rt": rt_data,
                "pw": 1 if (b["password"] or "").strip() else 0,
                "auth": 1 if (b["authorized"] or 0) else 0,
                "arch": 1 if (b["archived"] or 0) else 0,
                "arch_at": b["archived_at"] or "",
            })
        parts.append(f"baristas={quote(json.dumps(baristas, ensure_ascii=False))}")
        # Onay bekleyen yeni başlayanlar (approved=0) — 'Все баристы'ye girmez, ayrı liste
        pend_rows = db.execute(
            "SELECT user_id, name, username, created_at FROM users "
            "WHERE COALESCE(approved,0)=0 AND COALESCE(archived,0)=0 AND role!='owner' "
            "ORDER BY created_at DESC").fetchall()
        pending = [{"id": p["user_id"], "n": (p["name"] or "?"),
                    "u": p["username"] or "", "at": p["created_at"] or ""} for p in pend_rows]
        parts.append(f"pending={quote(json.dumps(pending, ensure_ascii=False))}")
        # Loglar (son 50) — Настройки→Логи için. Client en yeniyi üstte göstersin diye eski→yeni sırada gönder.
        try:
            log_rows = db.execute(
                "SELECT action, actor_name, target_name, details, created_at FROM logs ORDER BY id DESC LIMIT 50").fetchall()
            logs_data = [{"action": r["action"], "actor_name": r["actor_name"],
                          "target_name": r["target_name"], "details": r["details"],
                          "created_at": r["created_at"]} for r in log_rows]
            logs_data.reverse()
        except Exception:
            logs_data = []
        parts.append(f"logs={quote(json.dumps(logs_data, ensure_ascii=False))}")
        # Bugün стандарт'ı onaylayanlar (Отчёт izi)
        std_rows = db.execute(
            "SELECT user_name, created_at FROM std_acks WHERE date=? ORDER BY id DESC", (today_str,)).fetchall()
        std_acks_today = [{"name": r["user_name"], "at": r["created_at"]} for r in std_rows]
        parts.append(f"std_acks={quote(json.dumps(std_acks_today, ensure_ascii=False))}")
    return "&".join(parts)


def build_webapp_url(base_url, user_id, name, db):
    """Yol B: URL'e DEV hash GÖMÜLMEZ. State artık HTTP /api/state'ten geliyor.
    Hash'i gömmek owner'da (çok barista) Telegram buton-URL limitini aşıyordu
    ('Слишком много данных' hatası) ve URL kırpılınca aktif vardiya kayboluyordu.
    Sadece cache-buster ?v= ekliyoruz ki her açılışta TAZE HTML yüklensin."""
    ts = int(datetime.now(TZ).timestamp())
    sep = "&" if "?" in base_url else "?"
    return base_url + f"{sep}v={ts}"

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
            "Телевизоры включены (реклама на экранах)",
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
        db.execute("UPDATE users SET role='owner', approved=1 WHERE user_id=?", (user.id,))
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

    # Sade DM: kalıcı reply klavyeyi KALDIR (alttaki 'Открыть Caffelito' butonu gitsin).
    # Açılış: owner → Main App ('Открыть Caffelito' alt bar); barista → ≡ menü butonu (sync_user_ui).
    role_now = get_role(db, user.id)
    if auto_owner:
        msg = "👑 Вы — владелец. Caffelito готов."
    elif role_now == "owner":
        msg = "☕ Caffelito готов."
    else:
        msg = "☕ Caffelito."
    try:
        await update.message.reply_text(msg, reply_markup=ReplyKeyboardRemove())
    except Exception as e:
        logger.error(f"start reply failed: {e}")


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
        # Owner zaten var → baristaya hiçbir şey gösterme (sade DM).
        return
    db.execute("UPDATE users SET role='owner', approved=1 WHERE user_id=?", (user.id,))
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
        return  # baristaya sessiz
    if not context.args:
        await update.message.reply_text("Использование: /grantowner @username\nили /grantowner <user_id>")
        return
    target = find_user(db, context.args[0])
    if not target:
        await update.message.reply_text(
            f"❌ Не найден: {context.args[0]}\n\n"
            f"Этот человек должен сначала написать боту /start.")
        return
    db.execute("UPDATE users SET role='owner', approved=1 WHERE user_id=?", (target["user_id"],))
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
#  BORÇ (LOAN) HELPER
# ═══════════════════════════════════════

async def _decide_loan(context, db, actor, loan_id, decision, note, reply_fn):
    """Owner tarafından borç talebini sonuçlandırır.
    decision: 'approve' veya 'reject'
    """
    row = db.execute("SELECT * FROM loans WHERE id=?", (loan_id,)).fetchone()
    if not row:
        await reply_fn("❌ Запрос не найден.")
        return
    if row["status"] != "pending":
        await reply_fn(f"⚠️ Этот запрос уже {row['status']}.")
        return
    if decision not in ("approve", "reject"):
        return
    new_status = "approved" if decision == "approve" else "rejected"
    now = datetime.now(TZ).isoformat()
    db.execute(
        "UPDATE loans SET status=?, decided_by=?, decided_at=?, decision_note=? WHERE id=?",
        (new_status, actor.id, now, note or None, loan_id))
    if decision == "approve":
        # Onaylanmış borç, baristanın net maaşından düşecek (avans olarak)
        period = current_period()
        db.execute(
            "INSERT INTO payments (user_id, period, amount, kind, note, paid_by, paid_by_name, paid_at) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (row["barista_id"], period, row["amount"], "loan",
             f"Аванс: {row['reason']}", actor.id, actor.first_name, now))
    db.commit()
    log_action(db, "loan_" + decision, actor.id, actor.first_name,
               row["barista_id"], display_name_for(db, row["barista_id"]),
               {"loan_id": loan_id, "amount": row["amount"], "note": note})
    shown = display_name_for(db, row["barista_id"])
    # Bariste bildir
    try:
        if decision == "approve":
            await context.bot.send_message(
                row["barista_id"],
                f"✅ *Аванс одобрен*\n\n"
                f"Сумма: *{fmt_sum(row['amount'])}* сум\n"
                f"Будет вычтена из ближайшей зарплаты.\n"
                + (f"\nОт шефа: {md_safe(note)}" if note else ""),
                parse_mode="Markdown")
        else:
            await context.bot.send_message(
                row["barista_id"],
                f"❌ *Запрос аванса отклонён*\n\n"
                f"Сумма: {fmt_sum(row['amount'])} сум\n"
                + (f"Причина: {md_safe(note)}" if note else "Без комментария"),
                parse_mode="Markdown")
    except Exception:
        pass
    await reply_fn(
        ("✅ Аванс одобрен" if decision == "approve" else "❌ Запрос отклонён") +
        f"\n\nКому: {md_safe(shown)}\nСумма: {fmt_sum(row['amount'])} сум")


# ═══════════════════════════════════════
#  CALLBACK HANDLER
# ═══════════════════════════════════════

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "noop":
        return

    # ─── Borç onay/red butonları (inline) ───
    if data.startswith("loan_ok:") or data.startswith("loan_no:"):
        try:
            loan_id = int(data.split(":")[1])
        except (IndexError, ValueError):
            return
        decision = "approve" if data.startswith("loan_ok:") else "reject"
        db = get_db()
        if get_role(db, query.from_user.id) != "owner":
            await query.edit_message_text("❌ Только владелец может решать.")
            return
        async def _reply(txt):
            try: await query.edit_message_text(txt, parse_mode="Markdown")
            except Exception:
                try: await context.bot.send_message(query.message.chat_id, txt, parse_mode="Markdown")
                except: pass
        await _decide_loan(context, db, query.from_user, loan_id, decision, "", _reply)
        return

    # ─── /setgroup şube seçimi (inline) ───
    if data.startswith("setgrp:"):
        try:
            bid = int(data.split(":")[1])
        except (IndexError, ValueError):
            return
        db = get_db()
        # Owner ya da anonim grup admini (GroupAnonymousBot) bağlayabilir
        if get_role(db, query.from_user.id) != "owner" and query.from_user.id != ANON_ADMIN_ID:
            try: await query.edit_message_text("❌ Только владелец может привязать группу.")
            except Exception: pass
            return
        b = get_branch(db, bid)
        if not b:
            try: await query.edit_message_text("❌ Филиал не найден.")
            except Exception: pass
            return
        chat_id = query.message.chat_id
        bind_group_to_branch(db, context, bid, chat_id)
        try:
            await query.edit_message_text(
                f"✅ Группа привязана к филиалу «{b['name']}»!\n"
                f"Отчёты, заказы, задачи и кассы этого филиала теперь приходят сюда.\nID: {chat_id}")
        except Exception:
            try: await context.bot.send_message(chat_id, f"✅ Группа привязана к филиалу «{b['name']}»! ID: {chat_id}")
            except Exception: pass
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
            "INSERT INTO orders (chat_id, user_id, user_name, items, created_at, branch_id) VALUES (?,?,?,?,?,?)",
            (update.effective_chat.id, user.id, user.first_name,
             json.dumps(order), now.isoformat(), acting_branch_id(db, user.id)))
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

async def deliver_order(bot, group_id, header, esc_lines, footer):
    """Sipariş mesajını gruba gönder (uzunsa <pre> parçalara bölerek). Hem anlık
    hem zamanlı sipariş bunu kullanır. esc_lines zaten HTML-escape'lidir."""
    full = header + "<pre>" + "\n".join(esc_lines) + "</pre>" + footer
    if len(full.encode('utf-8')) <= 4096:
        await bot.send_message(chat_id=int(group_id), text=full, parse_mode="HTML")
        return
    batches, cur, clen = [], [], 0
    for ln in esc_lines:
        if cur and clen + len(ln) + 1 > 3500:
            batches.append(cur); cur, clen = [], 0
        cur.append(ln); clen += len(ln) + 1
    if cur:
        batches.append(cur)
    n = len(batches)
    for i, b in enumerate(batches):
        msg = (header if i == 0 else "") + "<pre>" + "\n".join(b) + "</pre>" + (footer if i == n - 1 else "")
        await bot.send_message(chat_id=int(group_id), text=msg, parse_mode="HTML")


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
    # Grup/owner mesajlarında gösterilecek ad: owner'ın atadığı display_name (yoksa TG adı)
    shown = display_name_for(db, user.id, fallback=user.first_name)

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

        # Rapor grubu: kullanıcının açık vardiyasının / ev şubesinin grubu (çok şube).
        # Tek şubede branch 1'in grubu = eski active_group olduğundan davranış değişmez.
        group_id = resolve_group_id(db, user.id, context)

        if action == "order":
            from html import escape as esc_html
            import re as _re
            def _clean(name):
                # Parantez içindeki birim/açıklamaları kaldır: "Молоко 3.2% (1 уп)" → "Молоко 3.2%"
                return _re.sub(r'\s*\([^)]*\)', '', str(name or '')).strip()
            total = data.get("c", 0)
            groups = data.get("g", [])
            order_items = []  # Отчёт → Заказы odası için kayıt

            # Önce tüm kalemleri topla: rows = [(имя, "Nx") | (имя, None) | None(=boşluk)]
            rows = []
            if groups:
                # Yeni kompakt format: ["Кофе|Эспрессо:1|Колумбия:3", ...]
                for gi, group_str in enumerate(groups):
                    parts = group_str.split('|')
                    if gi > 0:
                        rows.append(None)  # kategori arası boşluk
                    for item_str in parts[1:]:
                        if ':' in item_str:
                            iname, iqty = item_str.rsplit(':', 1)
                            nm, q = _clean(iname), iqty.strip()
                            rows.append((nm, q + "x"))
                            order_items.append({"n": nm, "q": q})
                        else:
                            rows.append((_clean(item_str), None))
            else:
                # Eski format desteği
                items = data.get("items") or data.get("i", {})
                names_from_app = data.get("names") or data.get("n", {})
                total = sum(items.values()) if items else total
                for pid, qty in items.items():
                    nm = _clean(names_from_app.get(pid) or NAMES.get(pid, pid))
                    rows.append((nm, str(qty) + "x"))
                    order_items.append({"n": nm, "q": str(qty)})

            # Sağa hizalı monospace tablo: isim solda, adет sağ kolonda hizalı
            named = [r for r in rows if r]
            maxn = max((len(r[0]) for r in named), default=0)  # gerçek en uzun isim → hepsi aynı kolona hizalı (cap yok)
            body_lines = []
            for r in rows:
                if r is None:
                    body_lines.append("")
                    continue
                nm, q = r
                if q is None:
                    body_lines.append(nm)
                elif len(nm) <= maxn:
                    body_lines.append(nm + " " * (maxn - len(nm) + 2) + q)
                else:
                    body_lines.append(nm + "  " + q)
            esc_lines = [esc_html(x) for x in body_lines]

            # ── Zamanlı sipariş mı? (send_at gelecekteyse gruba ŞİMDİ gönderme, sakla) ──
            _sa_raw = (data.get("send_at") or "").strip()
            _sched = _parse_user_time(_sa_raw) if _sa_raw else None
            if _sched and _sched > datetime.now(TZ).replace(tzinfo=None) + timedelta(minutes=1):
                _bid = acting_branch_id(db, user.id)
                db.execute(
                    "INSERT INTO scheduled_orders (user_id,user_name,group_id,branch_id,body,total,items,send_at,created_at,sent,canceled) "
                    "VALUES (?,?,?,?,?,?,?,?,?,0,0)",
                    (user.id, shown, str(group_id) if group_id else "", _bid,
                     "\n".join(esc_lines), total, json.dumps(order_items, ensure_ascii=False),
                     _sched.isoformat(), now.isoformat()))
                db.commit()
                await update.message.reply_text(
                    f"⏰ Заказ запланирован на *{_sched.strftime('%d.%m.%Y %H:%M')}*.\n"
                    f"Он автоматически уйдёт в группу в это время.\n"
                    f"_(Отменить — в приложении: Заказ → Запланированные.)_",
                    parse_mode="Markdown")
                return

            header = "<b>ЗАКАЗ — CAFFELITO</b>\n" + f"<b>{esc_html(shown)}</b> · {now.strftime('%d.%m.%Y %H:%M')}\n"
            footer = f"<b>Итого: {total} позиций</b>"
            if group_id:
                try:
                    await deliver_order(context.bot, group_id, header, esc_lines, footer)
                    logger.info("Order forwarded to group OK")
                except Exception as e:
                    logger.error(f"GROUP FORWARD FAILED: {e}")
            # Siparişi DB'ye kaydet (Отчёт → Заказы odası)
            try:
                _dbo = get_db()
                _dbo.execute(
                    "INSERT INTO orders (chat_id, user_id, user_name, items, created_at, branch_id) VALUES (?,?,?,?,?,?)",
                    (int(group_id) if group_id else 0, user.id, shown,
                     json.dumps(order_items, ensure_ascii=False), now.isoformat(), acting_branch_id(_dbo, user.id)))
                _dbo.commit()
            except Exception as e:
                logger.warning(f"order save failed: {e}")

        elif action == "tasks":
            completed = data.get("completed", [])
            pending = data.get("pending", [])
            total = data.get("total", len(completed) + len(pending))
            category = data.get("category", "")

            if group_id:
                try:
                    from html import escape as esc_html
                    # "uborka_Ежедневно" / "zadachi_Открытие" → güzel Rusça başlık (Kiril)
                    _pfx = {"uborka": "🫧 ЧИСТОТА", "temizlik": "🫧 ЧИСТОТА",
                            "zadachi": "✅ ЗАДАЧИ", "gorev": "✅ ЗАДАЧИ", "okk": "📋 ОКК"}
                    if "_" in category:
                        _p, _sub = category.split("_", 1)
                        cat_title = f"{_pfx.get(_p, '📋')} · {_sub}"
                    else:
                        cat_title = category or "📋 ЗАДАЧИ"
                    done_n, total_n = len(completed), (total or (len(completed) + len(pending)))
                    text = f"<b>{esc_html(cat_title)}</b>\n"
                    text += "━━━━━━━━━━━━━━━━━━━━\n"
                    text += f"👤 <b>{esc_html(shown)}</b>   ·   {now.strftime('%d.%m.%Y  %H:%M')}\n"
                    text += f"✅ Выполнено: <b>{done_n}/{total_n}</b>\n"
                    text += "━━━━━━━━━━━━━━━━━━━━\n\n"
                    for item in completed:
                        text += f"  ✅ {esc_html(item)}\n"
                    for item in pending:
                        text += f"  ❌ {esc_html(item)}\n"
                    if not pending:
                        text += "\n🎉 <b>Всё выполнено!</b>"
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
            # Barista'nın seçtiği şube (çok şube). Yoksa ev şubesi.
            sel_branch = data.get("branch") or data.get("branch_id")
            sh = start_shift(db, user.id, custom_start=custom_start, branch_id=sel_branch)
            # Vardiya artık açık → duyuru bu vardiyanın şubesinin grubuna gitsin.
            group_id = resolve_group_id(db, user.id, context, branch_id=sh["branch_id"])
            start_dt = datetime.fromisoformat(sh["start_time"])
            note_back = ""
            if custom_start:
                note_back = f"\n_(время указано вручную)_"
            await update.message.reply_text(
                f"🟢 *Смена началась!*\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"📅 {start_dt.strftime('%d.%m.%Y')}\n"
                f"⏰ Пришли в *{start_dt.strftime('%H:%M')}*{note_back}",
                parse_mode="Markdown")
            # Klavye butonunu taze URL ile yenile (yoksa tekrar açınca eski state görünür)
            await refresh_webapp_keyboard(update, context, db, user,
                "🔄 Откройте приложение — теперь видна активная смена 👇")
            if group_id:
                try:
                    from html import escape as esc_html
                    gtext = (f"🟢 <b>{esc_html(shown)}</b> начал(а) смену\n"
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
                    f"⏰ {start_dt.strftime('%H:%M')} → {end_dt.strftime('%H:%M')}  ({fmt_hm(hours)})\n"
                    f"🥤 Напитков: *{cups}* шт · 💰 {fmt_sum(drinks_bonus)} сум\n")
            if sweets:
                text += f"🍰 Десерты: *{sweets}* шт · 💰 {fmt_sum(dessert_bonus)} сум\n"
            text += (f"💵 Часы (12.000): {fmt_sum(hourly_pay)} _(в конце месяца)_\n"
                     f"💎 За смену: *{fmt_sum(total)}* сум\n"
                     f"━━━━━━━━━━━━━━━━━━\n"
                     f"📊 *Месяц {period}:*\n"
                     f"Часы: {fmt_hm(s['hours'])} | Смен: {s['shifts_count']}\n"
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
                    gtext = (f"🔴 <b>{esc_html(shown)}</b> закрыл(а) смену\n"
                             f"━━━━━━━━━━━━━━━━━━━━\n"
                             f"⏰ {start_dt.strftime('%H:%M')} → {end_dt.strftime('%H:%M')}  ({fmt_hm(hours)})\n"
                             f"🥤 Напитки: <b>{cups}</b> шт")
                    if sweets:
                        gtext += f"\n🍰 Десерты: <b>{sweets}</b> шт"
                    gtext += f"\n💰 Продажи: <b>{fmt_sum(sales_bonus)} сум</b>"
                    if note:
                        gtext += f"\n📝 {esc_html(note)}"
                    await context.bot.send_message(chat_id=int(group_id), text=gtext, parse_mode="HTML")
                except Exception as e:
                    logger.error(f"GROUP FORWARD FAILED: {e}")
                # Сменный отчёт — 'закрыл смену'dan SONRA gönderilir (cash_report buffer'ladı)
                try:
                    rep_t = context.bot_data.get("pending_report", {}).pop(user.id, None)
                    if rep_t:
                        await context.bot.send_message(chat_id=int(group_id), text=rep_t, parse_mode="HTML")
                except Exception as e:
                    logger.error(f"KASA report (after close) failed: {e}")
                # Stok uyarısı — kasa raporu + 'закрыл смену'dan SONRA (en sonda), ayrı mesaj
                try:
                    entry = context.bot_data.get("pending_stock", {}).pop(user.id, None)
                    if entry:
                        st_text, st_time = entry
                        if (datetime.now(TZ) - st_time).total_seconds() < 600:
                            # 1 DAKİKA SONRA gönder — handler'ı bloklamadan arka plan görevi
                            async def _delayed_stock(bot=context.bot, gid=int(group_id), text=st_text):
                                try:
                                    await asyncio.sleep(10)
                                    await bot.send_message(chat_id=gid, text=text, parse_mode="HTML")
                                except Exception as ex:
                                    logger.error(f"delayed STOK failed: {ex}")
                            asyncio.create_task(_delayed_stock())
                except Exception as e:
                    logger.error(f"STOK alert (after close) failed: {e}")
            # Sahiplere bildir (TAM detay — owner zarplata için görür)
            try:
                owners = db.execute("SELECT user_id FROM users WHERE role='owner' AND user_id != ?", (user.id,)).fetchall()
                for o in owners:
                    try:
                        otext = (f"📢 *{shown}* закрыл(а) смену\n"
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
                    gtext = (f"<b>СМЕНА — {esc_html(shown)}</b>\n"
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
            # distributions: dict {uid:amount} (app) VEYA list [{target,amount}] (eski) — ikisini de kabul et
            distributions = data.get("distributions") or {}
            note = (data.get("note") or "").strip()
            if not distributions:
                await update.message.reply_text("❌ Список получателей пуст.")
                return
            if isinstance(distributions, dict):
                _pairs = list(distributions.items())
            else:
                _pairs = [(d.get("target"), d.get("amount")) for d in distributions if isinstance(d, dict)]
            period = current_period()
            total_dist = 0
            recipients = []
            for _tid_raw, _amt_raw in _pairs:
                try:
                    tid = int(_tid_raw or 0)
                    amt = int(_amt_raw or 0)
                except Exception:
                    continue
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

        # ─── Филиалы (şube yönetimi — owner) ───
        elif action == "set_active_branch":
            # Owner uygulamada aktif şubeyi değiştirdi → /setgroup'un doğru şubeye
            # bağlaması için meta'ya yaz. (0 = «Все филиалы», grup bağlama için 0 sayılmaz.)
            db = get_db()
            if get_role(db, user.id) != "owner":
                return
            try:
                bid = int(data.get("branch_id") or 0)
            except Exception:
                bid = 0
            db.execute("INSERT OR REPLACE INTO meta (k,val) VALUES (?,?)",
                       (f"owner_branch_{user.id}", str(bid)))
            db.commit()

        elif action == "set_my_branch":
            # Barista girişte (veya vardiya ekranından) çalıştığı şubeyi seçti →
            # oturum şubesi meta'ya yazılır; grup yönlendirmesi bunu kullanır.
            db = get_db()
            try:
                bid = int(data.get("branch_id") or 0)
            except Exception:
                bid = 0
            if bid and get_branch(db, bid):
                db.execute("INSERT OR REPLACE INTO meta (k,val) VALUES (?,?)",
                           (f"cur_branch_{user.id}", str(bid)))
                db.commit()

        elif action == "move_shift_branch":
            # Açık vardiyayı başka şubeye taşı (çalışma süresi korunur, sonraki
            # raporlar yeni şubenin grubuna gider).
            db = get_db()
            try:
                bid = int(data.get("branch_id") or 0)
            except Exception:
                bid = 0
            if bid and get_branch(db, bid):
                act = get_active_shift(db, user.id)
                if act is not None:
                    db.execute("UPDATE shifts SET branch_id=? WHERE id=?", (bid, act["id"]))
                db.execute("INSERT OR REPLACE INTO meta (k,val) VALUES (?,?)",
                           (f"cur_branch_{user.id}", str(bid)))
                db.commit()

        elif action == "cancel_scheduled":
            # Bekleyen zamanlı siparişi iptal et (kendi siparişin ya da owner)
            db = get_db()
            try:
                sid = int(data.get("id") or 0)
            except Exception:
                sid = 0
            if sid:
                row = db.execute("SELECT user_id FROM scheduled_orders WHERE id=?", (sid,)).fetchone()
                if row and (row["user_id"] == user.id or get_role(db, user.id) == "owner"):
                    db.execute("UPDATE scheduled_orders SET canceled=1 WHERE id=? AND COALESCE(sent,0)=0", (sid,))
                    db.commit()

        elif action == "pay_settings":
            # Owner: çalışma saatleri / ödeme kuralı
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец.")
                return
            def _clampi(v, lo, hi, dflt):
                try:
                    return max(lo, min(hi, int(v)))
                except Exception:
                    return dflt
            op = _clampi(data.get("open"), 0, 23, 7)
            cl = _clampi(data.get("close"), 0, 23, 3)
            mx = _clampi(data.get("max"), 1, 48, 20)
            un = 1 if int(data.get("unpaid", 1) or 0) else 0
            for k, v in (("pay_open", op), ("pay_close", cl), ("pay_max", mx), ("pay_unpaid", un)):
                db.execute("INSERT OR REPLACE INTO meta (k,val) VALUES (?,?)", (k, str(v)))
            db.commit()
            await update.message.reply_text(
                f"✅ *Часы работы обновлены*\n"
                f"Открытие: {op:02d}:00 · Закрытие: {cl:02d}:00\n"
                f"Макс. смена: {mx} ч\n"
                f"Закрытое окно ({cl:02d}:00–{op:02d}:00) не оплачивается: {'да' if un else 'нет'}",
                parse_mode="Markdown")

        elif action == "schedule_save":
            # Owner: haftalık график'i kaydet (meta blob, hafta=Pazartesi tarihi)
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец.")
                return
            wk = (data.get("week") or "").strip()[:10]
            sdata = data.get("data")
            if wk and sdata is not None:
                db.execute("INSERT OR REPLACE INTO meta (k,val) VALUES (?,?)",
                           (f"sched_{wk}", json.dumps(sdata, ensure_ascii=False)))
                db.commit()

        elif action == "schedule_image":
            # Owner: uygulamada çizilen график resmini (base64 PNG) şube gruplarına gönder
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец.")
                return
            b64 = data.get("png") or ""
            if "," in b64:
                b64 = b64.split(",", 1)[1]
            try:
                raw = base64.b64decode(b64)
            except Exception:
                raw = b""
            if not raw:
                await update.message.reply_text("❌ Не удалось создать изображение.")
                return
            cap = (data.get("caption") or "📅 График смен").strip()[:900]
            sent_to = set()
            for b in get_branches(db, only_active=True):
                g = b.get("group_chat_id")
                if g and str(g) not in sent_to:
                    sent_to.add(str(g))
                    try:
                        bio = io.BytesIO(raw); bio.name = "grafik.png"
                        await context.bot.send_photo(chat_id=int(g), photo=bio, caption=cap)
                    except Exception as e:
                        logger.warning(f"schedule_image send {g}: {e}")
            if not sent_to:
                try:
                    bio = io.BytesIO(raw); bio.name = "grafik.png"
                    await context.bot.send_photo(chat_id=user.id, photo=bio, caption=cap + "\n(нет привязанных групп)")
                except Exception:
                    pass
            await update.message.reply_text(
                f"✅ График отправлен: {len(sent_to)} груп(пы)." if sent_to
                else "⚠️ Нет привязанных групп — отправил вам в личку.")

        elif action == "create_branch":
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец.")
                return
            bname = (data.get("name") or "").strip()[:40]
            if not bname:
                await update.message.reply_text("❌ Укажите название филиала.")
                return
            mx = db.execute("SELECT COALESCE(MAX(sort_order),0) AS m FROM branches").fetchone()["m"]
            cur = db.execute(
                "INSERT INTO branches (name, group_chat_id, sort_order, active, created_at) "
                "VALUES (?,?,?,1,?)", (bname, None, (mx or 0) + 1, now.isoformat()))
            db.commit()
            log_action(db, "create_branch", user.id, user.first_name, None, None,
                       {"id": cur.lastrowid, "name": bname})
            await update.message.reply_text(
                f"🏢 Филиал добавлен: *{md_safe(bname)}*\n\n"
                f"Чтобы отчёты уходили в нужную группу — выберите этот филиал в приложении "
                f"и напишите /setgroup в его Telegram-группе.",
                parse_mode="Markdown")

        elif action == "update_branch":
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец.")
                return
            try:
                bid = int(data.get("branch_id") or 0)
            except Exception:
                bid = 0
            b = get_branch(db, bid)
            if not b:
                await update.message.reply_text("❌ Филиал не найден.")
                return
            sets, params = [], []
            if data.get("name") is not None:
                nm = (data.get("name") or "").strip()[:40]
                if nm:
                    sets.append("name=?"); params.append(nm)
            if data.get("active") is not None:
                sets.append("active=?"); params.append(1 if int(data.get("active") or 0) else 0)
            if not sets:
                return
            params.append(bid)
            db.execute(f"UPDATE branches SET {', '.join(sets)} WHERE id=?", params)
            db.commit()
            log_action(db, "update_branch", user.id, user.first_name, None, None,
                       {"id": bid, "fields": {k: data.get(k) for k in ("name", "active") if data.get(k) is not None}})
            await update.message.reply_text("✅ Филиал обновлён.")

        elif action == "assign_branch":
            # Bir baristanın ev şubesini değiştir (owner)
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец.")
                return
            try:
                tid = int(data.get("target") or 0)
                bid = int(data.get("branch_id") or 0)
            except Exception:
                tid = bid = 0
            if not tid or not get_branch(db, bid):
                await update.message.reply_text("❌ Неверные данные.")
                return
            db.execute("UPDATE users SET branch_id=? WHERE user_id=?", (bid, tid))
            db.commit()
            log_action(db, "assign_branch", user.id, user.first_name, tid,
                       display_name_for(db, tid), {"branch_id": bid})

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
                f"Бариста уже получил(а) его в личке. Также можете передать вручную:\n`/login {new_pwd}`",
                parse_mode="Markdown")
            # Baristaya kendi şifresini DM gönder (owner elle iletmek zorunda kalmasın)
            try:
                await context.bot.send_message(
                    target_id,
                    f"🔐 *Ваш код для входа: {new_pwd}*\n\n"
                    f"Откройте приложение Caffelito и введите *{new_pwd}* на экране входа.\n"
                    f"(или отправьте боту `/login {new_pwd}`)",
                    parse_mode="Markdown")
            except Exception as e:
                logger.warning(f"password DM to {target_id} failed: {e}")
                await update.message.reply_text(
                    "⚠️ Не удалось отправить пароль бариста в личку (возможно, он(а) не запускал(а) бота). Передайте вручную.")

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
        elif action == "approve_user":
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец.")
                return
            target_id = int(data.get("target", 0) or 0)
            row = db.execute("SELECT * FROM users WHERE user_id=?", (target_id,)).fetchone()
            if not row:
                await update.message.reply_text("❌ Пользователь не найден.")
                return
            shown_t = row["display_name"] or row["name"] or "?"
            db.execute("UPDATE users SET approved=1, archived=0 WHERE user_id=?", (target_id,))
            db.commit()
            log_action(db, "approve_user", user.id, user.first_name, target_id, shown_t, {})
            try:
                await context.bot.send_message(
                    target_id, "✅ Вас добавили в команду Caffelito! Владелец выдаст пароль для входа.")
            except Exception:
                pass
            await update.message.reply_text(
                f"✅ *{md_safe(shown_t)}* принят(а). Задайте пароль в «Люди».", parse_mode="Markdown")

        elif action == "reject_user":
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец.")
                return
            target_id = int(data.get("target", 0) or 0)
            row = db.execute("SELECT * FROM users WHERE user_id=?", (target_id,)).fetchone()
            if not row or (row["role"] or "") == "owner":
                await update.message.reply_text("❌ Неверный пользователь.")
                return
            shown_t = row["name"] or "?"
            db.execute("DELETE FROM users WHERE user_id=? AND COALESCE(approved,0)=0", (target_id,))
            db.commit()
            log_action(db, "reject_user", user.id, user.first_name, target_id, shown_t, {})
            await update.message.reply_text(f"🗑 Заявка отклонена.")

        elif action == "unapprove_user":
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец.")
                return
            target_id = int(data.get("target", 0) or 0)
            row = db.execute("SELECT * FROM users WHERE user_id=?", (target_id,)).fetchone()
            if not row or (row["role"] or "") == "owner":
                await update.message.reply_text("❌ Нельзя.")
                return
            shown_t = row["display_name"] or row["name"] or "?"
            db.execute("UPDATE users SET approved=0, authorized=0 WHERE user_id=?", (target_id,))
            db.commit()
            log_action(db, "unapprove_user", user.id, user.first_name, target_id, shown_t, {})
            await update.message.reply_text(f"↩️ *{md_safe(shown_t)}* возвращён(а) в заявки.", parse_mode="Markdown")

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

        # ─── Geçmişi temizle (sadece owner): tüm veya belirli tarihe kadar ───
        elif action == "clear_history":
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец может очищать историю.")
            else:
                mode = data.get("mode", "all")
                # Seçilebilir türler → (tablo, tarih-kolonu, etiket, branch-filtre-kolonu)
                # branch-filtre-kolonu: ('branch_id', None)=doğrudan şube · (None,'user_id')=personel (şube üyesi)
                TMAP = {
                    "shifts": ("shifts", "date", "Смен", ("branch_id", None)),
                    "orders": ("orders", "created_at", "Заказов", ("branch_id", None)),
                    "cash": ("cashreports", "created_at", "Кассовых отчётов", ("branch_id", None)),
                    "fines": ("fines", "created_at", "Штрафов", (None, "user_id")),
                    "tips": ("tips", "created_at", "Чаевых", (None, "user_id")),
                    "payments": ("payments", "paid_at", "Выплат", (None, "user_id")),
                    "loans": ("loans", "created_at", "Авансов", (None, "barista_id")),
                }
                sel = data.get("types") or list(TMAP.keys())  # boşsa hepsi (eski uyumluluk)
                try:
                    bid = int(data.get("branch_id") or 0)
                except Exception:
                    bid = 0
                if bid and not get_branch(db, bid):
                    bid = 0
                d = (data.get("date") or "").strip() if mode == "before" else ""
                if mode == "before" and not d:
                    await update.message.reply_text("❌ Дата не указана.")
                else:
                    parts_msg = []
                    for key in sel:
                        if key not in TMAP:
                            continue
                        tbl, col, lbl, (bcol, ucol) = TMAP[key]
                        conds, params = [], []
                        if mode == "before":
                            conds.append(f"{col} < ?"); params.append(d)
                        if bid:
                            if bcol:
                                conds.append(f"COALESCE({bcol},1)=?"); params.append(bid)
                            elif ucol:
                                conds.append(f"{ucol} IN (SELECT user_id FROM users WHERE COALESCE(branch_id,1)=?)"); params.append(bid)
                        where = (" WHERE " + " AND ".join(conds)) if conds else ""
                        try:
                            n = db.execute(f"DELETE FROM {tbl}{where}", params).rowcount
                            parts_msg.append(f"{lbl}: {n}")
                        except Exception as ex:
                            logger.warning(f"clear {tbl} failed: {ex}")
                    db.commit()
                    br_lbl = ("" if not bid else f" · {(get_branch(db, bid) or {}).get('name','?')}")
                    head = (f"🗑 *Удалено до {d}{br_lbl}:*" if mode == "before" else f"🗑 *Удалено{br_lbl}:*")
                    await update.message.reply_text(head + "\n" + " · ".join(parts_msg), parse_mode="Markdown")
                    await refresh_webapp_keyboard(update, context, db, user, "🔄 История обновлена 👇")

        # ─── Kasa / Сменный отчёт (vardiya kapanış raporu) ───
        elif action == "cash_report":
            from html import escape as esc_html
            cups = data.get("cups", [])  # [{n,b,r,o,s}]
            itg = int(data.get("itogo", 0) or 0)
            clk = int(data.get("click", 0) or 0)
            pay = int(data.get("payme", 0) or 0)
            kar = int(data.get("karta", 0) or 0)
            term = int(data.get("terminal", 0) or 0)
            vsh = int(data.get("vyshlo", 0) or 0)
            sdachi = int(data.get("na_sdachi", 0) or 0)
            exps = data.get("expenses", [])  # [{n,a}]
            note = (data.get("note") or "").strip()
            daily_pay = int(data.get("daily_pay", 0) or 0)  # günlük bonus (satılan bardak) — kasadan alınır
            cashless = clk + pay + kar + term
            schitano = itg - cashless
            exp_total = sum(int(e.get("a", 0) or 0) for e in exps)
            kassa = vsh - sdachi - daily_pay
            cups_total = sum(int(c.get("s", 0) or 0) for c in cups)
            # Günlük bonus kasadan alındı → aylık maaşta çift sayılmasın diye 'ödendi' kaydet
            if daily_pay > 0:
                db.execute(
                    "INSERT INTO payments (user_id, amount, period, paid_by, paid_by_name, paid_at) VALUES (?,?,?,?,?,?)",
                    (user.id, daily_pay, now.strftime("%Y-%m"), user.id, user.first_name, now.isoformat()))
            ostalos = {str(c.get("n", "")): int(c.get("o", 0) or 0) for c in cups}
            shift_hours = float(data.get("hours", 0) or 0)
            shift_start = (data.get("start_time") or "")
            shift_end = (data.get("end_time") or "")
            coffee_kg = float(data.get("coffee_kg", 0) or 0)  # kalan kahve çekirdeği (kg) — stok uyarısı için
            _cr_branch = acting_branch_id(db, user.id)
            db.execute(
                "INSERT INTO cashreports (user_id,user_name,date,period,created_at,bylo,restock,ostalos,sold,cups_total,itogo,click,payme,karta,terminal,cashless,schitano,vyshlo,na_sdachi,kassa,expenses,expenses_total,note,daily_pay,hours,start_time,end_time,coffee_kg,branch_id) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (user.id, user.first_name, now.strftime("%Y-%m-%d"), now.strftime("%Y-%m"), now.isoformat(),
                 json.dumps({str(c.get("n","")): int(c.get("b",0) or 0) for c in cups}, ensure_ascii=False),
                 json.dumps({str(c.get("n","")): int(c.get("r",0) or 0) for c in cups}, ensure_ascii=False),
                 json.dumps(ostalos, ensure_ascii=False),
                 json.dumps({str(c.get("n","")): int(c.get("s",0) or 0) for c in cups}, ensure_ascii=False),
                 cups_total, itg, clk, pay, kar, term, cashless, schitano, vsh, sdachi, kassa,
                 json.dumps(exps, ensure_ascii=False), exp_total, note, daily_pay, shift_hours, shift_start, shift_end, coffee_kg, _cr_branch))
            db.commit()
            if group_id:
                try:
                    t = "<b>📋 СМЕННЫЙ ОТЧЁТ — CAFFELITO</b>\n"
                    t += "━━━━━━━━━━━━━━━━━━━━\n"
                    t += f"<b>{esc_html(shown)}</b> · {now.strftime('%d.%m.%Y %H:%M')}\n"
                    t += "━━━━━━━━━━━━━━━━━━━━\n"
                    t += "<b>🥤 Стаканы (было → осталось = продано)</b>\n"
                    for c in cups:
                        b = int(c.get("b", 0) or 0); r = int(c.get("r", 0) or 0)
                        o = int(c.get("o", 0) or 0); s = int(c.get("s", 0) or 0)
                        if b or r or o or s:
                            rtxt = (f" +{r}" if r > 0 else f" {r}") if r else ""
                            t += f"  <b>{esc_html(str(c.get('n','')))}:    {b}{rtxt} → {o} = {s}</b>\n"
                    t += f"\n  <b>🧮 ИТОГО ПРОДАНО:  {cups_total} шт</b>\n"
                    # POS para mutabakatı KALDIRILDI (onu POS yapıyor) — sadece расходы + günlük bonus
                    if exps:
                        t += "━━━━━━━━━━━━━━━━━━━━\n<b>💸 Расходы</b>\n"
                        for e in exps:
                            t += f"  <b>{esc_html(str(e.get('n','')))}: {fmt_sum(int(e.get('a',0) or 0))}</b>\n"
                        t += f"  <b>Итого расходы: {fmt_sum(exp_total)} сум</b>\n"
                    if daily_pay:
                        t += "━━━━━━━━━━━━━━━━━━━━\n"
                        t += f"<b>💵 Дневной бонус: {fmt_sum(daily_pay)} сум</b>"
                    if note:
                        t += f"\n📝 {esc_html(note)}"
                    # HEMEN gönderme — önce 'закрыл смену', SONRA bu отчёт gelsin diye buffer'la (shift_end gönderir)
                    context.bot_data.setdefault("pending_report", {})[user.id] = t
                except Exception as e:
                    logger.error(f"KASA report buffer failed: {e}")
                # ─── Stok uyarısı (kalan bardak) — gruba, herkese ───
                try:
                    from html import escape as esc_html2
                    low = []  # (urgency, name, qty)
                    for c in cups:
                        b = int(c.get("b", 0) or 0) + int(c.get("r", 0) or 0)  # завоз/передача dahil efektif başlangıç
                        o = int(c.get("o", 0) or 0)
                        if b <= 0:
                            continue  # stoklanmayan boy
                        if o <= 50:
                            low.append((3, str(c.get("n", "")), o))
                        elif o <= 70:
                            low.append((2, str(c.get("n", "")), o))
                        elif o <= 100:
                            low.append((1, str(c.get("n", "")), o))
                    # Kahve çekirdeği (kg) — eşik ≤3🔴 / ≤5⚠️ / ≤8📦
                    coffee_urg = 0
                    if coffee_kg > 0:
                        coffee_urg = 3 if coffee_kg <= 3 else (2 if coffee_kg <= 5 else (1 if coffee_kg <= 8 else 0))
                    if low or coffee_urg:
                        low.sort(key=lambda x: x[0], reverse=True)
                        max_urg = max([coffee_urg] + [u for u, _, _ in low])
                        head = ("🔴 <b>СКЛАД — ОЧЕНЬ СРОЧНО заказать!</b>" if max_urg == 3
                                else "⚠️ <b>СКЛАД — срочно заказать</b>" if max_urg == 2
                                else "📦 <b>СКЛАД — пора заказать</b>")
                        st = head + "\n━━━━━━━━━━━━━━━━━━━━\n"
                        for urg, nm, q in low:
                            ic = "🔴" if urg == 3 else ("⚠️" if urg == 2 else "📦")
                            st += f"  {ic} {esc_html2(nm)}: осталось <b>{q}</b> шт\n"
                        if coffee_urg:
                            ic = "🔴" if coffee_urg == 3 else ("⚠️" if coffee_urg == 2 else "📦")
                            st += f"  {ic} ☕ Кофе в зёрнах: осталось <b>{coffee_kg:g}</b> кг\n"
                        # HEMEN gönderme — 'закрыл смену' mesajından SONRA, en sonda gelsin (buffer'la)
                        context.bot_data.setdefault("pending_stock", {})[user.id] = (st, now)
                except Exception as e:
                    logger.error(f"STOK alert failed: {e}")
            await refresh_webapp_keyboard(update, context, db, user, "🔄 Касса сдана. Готово 👇")

        # ─── Ступени обслуживания: günlük ознакомление onayı ───
        elif action == "standard_ack":
            today_str = now.strftime("%Y-%m-%d")
            db.execute(
                "INSERT OR IGNORE INTO std_acks (user_id,user_name,date,created_at) VALUES (?,?,?,?)",
                (user.id, shown, today_str, now.isoformat()))
            db.commit()

        # ─── Borç talebi: barista istek gönderir ───
        elif action == "loan_request":
            db = get_db()
            amount = int(data.get("amount", 0) or 0)
            reason = (data.get("reason") or "").strip()
            if amount <= 0 or amount > 5_000_000:
                await update.message.reply_text("❌ Сумма некорректна.")
                return
            if not reason:
                await update.message.reply_text("❌ Укажите причину.")
                return
            # Aynı kullanıcının pending talebi varsa engelle
            existing = db.execute("SELECT id FROM loans WHERE barista_id=? AND status='pending'",
                                  (user.id,)).fetchone()
            if existing:
                await update.message.reply_text("⚠️ У вас уже есть запрос в ожидании.")
                return
            now = datetime.now(TZ).isoformat()
            cur = db.execute(
                "INSERT INTO loans (barista_id, amount, reason, status, created_at) "
                "VALUES (?,?,?,'pending',?)",
                (user.id, amount, reason, now))
            db.commit()
            loan_id = cur.lastrowid
            log_action(db, "loan_request", user.id, user.first_name, user.id, user.first_name,
                       {"amount": amount, "reason": reason})
            shown = display_name_for(db, user.id, fallback=user.first_name)
            # Owner'lara bildir
            owners = db.execute("SELECT user_id FROM users WHERE role='owner'").fetchall()
            for o in owners:
                if o["user_id"] == user.id:
                    continue
                try:
                    kb = InlineKeyboardMarkup([
                        [InlineKeyboardButton("✅ Одобрить", callback_data=f"loan_ok:{loan_id}"),
                         InlineKeyboardButton("❌ Отклонить", callback_data=f"loan_no:{loan_id}")]
                    ])
                    await context.bot.send_message(
                        o["user_id"],
                        f"💸 *Запрос аванса*\n\n"
                        f"От: *{md_safe(shown)}*\n"
                        f"Сумма: *{fmt_sum(amount)}* сум\n"
                        f"Причина: {md_safe(reason)}",
                        parse_mode="Markdown",
                        reply_markup=kb)
                except Exception:
                    pass
            await update.message.reply_text(
                f"✅ Запрос отправлен\n\nСумма: {fmt_sum(amount)} сум\nЖдите решения шефа.")

        # ─── Owner: borç onayla/reddet (webapp üzerinden) ───
        elif action == "loan_decide":
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец.")
                return
            loan_id = int(data.get("loan_id", 0) or 0)
            decision = data.get("decision", "")  # 'approve' or 'reject'
            note = (data.get("note") or "").strip()
            await _decide_loan(context, db, user, loan_id, decision, note,
                               update.message.reply_text)

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

        # ─── Owner: sertifikayı geri al (yanlışlıkla/şüpheli geçen barista) ───
        elif action == "cert_revoke":
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец может отзывать сертификат.")
                return
            target_id = int(data.get("target", 0) or 0)
            if not target_id:
                return
            # Geçmiş başarılı sınavları sıfırla → artık sertifikalı sayılmaz; yeni sınav atanabilir
            db.execute("UPDATE rt_exams SET passed=0 WHERE user_id=? AND passed=1", (target_id,))
            db.execute("UPDATE rt_exam_invites SET status='revoked' WHERE barista_id=? AND status IN ('pending','active')", (target_id,))
            db.commit()
            log_action(db, "cert_revoke", user.id, user.first_name, target_id,
                       display_name_for(db, target_id), {})
            try:
                await context.bot.send_message(
                    target_id,
                    "⚠️ *Сертификат отозван*\n\nВладелец отозвал ваш сертификат Caffelito. "
                    "При необходимости вы сможете пройти сертификационный экзамен заново.",
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
            passed = 1 if score >= 100 else 0  # 1 000 000 prizi için TAM скор şart (sunucu da doğrular)
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
                cert_name = (data.get("name") or "").strip()
                msg = (
                    f"🎓 *Зачёт завершён*\n\n"
                    f"Бариста: *{md_safe(shown)}*\n"
                    + (f"На сертификате: *{md_safe(cert_name)}*\n" if cert_name else "")
                    + f"Результат: *{score}%* ({correct}/{total})\n"
                    f"Статус: {'🏆 Сдан' if passed else '❌ Не сдан'}"
                )
                if passed:
                    msg += "\n\n💰 *Приз: 1 000 000 сум* — выплатите бариста через ✅ Выплатить."
                await context.bot.send_message(inv["owner_id"], msg, parse_mode="Markdown")
            except Exception as e:
                logger.warning(f"exam finish notify failed: {e}")
            # Baristaya tebrik
            try:
                if passed:
                    await update.message.reply_text(
                        f"🏆 *Зачёт сдан!* {score}%\n\n💰 Приз *1 000 000 сум* — шеф скоро выплатит. Молодец!",
                        parse_mode="Markdown")
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
            # NOT: Bu PROBA (Пробный тест / hazırlık) — owner'a bildirim GİTMEZ (spam olmasın).
            # Sertifika + 1 000 000 sadece owner-atamalı resmi sınavdan (exam_finish) gelir.
            # Baristaya sessiz, kısa hazırlık geri bildirimi:
            try:
                await update.message.reply_text(
                    f"📝 Пробный тест: {score}% ({correct}/{total})\n"
                    f"На сертификационном экзамене нужно 100%. Тренируйтесь!")
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


def parse_payment(text):
    """Click/Payme bildirim mesajını çöz → {provider,amount,ok,txid,pay_at} veya None."""
    if not text:
        return None
    low = text.lower()
    if ("сум" not in low) and ("сўм" not in low):
        return None
    if not any(w in low for w in ("успешно", "оплачен", "подтвержд", "отмен", "аннулир")):
        return None
    m = re.search(r'([0-9][0-9\s .,]*)\s*с[уў]м', text)
    if not m:
        return None
    raw = re.sub(r'[\s ]', '', m.group(1))  # boşlukları sil
    intpart = raw[:-3] if (len(raw) > 3 and raw[-3] in ',.') else raw  # kuruş (,00/.00) at
    amount = int(re.sub(r'\D', '', intpart) or 0)
    if amount <= 0:
        return None
    cancelled = ("❌" in text) or ("🔴" in text) or ("отмен" in low) or ("аннулир" in low) or ("возврат" in low)
    ok = (not cancelled) and (("✅" in text) or ("🟢" in text) or ("успешно" in low))
    if ("подтвержд" in low) or ("аннулир" in low) or ("clickuz" in low):
        provider = "click"
    elif ("оплачен" in low) or ("отмен" in low) or ("payme" in low):
        provider = "payme"
    else:
        provider = "?"
    pay_at = ""
    tm = re.search(r'(\d{1,2}:\d{2}:\d{2})\s+(\d{2}\.\d{2}\.\d{4})', text)
    if tm:
        try:
            pay_at = datetime.strptime(tm.group(2) + " " + tm.group(1), "%d.%m.%Y %H:%M:%S").replace(tzinfo=TZ).isoformat()
        except Exception:
            pay_at = ""
    idm = re.search(r'🆔\s*([0-9a-fA-F]+)', text)
    txid = idm.group(1) if idm else ""
    return {"provider": provider, "amount": amount, "ok": 1 if ok else 0, "txid": txid, "pay_at": pay_at}


async def capture_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Click/Payme gruplarındaki bildirimleri yakala + pay_feed'e kaydet (test modunda owner'a DM)."""
    msg = update.effective_message
    if not msg:
        return
    text = msg.text or msg.caption or ""
    chat = update.effective_chat
    p = parse_payment(text)
    if not p:
        # DEBUG: kanal/grup mesajı Nero'ya geliyor mu? (meta pay_debug=1 iken owner'a DM)
        try:
            if chat and chat.type != "private":
                _d = get_db()
                _flag = _d.execute("SELECT val FROM meta WHERE k='pay_debug'").fetchone()
                if _flag and _flag["val"] == "1":
                    for o in _d.execute("SELECT user_id FROM users WHERE role='owner'").fetchall():
                        try:
                            await context.bot.send_message(
                                o["user_id"], f"🐞 получено из [{chat.type}] id `{chat.id}`:\n«{(text or '')[:60]}»",
                                parse_mode="Markdown")
                        except Exception:
                            pass
        except Exception:
            pass
        return
    try:
        db = get_db()
        if not p["txid"]:
            p["txid"] = (p["pay_at"] or "") + "_" + str(p["amount"])  # txid yoksa fallback
        cur = db.execute(
            "INSERT OR IGNORE INTO pay_feed (provider, amount, ok, txid, pay_at, chat_id, chat_title, raw, created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (p["provider"], p["amount"], p["ok"], p["txid"], p["pay_at"],
             chat.id, chat.title or "", text[:500], datetime.now(TZ).isoformat()))
        db.commit()
        if cur.rowcount == 0:
            return  # zaten kaydedildi — tekrar DM atma
        flag = db.execute("SELECT val FROM meta WHERE k='pay_capture_dm'").fetchone()
        dm_on = (flag is None) or (flag["val"] == "1")  # test modunda owner'a DM (varsayılan açık)
        if dm_on:
            st = "✅" if p["ok"] else "❌ отмена"
            for o in db.execute("SELECT user_id FROM users WHERE role='owner'").fetchall():
                try:
                    await context.bot.send_message(
                        o["user_id"],
                        f"🔍 *Захвачено* [{p['provider']}] · {st}\n"
                        f"💰 {p['amount']:,} сум · 🕓 {p['pay_at'][11:19] if p['pay_at'] else '?'}\n"
                        f"💬 «{md_safe(chat.title or '?')}» (id `{chat.id}`)",
                        parse_mode="Markdown")
                except Exception:
                    pass
    except Exception as e:
        logger.warning(f"capture_payment failed: {e}")


async def on_my_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Nero bir gruba/kanala eklenince veya admin olunca owner'a durum DM'i (teşhis)."""
    cm = update.my_chat_member
    if not cm:
        return
    chat = cm.chat
    new_status = cm.new_chat_member.status if cm.new_chat_member else "?"
    try:
        db = get_db()
        for o in db.execute("SELECT user_id FROM users WHERE role='owner'").fetchall():
            try:
                await context.bot.send_message(
                    o["user_id"],
                    f"🤖 *Nero статус изменён*\n"
                    f"Чат: «{md_safe(chat.title or '?')}»\n"
                    f"Тип: *{chat.type}* · ID: `{chat.id}`\n"
                    f"Новый статус Nero: *{new_status}*\n\n"
                    + ("✅ Для чтения сообщений в канале Nero должен быть *администратором*." if chat.type == "channel"
                       else "ℹ️ В группе бот не видит сообщения других ботов (Click/Payme)."),
                    parse_mode="Markdown")
            except Exception:
                pass
    except Exception as e:
        logger.warning(f"on_my_chat_member failed: {e}")


async def cmd_paydebug(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Owner: ödeme yakalama debug DM'ini aç/kapat. /paydebug on|off"""
    db = get_db()
    user = update.effective_user
    if get_role(db, user.id) != "owner":
        return
    arg = (context.args[0].lower() if context.args else "")
    val = "1" if arg == "on" else "0"
    db.execute("INSERT OR REPLACE INTO meta (k,val) VALUES ('pay_debug', ?)", (val,))
    db.commit()
    await update.message.reply_text(f"🐞 Отладка приёма платежей: {'ВКЛ' if val=='1' else 'выкл'}")


def bind_group_to_branch(db, context, branch_id, chat_id):
    """Bir Telegram grubunu (chat_id) bir şubeye bağla. Ana şube ise eski tekil
    grup mekanizmasını da güncelle (geriye dönük fallback)."""
    db.execute("UPDATE branches SET group_chat_id=? WHERE id=?", (str(chat_id), int(branch_id)))
    if int(branch_id) == DEFAULT_BRANCH_ID:
        if context is not None:
            context.bot_data["group_id"] = str(chat_id)
        db.execute("INSERT OR REPLACE INTO meta (k,val) VALUES ('active_group', ?)", (str(chat_id),))
    db.commit()


async def cmd_setgroup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Grubu bir şubeye bağla — grupta /setgroup yaz, çıkan butonlardan şube seç."""
    chat = update.effective_chat
    if chat.type not in ['group', 'supergroup']:
        await update.message.reply_text(
            "❌ Эту команду нужно использовать в ГРУППЕ (добавьте бота в группу филиала), а не в личном чате.")
        return
    db = get_db()
    user = update.effective_user
    _role = get_role(db, user.id)
    # Anonim yönetici (Telegram "Remain Anonymous") → from = GroupAnonymousBot (1087968824)
    # veya mesaj grup adına gönderilir (sender_chat == bu grup). Anonim = grup admini → izin ver.
    msg = update.effective_message
    is_anon = (user and user.id == ANON_ADMIN_ID) or bool(
        getattr(msg, "sender_chat", None) and msg.sender_chat.id == chat.id)
    if _role != "owner" and not is_anon:
        await update.message.reply_text(
            "❌ Привязать группу может только владелец.\n"
            f"Ваш ID: `{user.id}` · роль: `{_role or 'нет'}`\n"
            "_Если вы владелец и видите это — либо вы пишете как «анонимный администратор» "
            "(отключите анонимность в правах админа), либо где-то запущен второй экземпляр бота._",
            parse_mode="Markdown")
        return
    branches = get_branches(db, only_active=True)
    if not branches:
        bind_group_to_branch(db, context, DEFAULT_BRANCH_ID, chat.id)
        await update.message.reply_text(f"✅ Группа привязана.\nID: `{chat.id}`", parse_mode="Markdown")
        return
    if len(branches) == 1:
        b = branches[0]
        bind_group_to_branch(db, context, b["id"], chat.id)
        await update.message.reply_text(
            f"✅ Группа привязана к филиалу *{md_safe(b['name'])}*!\nID: `{chat.id}`", parse_mode="Markdown")
        return
    # Birden fazla şube → owner butondan seçsin (app'te önceden seçmeye gerek yok)
    kb = [[InlineKeyboardButton(
        ("✅ " if str(b.get("group_chat_id") or "") == str(chat.id) else "🏢 ") + b["name"],
        callback_data=f"setgrp:{b['id']}")] for b in branches]
    await update.message.reply_text(
        f"🏢 *К какому филиалу привязать эту группу?*\n"
        f"Отчёты выбранного филиала будут приходить сюда.\nID: `{chat.id}`",
        reply_markup=InlineKeyboardMarkup(kb), parse_mode="Markdown")


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
        db.execute("UPDATE users SET role='owner', approved=1 WHERE user_id=?", (user.id,))
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
    group_id = context.bot_data.get("group_id") or GROUP_CHAT_ID or "не задан"
    await update.message.reply_text(
        f"ℹ️ *Информация:*\n\n"
        f"Этот чат ID: `{chat.id}`\n"
        f"Тип: {chat.type}\n"
        f"GROUP\\_CHAT\\_ID: `{group_id}`",
        parse_mode="Markdown")


async def cmd_test(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Test: gruba mesaj göndermeyi dene"""
    group_id = context.bot_data.get("group_id") or GROUP_CHAT_ID
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


async def payment_reminder_loop(app):
    """Her gün kontrol: Railway ödemesinden (PAY_DAY=14) PAY_REMIND_BEFORE=3 gün önce
    (ayın 11'i) owner'lara DM hatırlatma. Ayda bir gönderilir (meta ile takip)."""
    await asyncio.sleep(20)
    pay_day = int(os.getenv("PAY_DAY", "14") or 14)
    before = int(os.getenv("PAY_REMIND_BEFORE", "3") or 3)
    remind_day = max(1, pay_day - before)
    while True:
        try:
            now = datetime.now(TZ)
            if now.day == remind_day:
                db = get_db()
                cur = now.strftime("%Y-%m")
                row = db.execute("SELECT val FROM meta WHERE k='pay_reminder'").fetchone()
                if not row or row["val"] != cur:
                    owners = db.execute("SELECT user_id FROM users WHERE role='owner'").fetchall()
                    msg = (f"💳 *Напоминание об оплате*\n\n"
                           f"Через {before} дня ({pay_day}-го числа) — оплата хостинга Nero (Railway, ~5$).\n"
                           f"Пополните карту, чтобы бот не отключился. 🙏")
                    for o in owners:
                        try:
                            await app.bot.send_message(o["user_id"], msg, parse_mode="Markdown")
                        except Exception as e:
                            logger.warning(f"pay reminder dm {o['user_id']}: {e}")
                    db.execute("INSERT OR REPLACE INTO meta (k,val) VALUES ('pay_reminder',?)", (cur,))
                    db.commit()
                    logger.info("Ödeme hatırlatması gönderildi")
        except Exception as e:
            logger.warning(f"payment_reminder_loop: {e}")
        await asyncio.sleep(3600)  # her saat kontrol


async def scheduled_orders_loop(app):
    """Zamanı gelen zamanlı siparişleri gruba gönderir (her 60 sn kontrol)."""
    import html as _html
    await asyncio.sleep(25)
    while True:
        try:
            db = get_db()
            now_iso = datetime.now(TZ).replace(tzinfo=None).isoformat()
            due = db.execute(
                "SELECT * FROM scheduled_orders WHERE COALESCE(sent,0)=0 AND COALESCE(canceled,0)=0 "
                "AND send_at<=? ORDER BY id", (now_iso,)).fetchall()
            for so in due:
                gid = so["group_id"] or branch_group_id(db, so["branch_id"]) or ""
                esc_lines = (so["body"] or "").split("\n")
                total = so["total"] or 0
                shown = so["user_name"] or "?"
                sent_now = datetime.now(TZ)
                header = ("<b>ЗАКАЗ — CAFFELITO</b>\n"
                          f"<b>{_html.escape(str(shown))}</b> · {sent_now.strftime('%d.%m.%Y %H:%M')} ⏰\n")
                footer = f"<b>Итого: {total} позиций</b>"
                try:
                    if gid:
                        await deliver_order(app.bot, gid, header, esc_lines, footer)
                    db.execute(
                        "INSERT INTO orders (chat_id,user_id,user_name,items,created_at,branch_id) VALUES (?,?,?,?,?,?)",
                        (int(gid) if gid else 0, so["user_id"], shown, so["items"] or "[]",
                         sent_now.isoformat(), so["branch_id"] or 1))
                    db.execute("UPDATE scheduled_orders SET sent=1 WHERE id=?", (so["id"],))
                    db.commit()
                    try:
                        await app.bot.send_message(so["user_id"], "⏰ Ваш запланированный заказ отправлен в группу.")
                    except Exception:
                        pass
                    logger.info(f"scheduled order {so['id']} sent")
                except Exception as e:
                    logger.warning(f"scheduled order {so['id']} send failed: {e}")
        except Exception as e:
            logger.warning(f"scheduled_orders_loop: {e}")
        await asyncio.sleep(60)


async def setup_commands(app):
    """Default komut listesi — barista minimali. Owner'lar per-chat override alır."""
    await app.bot.set_my_commands(BARISTA_COMMANDS, scope=BotCommandScopeDefault())
    # Kayıtlı aktif grup (test/şube için /setgroup ile değiştirilebilir) — restart'ta yüklensin
    try:
        _db = get_db()
        _ag = _db.execute("SELECT val FROM meta WHERE k='active_group'").fetchone()
        if _ag and _ag["val"]:
            app.bot_data["group_id"] = _ag["val"]
            logger.info(f"active_group yüklendi: {_ag['val']}")
    except Exception as e:
        logger.warning(f"active_group load failed: {e}")
    # Yol B: Mini App'i + API'yi sunan HTTP sunucusunu başlat
    await start_web_server(app)
    # Ödeme hatırlatma arka plan görevi
    asyncio.create_task(payment_reminder_loop(app))
    asyncio.create_task(scheduled_orders_loop(app))


# ═══════════════════════════════════════════════════════════════════
#  YOL B — HTTP BACKEND (Mini App'i ana ekrandan açabilmek için)
#  Telegram kuralı: tg.sendData() sadece klavye-butonu akışında çalışır.
#  Ana ekran/link ile açılınca veri ancak HTTP + initData imzasıyla
#  güvenli şekilde gönderilebilir. Bu blok onu sağlar.
# ═══════════════════════════════════════════════════════════════════

def validate_init_data(init_data: str):
    """Telegram WebApp initData imzasını doğrular. Geçerliyse user dict döner, değilse None.
    Algoritma: secret = HMAC_SHA256('WebAppData', bot_token);
               hash   = HMAC_SHA256(secret, data_check_string)."""
    try:
        if not init_data:
            return None
        pairs = dict(parse_qsl(init_data, keep_blank_values=True))
        recv_hash = pairs.pop("hash", None)
        if not recv_hash:
            return None
        data_check = "\n".join(f"{k}={pairs[k]}" for k in sorted(pairs))
        secret = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
        calc = hmac.new(secret, data_check.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(calc, recv_hash):
            return None
        return json.loads(pairs.get("user", "{}"))
    except Exception as e:
        logger.warning(f"initData validation error: {e}")
        return None


# ─── handle_webapp_data'yı değiştirmeden HTTP'den çağırabilmek için shim ───
class _ShimUser:
    def __init__(self, uid, first_name, username):
        self.id = uid
        self.first_name = first_name or "Бариста"
        self.username = username
        self.full_name = first_name or "Бариста"


class _ShimChat:
    def __init__(self, uid):
        self.id = uid
        self.type = "private"


class _ShimWebAppData:
    def __init__(self, data):
        self.data = data


class _ShimMessage:
    def __init__(self, bot, chat_id, data):
        self._bot = bot
        self._chat_id = chat_id
        self.web_app_data = _ShimWebAppData(data)

    async def reply_text(self, text, **kwargs):
        # reply_text → kullanıcının özel sohbetine normal mesaj
        kwargs.pop("reply_to_message_id", None)
        kwargs.pop("quote", None)
        kwargs.pop("do_quote", None)
        try:
            return await self._bot.send_message(chat_id=self._chat_id, text=text, **kwargs)
        except Exception as e:
            logger.warning(f"shim reply_text failed: {e}")
            return None


class _ShimUpdate:
    def __init__(self, bot, uid, first_name, username, data):
        self.effective_user = _ShimUser(uid, first_name, username)
        self.effective_chat = _ShimChat(uid)
        self.message = _ShimMessage(bot, uid, data)
        self.effective_message = self.message


class _ShimContext:
    def __init__(self, bot, bot_data):
        self.bot = bot
        self.bot_data = bot_data


def _cors(resp):
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return resp


def _nocache(resp):
    """Telegram/WebView eski sayfayı/state'i önbellekte tutmasın (rol değişince takılmasın)."""
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp


async def web_index(request):
    """Mini App HTML'ini sun (aynı origin → CORS yok, hash gerekmez)."""
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "index.html")
    try:
        with open(path, "r", encoding="utf-8") as f:
            html = f.read()
        return _nocache(_cors(web.Response(text=html, content_type="text/html")))
    except Exception as e:
        return web.Response(text=f"index.html bulunamadı: {e}", status=500)


async def web_image(request):
    """nero.jpg gibi yerel görselleri sun."""
    fname = request.match_info.get("fname", "")
    if fname not in ("nero.jpg", "sertifikat.png"):
        return web.Response(status=404)
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), fname)
    if not os.path.exists(path):
        return web.Response(status=404)
    return web.FileResponse(path)


async def web_health(request):
    return web.Response(text="ok")


def _app_build():
    """index.html'deki APP_BUILD sayısını oku (tek kaynak — client kendi sürümüyle karşılaştırır)."""
    try:
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "index.html")
        with open(path, "r", encoding="utf-8") as f:
            txt = f.read()
        m = re.search(r"APP_BUILD\s*=\s*(\d+)", txt)
        if m:
            return m.group(1)
    except Exception as e:
        logger.warning(f"_app_build failed: {e}")
    return "0"


async def web_ver(request):
    """Güncel build sürümünü döndür — client cache'li eskiyse kendini yeniler."""
    return _nocache(_cors(web.Response(text=_app_build(), content_type="text/plain")))


async def web_options(request):
    return _cors(web.Response(text=""))


async def _read_json(request):
    """Gövdeyi içerik tipinden bağımsız oku (text/plain de olabilir → CORS preflight'ı atlamak için)."""
    try:
        raw = await request.text()
        return json.loads(raw) if raw else {}
    except Exception:
        return None


async def api_state(request):
    """POST {initData} → kullanıcının state'ini hash-payload formatında döner.
    İstemci bunu location.hash'e yazar; mevcut JS değişmeden okur."""
    body = await _read_json(request)
    if body is None:
        return _cors(web.json_response({"error": "bad json"}, status=400))
    user = validate_init_data(body.get("initData", ""))
    if not user:
        return _cors(web.json_response({"error": "unauthorized"}, status=403))
    db = get_db()
    try:
        payload = build_hash_payload(db, user["id"], user.get("first_name", "Бариста"))
    except Exception as e:
        # ÖNEMLİ: build patlarsa bile owner kilitlenmesin — en azından rol+isim dönsün.
        logger.error(f"build_hash_payload failed for {user.get('id')}: {e}")
        from urllib.parse import quote as _q
        try:
            _role = get_role(db, user["id"])
        except Exception:
            _role = "barista"
        payload = f"uid={user['id']}&role={_role}&name={_q(user.get('first_name','') or '')}&std_ack=0"
    return _nocache(_cors(web.Response(text=payload, content_type="text/plain")))


async def api_action(request):
    """POST {initData, data} → sendData ile aynı işi yapar (sipariş/vardiya vb.).
    initData imzası doğrulanır, sonra handle_webapp_data değiştirilmeden çağrılır."""
    body = await _read_json(request)
    if body is None:
        return _cors(web.json_response({"error": "bad json"}, status=400))
    user = validate_init_data(body.get("initData", ""))
    if not user:
        return _cors(web.json_response({"error": "unauthorized"}, status=403))
    data_str = body.get("data")
    if not data_str:
        return _cors(web.json_response({"error": "no data"}, status=400))
    tg_app = request.app["tg_app"]
    shim_update = _ShimUpdate(
        tg_app.bot, user["id"], user.get("first_name", "Бариста"),
        user.get("username"), data_str)
    shim_context = _ShimContext(tg_app.bot, tg_app.bot_data)
    try:
        await handle_webapp_data(shim_update, shim_context)
    except Exception as e:
        logger.error(f"api_action handle_webapp_data failed: {e}")
        return _cors(web.json_response({"error": str(e)}, status=500))
    return _cors(web.json_response({"ok": True}))


async def api_admin(request):
    """Geçici owner-only bakım ucu: scan / açık vardiyaları kapat (endshifts)."""
    body = await _read_json(request)
    if body is None:
        return _cors(web.json_response({"error": "bad json"}, status=400))
    user = validate_init_data(body.get("initData", ""))
    if not user:
        return _cors(web.json_response({"error": "unauthorized"}, status=403))
    db = get_db()
    if get_role(db, user["id"]) != "owner":
        return _cors(web.json_response({"error": "owner only"}, status=403))
    op = body.get("op", "scan")
    out = {"op": op}
    try:
        open_sh = db.execute("SELECT id,user_id,start_time FROM shifts WHERE start_time IS NOT NULL AND end_time IS NULL ORDER BY id").fetchall()
        out["open_shifts"] = [dict(s) for s in open_sh]
    except Exception as e:
        out["open_shifts"] = f"hata: {e}"
    if op == "endshifts":
        now = datetime.now(TZ).replace(tzinfo=None).isoformat()
        n = db.execute("UPDATE shifts SET end_time=?, hours=0, total=0, bonus=0, hourly_pay=0 "
                       "WHERE start_time IS NOT NULL AND end_time IS NULL", (now,)).rowcount
        db.commit()
        out["closed"] = n
    return _cors(web.json_response(out))


async def start_web_server(app):
    web_app = web.Application()
    web_app["tg_app"] = app
    web_app.add_routes([
        web.get("/", web_index),
        web.get("/index.html", web_index),
        web.get("/health", web_health),
        web.get("/api/ver", web_ver),
        web.get("/{fname:.+\\.jpg}", web_image),
        web.get("/{fname:.+\\.png}", web_image),
        web.post("/api/state", api_state),
        web.post("/api/action", api_action),
        web.post("/api/admin", api_admin),
        web.options("/api/state", web_options),
        web.options("/api/action", web_options),
        web.options("/api/admin", web_options),
    ])
    runner = web.AppRunner(web_app)
    await runner.setup()
    port = int(os.getenv("PORT", "8080"))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    app.bot_data["_web_runner"] = runner
    logger.info(f"🌐 Web sunucusu açıldı: 0.0.0.0:{port}")


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
    # Click/Payme yakalama İPTAL edildi (notifier'lar bot, grup bot-to-bot engeli, admin gerekiyor).
    # Handler'lar kayıt edilmiyor → capture/paydebug/my_chat_member çalışmaz.
    print("☕ Caffelito Bot запущен!")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
