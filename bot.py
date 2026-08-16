"""
CAFFELITO TELEGRAM BOT ☕
Заказ, Задачи, Уборка и ОКК контроль
"""

import json, os, logging, sqlite3, hmac, hashlib, asyncio, re, io, base64, tempfile
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
# ─── Nero kademeli açılış (flags.json ile yönlendirme) ───
# NERO_WEBAPP_URL artık SÜRÜM SEÇMEZ (bkz. nero_app_url): Nero hep /app.
NERO_FLAGS_URL  = os.getenv("NERO_FLAGS_URL", "")
NERO_WEBAPP_URL = os.getenv("NERO_WEBAPP_URL", "")
_nero_cache = {"cfg": None, "at": 0.0}
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
                     ("desserts", "TEXT"), ("dessert_bonus", "INTEGER DEFAULT 0"),
                     ("overtime", "INTEGER DEFAULT 0"), ("overtime_h", "REAL DEFAULT 0")]:
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
    # ─── Manuel bakiye düzeltmeleri (Корректировка) — owner mutabakat aracı (+/-) ───
    db.execute("""CREATE TABLE IF NOT EXISTS adjustments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER, amount INTEGER, note TEXT, period TEXT,
        branch_id INTEGER, added_by INTEGER, added_by_name TEXT,
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
    # edits: owner'ın sonradan yaptığı düzeltmelerin APPEND-ONLY geçmişi (JSON dizi).
    #        Her giriş {at, by, by_name, ch:{alan:[eski,yeni]}} — eski değer ASLA silinmez.
    for _col, _typ in (("daily_pay", "INTEGER"), ("hours", "REAL"),
                       ("start_time", "TEXT"), ("end_time", "TEXT"),
                       ("coffee_kg", "REAL"),
                       ("edits", "TEXT"), ("edited_at", "TEXT"),
                       ("edited_by", "INTEGER"), ("edited_by_name", "TEXT")):
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
    # ─── Cihaz kaydı (Device ID) — Nero erişiminin İKİNCİ kapısı ───
    # `nero_access_ok` KİMLİK kapısıdır (kim girebilir); bu CİHAZ kapısıdır
    # (hangi telefondan/tabletten girebilir). Kural TOFU: kişinin İLK cihazı
    # sessizce güvenilir sayılır — mevcut kimse kilitlenmesin — sonraki her
    # cihaz owner onayı bekler. Owner asla engellenmez (onayı o veriyor).
    db.execute("""CREATE TABLE IF NOT EXISTS devices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER, device_id TEXT,
        label TEXT, platform TEXT,
        approved INTEGER DEFAULT 0, revoked INTEGER DEFAULT 0,
        first_seen TEXT, last_seen TEXT, seen_count INTEGER DEFAULT 0,
        UNIQUE(user_id, device_id))""")
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
    # ─── График (haftalık vardiya planı) — Nero owner atamaları ───
    # week_key = o haftanın PAZARTESİ tarihi (YYYY-MM-DD) → göreli «week» offset'i
    # mutlak tarihe çevrilir (week 0 bugün ≠ week 0 gelecek hafta). day = 0 Пн … 6 Вс.
    # code = vardiya şablon anahtarı (ör. "c5m") veya "off" (выходной).
    db.execute("""CREATE TABLE IF NOT EXISTS shift_grid (
        week_key TEXT, day INTEGER, user_id INTEGER,
        code TEXT, updated_by INTEGER, updated_by_name TEXT, updated_at TEXT,
        PRIMARY KEY (week_key, day, user_id))""")
    # ─── Выходной заявкаları (barista → owner onayı) ───
    db.execute("""CREATE TABLE IF NOT EXISTS dayoff_requests (
        id INTEGER PRIMARY KEY,
        user_id INTEGER, week_key TEXT, day INTEGER, note TEXT,
        status TEXT DEFAULT 'pending',
        decided_by INTEGER, decided_by_name TEXT, decided_at TEXT, created_at TEXT)""")
    # ─── Vardiya ŞABLONLARI (owner tanımlar) ───
    # Eskiden şablonlar (c5d/c5n/mgd…) YALNIZCA uygulamanın içinde sabitti: owner
    # yenisini ekleyemiyor, saatini değiştiremiyordu ve uygulama yenilenince plan
    # kodları anlamını kaybediyordu. Artık kalıcı.
    db.execute("""CREATE TABLE IF NOT EXISTS shift_templates (
        code TEXT PRIMARY KEY,
        branch_id INTEGER, start_t TEXT, end_t TEXT,
        active INTEGER DEFAULT 1, sort_order INTEGER DEFAULT 0,
        updated_by INTEGER, updated_at TEXT)""")
    # ─── AÇIK VARDİYALAR (kimse atanmamış) ───
    # İzin onaylanınca ya da owner elle açınca oluşur. Barista talip olur
    # (claimed), owner onaylar (done) → plana yazılır. Hiçbir devir owner
    # onayı olmadan kesinleşmez.
    db.execute("""CREATE TABLE IF NOT EXISTS open_shifts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        week_key TEXT, day INTEGER, code TEXT, branch_id INTEGER,
        from_uid INTEGER, from_name TEXT,
        status TEXT DEFAULT 'open',
        claim_uid INTEGER, claim_name TEXT, claim_at TEXT,
        decided_by INTEGER, decided_by_name TEXT, decided_at TEXT,
        reason TEXT, created_at TEXT)""")
    # Şube başına GÜNLÜK MAKSİMUM ÇALIŞAN — plana bu sayıdan fazlası atanamaz.
    try:
        db.execute("ALTER TABLE branches ADD COLUMN max_staff INTEGER DEFAULT 2")
    except sqlite3.OperationalError:
        pass
    # Kişiye özel haftalık izin kotası (NULL = genel ayar) ve kendi izin koyma hakkı.
    for _uc, _ut in (("off_limit", "INTEGER"), ("off_self", "INTEGER")):
        try:
            db.execute(f"ALTER TABLE users ADD COLUMN {_uc} {_ut}")
        except sqlite3.OperationalError:
            pass
    # ŞABLON GÖÇÜ — ZORUNLU. Mevcut plandaki hücreler «c5d/c5n/mgd/mgn/mgf»
    # kodlarını kullanıyor; bu kodlar bugüne kadar yalnızca uygulamanın içinde
    # sabitti. Tohumlamazsak var olan bütün plan «böyle bir şablon yok» sayılır
    # ve tek bir hücre bile düzenlenemez. Şubeye ADIYLA bağlanır; şube yoksa
    # o şablon atlanır (uydurma şube yaratılmaz).
    # TOHUM KODA GÖRE, TEK SEFERLİK DEĞİL. Önceden `meta` bayrağı bir kez
    # yazılıyordu: o an şube yoksa (ya da adı tutmadıysa) o şablon BİR DAHA
    # ASLA oluşmuyordu — «Magic şablonları çıkmıyor» tam olarak buydu. Artık
    # her açılışta EKSİK kod var mı diye bakılır; owner sildiyse geri gelmesin
    # diye silinenler `meta`da işaretlenir.
    try:
        _bynm = {}
        for _b in db.execute("SELECT id, name FROM branches").fetchall():
            _bynm[(_b["name"] or "").strip().lower()] = _b["id"]
        _seed = [("c5d", "c5", "07:00", "17:00", 1), ("c5n", "c5", "17:00", "03:00", 2),
                 ("mgd", "magic", "07:00", "15:30", 3), ("mgn", "magic", "15:30", "00:00", 4),
                 ("mgf", "magic", "07:00", "00:00", 5)]
        _have = {r["code"] for r in db.execute("SELECT code FROM shift_templates").fetchall()}
        _killed = set()
        try:
            _kr = db.execute("SELECT val FROM meta WHERE k='seed_tpl_removed'").fetchone()
            if _kr and _kr["val"]:
                _killed = set(json.loads(_kr["val"]))
        except Exception:
            _killed = set()
        _n = 0
        for _c, _bn, _s, _e, _so in _seed:
            if _c in _have or _c in _killed:
                continue
            _bid = _bynm.get(_bn)
            if not _bid:
                continue                     # şube henüz yok → sonraki açılışta dener
            db.execute(
                "INSERT OR IGNORE INTO shift_templates (code, branch_id, start_t, end_t, active, sort_order, updated_at) "
                "VALUES (?,?,?,?,1,?,?)",
                (_c, _bid, _s, _e, _so, datetime.now(TZ).isoformat()))
            _n += 1
        if _n:
            logger.info(f"vardiya sablonu tohumlandi (eksik olanlar): {_n}")
    except Exception as _e:
        logger.warning(f"sablon tohumu: {_e}")
    # ─── Sipariş kategorileri (özel katalog başlıkları) — Nero owner düzenler ───
    db.execute("""CREATE TABLE IF NOT EXISTS order_categories (
        id TEXT PRIMARY KEY, name TEXT,
        sort_order INTEGER DEFAULT 0,
        created_by INTEGER, created_at TEXT, deleted INTEGER DEFAULT 0)""")
    # ─── Филиалы (şubeler) — çok şube desteği ───
    db.execute("""CREATE TABLE IF NOT EXISTS branches (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        group_chat_id TEXT,
        sort_order INTEGER DEFAULT 0,
        active INTEGER DEFAULT 1,
        created_at TEXT)""")
    # Şube-bazlı çalışma saatleri (kapalı pencere): open_hour, close_hour, unpaid_win
    for _bc2, _bd2 in (("open_hour", "INTEGER"), ("close_hour", "INTEGER"), ("unpaid_win", "INTEGER")):
        try:
            db.execute(f"ALTER TABLE branches ADD COLUMN {_bc2} {_bd2}")
        except sqlite3.OperationalError:
            pass
    # Şube işgücü ayarı: «Ассистент/стажёр» pozisyonu bu şubede açık mı?
    # 0 (varsayılan) = tek aktif barista slotu; 1 = barista + asistan İKİ bağımsız slot.
    # Şube adına göre DEĞİL — her şube kendi ayarını taşır (gelecekteki şubeler dahil).
    try:
        db.execute("ALTER TABLE branches ADD COLUMN trainee_enabled INTEGER DEFAULT 0")
    except sqlite3.OperationalError:
        pass
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
    # ─── Зарплатные категории (maaş kategorileri) — dinamik saatlik ücret grupları ───
    # Her barista bir kategoriye bağlanır; kategori kendi ставка'sını taşır. use_kpi=1
    # kategoriler satış (bardak/tatlı) bonusundan etkilenmez. min_months/next_cat_id
    # ileride terfi modülü için saklanır.
    db.execute("""CREATE TABLE IF NOT EXISTS salary_categories (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        hourly_rate INTEGER DEFAULT 0,
        min_months REAL DEFAULT 0,
        next_cat_id INTEGER,
        description TEXT,
        use_kpi INTEGER DEFAULT 0,
        active INTEGER DEFAULT 1,
        sort_order INTEGER DEFAULT 0,
        created_at TEXT)""")
    # users → bağlı olduğu maaş kategorisi (NULL = global ставка, geriye tam uyumlu)
    try:
        db.execute("ALTER TABLE users ADD COLUMN salary_cat_id INTEGER")
    except sqlite3.OperationalError:
        pass
    # Kategori bardak-bonus sistemi: 'own' (bizim/Цены) | 'caffelito' (resmi preset).
    try:
        db.execute("ALTER TABLE salary_categories ADD COLUMN bonus_system TEXT DEFAULT 'own'")
    except sqlite3.OperationalError:
        pass
    # Caffelito bardak bonus preset'i (bizim 'prices' tablosundan AYRI, düzenlenebilir).
    db.execute("""CREATE TABLE IF NOT EXISTS caffelito_bonus (
        drink_id TEXT PRIMARY KEY,
        amount INTEGER DEFAULT 0)""")
    try:
        _cbc = db.execute("SELECT COUNT(*) AS c FROM caffelito_bonus").fetchone()
        if (_cbc["c"] or 0) == 0:
            for _did, _amt in CAFFELITO_BONUS_DEFAULTS.items():
                db.execute("INSERT OR REPLACE INTO caffelito_bonus (drink_id, amount) VALUES (?,?)",
                           (_did, int(_amt)))
    except sqlite3.OperationalError:
        pass
    # ═══ ÜRÜN BONUSU MOTORU (Product Bonus Engine) — bardak bonusundan TAMAMEN AYRI ═══
    # Yapılandırılabilir ürün kataloğu: perakende + gıda satışları. Kod-sabit ürün YOK.
    db.execute("""CREATE TABLE IF NOT EXISTS product_catalog (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        price INTEGER DEFAULT 0,
        bonus_type TEXT DEFAULT 'percent',
        bonus_value INTEGER DEFAULT 0,
        category TEXT DEFAULT 'food',
        active INTEGER DEFAULT 1,
        sort_order INTEGER DEFAULT 0,
        created_at TEXT)""")
    # Maaş kategorisi ürün-bonusu UYGUNLUĞU (stajyer=0). Varsayılan 0 = opt-in (sessiz
    # ödeme olmasın); owner Barista 1/2/3 için açar.
    try:
        db.execute("ALTER TABLE salary_categories ADD COLUMN product_bonus INTEGER DEFAULT 0")
    except sqlite3.OperationalError:
        pass
    # Vardiya kapatmada bardak/kasa sayımı yapılsın mı? 1=evet (barista) · 0=stajyer atlar.
    # Varsayılan 1 → mevcut TÜM kategoriler eskisi gibi kasa sayar (geriye tam uyumlu).
    try:
        db.execute("ALTER TABLE salary_categories ADD COLUMN does_kasa INTEGER DEFAULT 1")
    except sqlite3.OperationalError:
        pass
    # Kategorinin işgal ettiği SLOT: 'barista' (varsayılan) | 'assistant' (ассистент/стажёр).
    # Şubede trainee_enabled=1 ise iki slot bağımsız çalışır; kapalıysa herkes barista slotu.
    try:
        db.execute("ALTER TABLE salary_categories ADD COLUMN slot_role TEXT DEFAULT 'barista'")
    except sqlite3.OperationalError:
        pass
    # Vardiya SNAPSHOT kolonları: başlangıçta rol+kategori+ставка dondurulur —
    # kategori/ücret sonradan değişse TARİHSEL vardiyalar asla etkilenmez.
    for _sc2, _sd2 in (("shift_role", "TEXT"), ("cat_id", "INTEGER"),
                       ("cat_name", "TEXT"), ("rate", "INTEGER")):
        try:
            db.execute(f"ALTER TABLE shifts ADD COLUMN {_sc2} {_sd2}")
        except sqlite3.OperationalError:
            pass
    # Kişi × şube kategori ataması (opsiyonel override): aynı kişi bir şubede stajyer,
    # diğerinde Barista 1 çalışabilir. Satır yoksa global (users.salary_cat_id) geçerli.
    db.execute("""CREATE TABLE IF NOT EXISTS branch_staff (
        user_id INTEGER,
        branch_id INTEGER,
        salary_cat_id INTEGER,
        PRIMARY KEY (user_id, branch_id))""")
    # Ürün satış kayıtları (ödeme anında girilir): dönemin gross'una ürün bonusu ekler.
    db.execute("""CREATE TABLE IF NOT EXISTS product_sales (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        period TEXT,
        sales TEXT,
        revenue INTEGER DEFAULT 0,
        bonus INTEGER DEFAULT 0,
        created_at TEXT,
        paid_by INTEGER)""")
    # İlk kurulum: katalog boşsa kullanıcının örnek ürünleriyle tohumla (hepsi düzenlenebilir).
    try:
        _pcc = db.execute("SELECT COUNT(*) AS c FROM product_catalog").fetchone()
        if (_pcc["c"] or 0) == 0:
            _so = 0
            for _cat, _items in PRODUCT_SEED:
                for _nm, _pr in _items:
                    db.execute(
                        "INSERT INTO product_catalog (name, price, bonus_type, bonus_value, category, active, sort_order, created_at) "
                        "VALUES (?,?,?,?,?,1,?,?)",
                        (_nm, int(_pr), "percent", 5, _cat, _so, datetime.now(TZ).isoformat()))
                    _so += 1
    except sqlite3.OperationalError:
        pass
    # İlk kurulum: hiç kategori yoksa mevcut global ставка ile "Базовая ставка" tohumla.
    # (Baristalar otomatik atanmaz → atanmayan global ставка kullanır, davranış değişmez.)
    try:
        _scc = db.execute("SELECT COUNT(*) AS c FROM salary_categories").fetchone()
        if (_scc["c"] or 0) == 0:
            _r0 = HOURLY_RATE
            try:
                _rr0 = db.execute("SELECT val FROM meta WHERE k='pay_rate'").fetchone()
                if _rr0 and _rr0["val"]:
                    _r0 = int(_rr0["val"])
            except Exception:
                pass
            db.execute(
                "INSERT INTO salary_categories (name, hourly_rate, sort_order, active, created_at) "
                "VALUES (?,?,0,1,?)",
                ("Базовая ставка", int(_r0), datetime.now(TZ).isoformat()))
    except sqlite3.OperationalError:
        pass
    # Stajyer kategorisi — bir kez tohumla (meta bayrağı). does_kasa=0 (bardak/kasa yok),
    # ürün bonusu yok. Owner ставка'yı sonra düzenler; silinirse tekrar gelmez.
    try:
        if not db.execute("SELECT 1 FROM meta WHERE k='stajyer_seeded'").fetchone():
            if not db.execute("SELECT 1 FROM salary_categories WHERE name=?", ("Стажёр",)).fetchone():
                _mos = db.execute("SELECT COALESCE(MAX(sort_order),-1)+1 AS s FROM salary_categories").fetchone()
                db.execute(
                    "INSERT INTO salary_categories (name, hourly_rate, use_kpi, active, product_bonus, does_kasa, sort_order, created_at) "
                    "VALUES (?,?,0,1,0,0,?,?)",
                    ("Стажёр", int(HOURLY_RATE), (_mos["s"] if _mos else 1), datetime.now(TZ).isoformat()))
            db.execute("INSERT OR REPLACE INTO meta (k,val) VALUES ('stajyer_seeded', ?)", (datetime.now(TZ).isoformat(),))
    except Exception:
        pass
    # Bir kez: daha önce tohumlanan «Стажёр» kategorisini assistant slotuna işaretle
    # (kolon yeni eklendi; owner sonradan istediği kategoriyi işaretleyebilir).
    try:
        if not db.execute("SELECT 1 FROM meta WHERE k='slotrole_seeded'").fetchone():
            db.execute("UPDATE salary_categories SET slot_role='assistant' "
                       "WHERE name=? AND COALESCE(slot_role,'barista')='barista'", ("Стажёр",))
            db.execute("INSERT OR REPLACE INTO meta (k,val) VALUES ('slotrole_seeded', ?)", (datetime.now(TZ).isoformat(),))
    except Exception:
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
    """Şubeler listesi (saatlerle + işgücü ayarı)."""
    q = ("SELECT id, name, group_chat_id, sort_order, active, open_hour, close_hour, unpaid_win, "
         "COALESCE(trainee_enabled,0) AS trainee_enabled FROM branches")
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
PAY_DEFAULTS = {"open": 7, "close": 3, "max": 20, "unpaid": 1, "rate": HOURLY_RATE}


def get_pay_cfg(db):
    """{open, close, max, unpaid, rate} — açılış/kapanış saati, max vardiya (saat),
    kapalı pencere düşümü, saatlik ücret. Owner Настройки'den değiştirir."""
    cfg = dict(PAY_DEFAULTS)
    try:
        rows = db.execute(
            "SELECT k,val FROM meta WHERE k IN ('pay_open','pay_close','pay_max','pay_unpaid','pay_rate')").fetchall()
        for r in rows:
            key = r["k"].split("_", 1)[1]
            try:
                cfg[key] = int(r["val"])
            except Exception:
                pass
    except Exception:
        pass
    return cfg


def get_salary_categories(db, only_active=False):
    """Maaş kategorileri listesi (dict)."""
    q = ("SELECT id,name,hourly_rate,min_months,next_cat_id,description,use_kpi,"
         "COALESCE(bonus_system,'own') AS bonus_system,COALESCE(product_bonus,0) AS product_bonus,"
         "COALESCE(does_kasa,1) AS does_kasa,COALESCE(slot_role,'barista') AS slot_role,"
         "active,sort_order "
         "FROM salary_categories")
    if only_active:
        q += " WHERE COALESCE(active,1)=1"
    q += " ORDER BY sort_order, id"
    try:
        return [dict(r) for r in db.execute(q).fetchall()]
    except Exception:
        return []


def get_product_catalog(db, only_active=False):
    """Ürün bonusu kataloğu (dict listesi). Bardak/tatlı bonusundan AYRI motor."""
    q = ("SELECT id,name,price,COALESCE(bonus_type,'percent') AS bonus_type,"
         "COALESCE(bonus_value,0) AS bonus_value,COALESCE(category,'food') AS category,"
         "COALESCE(active,1) AS active,sort_order FROM product_catalog")
    if only_active:
        q += " WHERE COALESCE(active,1)=1"
    q += " ORDER BY sort_order, id"
    try:
        return [dict(r) for r in db.execute(q).fetchall()]
    except Exception:
        return []


def product_bonus_per_unit(prod):
    """Bir ürünün BİRİM bonusu: percent → price*value/100, fixed → value."""
    try:
        if (prod.get("bonus_type") or "percent") == "fixed":
            return int(prod.get("bonus_value") or 0)
        return int(int(prod.get("price") or 0) * int(prod.get("bonus_value") or 0) / 100)
    except Exception:
        return 0


def calc_product_bonus(db, sales, eligible=True):
    """sales={product_id: qty} → {revenue, bonus, lines:[{id,name,qty,price,unit,revenue,bonus}]}.
    eligible=False (ör. stajyer/uygun olmayan kategori) → bonus 0 (ciro yine hesaplanır)."""
    out = {"revenue": 0, "bonus": 0, "lines": []}
    if not sales:
        return out
    by_id = {p["id"]: p for p in get_product_catalog(db)}
    for pid, qty in sales.items():
        try:
            p = by_id.get(int(pid))
            q = int(qty or 0)
        except Exception:
            continue
        if not p or q <= 0:
            continue
        rev = int(p.get("price") or 0) * q
        unit = product_bonus_per_unit(p)
        bon = (unit * q) if eligible else 0
        out["revenue"] += rev
        out["bonus"] += bon
        out["lines"].append({"id": p["id"], "name": p["name"], "qty": q,
                             "price": p.get("price") or 0, "unit": unit,
                             "revenue": rev, "bonus": bon})
    return out


def get_product_report(db, period=None):
    """Ürün bonusu dashboard verisi (owner). product_sales'i dönem için agregeler:
    {revenue, bonus, count, avg, employees:[{nm,revenue,bonus}], products:[{name,qty,revenue,bonus}]}."""
    period = period or current_period()
    rep = {"revenue": 0, "bonus": 0, "count": 0, "avg": 0, "employees": [], "products": []}
    try:
        rows = db.execute(
            "SELECT ps.sales AS sales, ps.revenue AS revenue, ps.bonus AS bonus, "
            "COALESCE(u.display_name,u.name,'?') AS nm FROM product_sales ps "
            "LEFT JOIN users u ON u.user_id=ps.user_id WHERE ps.period=? ORDER BY ps.id DESC",
            (period,)).fetchall()
    except Exception:
        return rep
    by_name = {p["id"]: p["name"] for p in get_product_catalog(db)}
    by_price = {p["id"]: (p.get("price") or 0) for p in get_product_catalog(db)}
    emp = {}
    prod = {}  # pid -> [qty, revenue, bonus]
    for r in rows:
        nm = r["nm"] or "?"
        rev = r["revenue"] or 0
        bon = r["bonus"] or 0
        rep["revenue"] += rev
        rep["bonus"] += bon
        e = emp.setdefault(nm, {"nm": nm, "revenue": 0, "bonus": 0})
        e["revenue"] += rev
        e["bonus"] += bon
        try:
            sales = json.loads(r["sales"] or "{}")
        except Exception:
            sales = {}
        for pid, qty in sales.items():
            try:
                pid_i = int(pid)
                q = int(qty or 0)
            except Exception:
                continue
            if q <= 0:
                continue
            pr = prod.setdefault(pid_i, {"name": by_name.get(pid_i, "?"), "qty": 0, "revenue": 0, "bonus": 0})
            pr["qty"] += q
            # ürün başına ciro/bonus: kayıt anındaki katalog fiyatıyla değil, toplam
            # kaydın dağılımıyla yaklaşık — basitlik için katalog fiyatını kullan
            pr["revenue"] += q * by_price.get(pid_i, 0)
    rep["count"] = len(rows)
    rep["avg"] = int(rep["bonus"] / len(emp)) if emp else 0
    rep["employees"] = sorted(emp.values(), key=lambda x: -x["bonus"])
    rep["products"] = sorted(prod.values(), key=lambda x: -x["qty"])
    return rep


def get_caffelito_bonus(db):
    """Caffelito bardak-bonus preset'i {drink_id: amount} (default > DB override)."""
    out = dict(CAFFELITO_BONUS_DEFAULTS)
    try:
        for r in db.execute("SELECT drink_id, amount FROM caffelito_bonus").fetchall():
            out[r["drink_id"]] = int(r["amount"] or 0)
    except Exception:
        pass
    return out


def barista_pay_info(db, user_id, branch_id=None):
    """Baristanın maaş bilgisi: {rate, use_kpi, bonus_system, cat_id, cat_name, slot_role}.
    branch_id verilirse ÖNCE kişi×şube ataması (branch_staff) bakılır — aynı kişi farklı
    şubede farklı kategoriyle çalışabilir. Yoksa global kategori (geriye tam uyumlu)."""
    info = {"rate": int(get_pay_cfg(db).get("rate", HOURLY_RATE)),
            "use_kpi": 0, "bonus_system": "own", "product_ok": 0, "does_kasa": 1,
            "slot_role": "barista", "cat_id": None, "cat_name": None}
    try:
        _cols = ("id AS cid, name AS cname, hourly_rate AS rate, use_kpi AS kpi, "
                 "COALESCE(bonus_system,'own') AS bsys, COALESCE(product_bonus,0) AS pb, "
                 "COALESCE(does_kasa,1) AS dk, COALESCE(slot_role,'barista') AS sr, active")
        r = None
        if branch_id:
            _ov = db.execute(
                "SELECT salary_cat_id FROM branch_staff WHERE user_id=? AND branch_id=?",
                (user_id, int(branch_id))).fetchone()
            if _ov and _ov["salary_cat_id"]:
                r = db.execute(f"SELECT {_cols} FROM salary_categories WHERE id=?",
                               (_ov["salary_cat_id"],)).fetchone()
        if not r:
            r = db.execute(
                "SELECT c.id AS cid, c.name AS cname, c.hourly_rate AS rate, c.use_kpi AS kpi, "
                "COALESCE(c.bonus_system,'own') AS bsys, COALESCE(c.product_bonus,0) AS pb, "
                "COALESCE(c.does_kasa,1) AS dk, COALESCE(c.slot_role,'barista') AS sr, "
                "c.active AS active "
                "FROM users u JOIN salary_categories c ON c.id = u.salary_cat_id WHERE u.user_id=?",
                (user_id,)).fetchone()
        if r and (r["active"] is None or r["active"] == 1):
            info["rate"] = int(r["rate"] or 0)
            info["use_kpi"] = int(r["kpi"] or 0)
            info["bonus_system"] = r["bsys"] or "own"
            info["product_ok"] = int(r["pb"] or 0)
            info["does_kasa"] = int(r["dk"] if r["dk"] is not None else 1)
            info["slot_role"] = r["sr"] or "barista"
            info["cat_id"] = r["cid"]
            info["cat_name"] = r["cname"]
    except Exception:
        pass
    return info


def branch_trainee_enabled(db, branch_id):
    """Bu şubede «Ассистент/стажёр» pozisyonu açık mı? (şube-bazlı ayar, hardcode yok)"""
    try:
        r = db.execute("SELECT COALESCE(trainee_enabled,0) AS t FROM branches WHERE id=?",
                       (int(branch_id or 1),)).fetchone()
        return bool(r and r["t"])
    except Exception:
        return False


def slot_occupant(db, branch_id, role):
    """Bu şubede bu rol pozisyonunu tutan AÇIK vardiya → {uid, nm, st} veya None.
    Eski açık vardiyalarda shift_role NULL → 'barista' sayılır (geriye uyumlu)."""
    try:
        r = db.execute(
            "SELECT s.user_id AS uid, s.start_time AS st, "
            "COALESCE(u.display_name,u.name,'?') AS nm "
            "FROM shifts s LEFT JOIN users u ON u.user_id=s.user_id "
            "WHERE s.end_time IS NULL AND s.start_time IS NOT NULL "
            "AND COALESCE(s.branch_id,1)=? AND COALESCE(s.shift_role,'barista')=? "
            "ORDER BY s.id DESC LIMIT 1",
            (int(branch_id or 1), role)).fetchone()
        return dict(r) if r else None
    except Exception:
        return None


def slot_block_reason(db, user_id, branch_id):
    """Kullanıcı bu şubede vardiya başlatabilir mi? None=serbest, str=dostça engel mesajı.
    ROL her zaman kişinin (şube-etkin) kategorisinden gelir — toggle rolü değiştirmez.
    Toggle kapalı + kişi asistan kategorili → o şubede asistan POZİSYONU YOK → net mesaj."""
    bid = int(branch_id or 1)
    pi = barista_pay_info(db, user_id, branch_id=bid)
    role = pi.get("slot_role") or "barista"
    if role == "assistant" and not branch_trainee_enabled(db, bid):
        _bn = ""
        try:
            _br = get_branch(db, bid)
            _bn = (_br["name"] if _br else "") or ""
        except Exception:
            _bn = ""
        return ("ℹ️ Нет позиции ассистента\n\n"
                + (f"В филиале «{_bn}» " if _bn else "В этом филиале ")
                + "нет позиции ассистента/стажёра.\n"
                "Владелец может включить её (Управление → Филиалы) "
                "или назначить вам категорию бариста для этого филиала.")
    occ = slot_occupant(db, bid, role)
    if occ and occ["uid"] != user_id:
        st = ""
        try:
            st = datetime.fromisoformat(occ["st"]).strftime("%H:%M")
        except Exception:
            pass
        # Net «передача смены» mesajı: önce mevcut çalışan bitirir, sonra devralırsın.
        if role == "assistant":
            return ("🔄 Передача смены\n\n"
                    f"Смена ассистента ещё активна — {occ['nm']}"
                    + (f" (с {st})" if st else "") + ".\n"
                    "Текущий ассистент должен завершить свою смену — после этого вы сможете принять смену.")
        return ("🔄 Передача смены\n\n"
                f"Смена бариста ещё активна — {occ['nm']}"
                + (f" (с {st})" if st else "") + ".\n"
                "Текущий бариста должен завершить свою смену — после этого вы сможете принять смену.")
    return None


def refresh_open_shift_snapshot(db, user_id):
    """AÇIK vardiyanın rol/kategori/ставка snapshot'ını kişinin GÜNCEL (şube-etkin)
    kategorisine göre tazeler. Kapanmış (tarihsel) vardiyalara ASLA dokunmaz.
    Owner canlı vardiya sırasında kategori düzeltirse anında yansısın diye
    (ör. kişi yanlışlıkla barista rolüyle başladıysa → stajyer atanınca açık
    vardiya stajyer olur, barista slotu boşalır)."""
    try:
        act = get_active_shift(db, user_id)
        if not act:
            return False
        bid = act["branch_id"] if act["branch_id"] else user_branch_id(db, user_id)
        pi = barista_pay_info(db, user_id, branch_id=bid)
        role = pi.get("slot_role") or "barista"
        db.execute("UPDATE shifts SET shift_role=?, cat_id=?, cat_name=?, rate=? WHERE id=?",
                   (role, pi.get("cat_id"), pi.get("cat_name") or "",
                    int(pi.get("rate") or 0), act["id"]))
        db.commit()
        return True
    except Exception:
        return False


def paid_hours(start_dt, end_dt, cfg):
    """Ödenecek saat: ham süreden KAPALI pencereye (kapanış→açılış, ör. 01:00–07:00)
    denk gelen kısım düşülür. MAX CAP YOK (kullanıcı isteğiyle kaldırıldı) → к оплате =
    çalışılan − kapalı saatler. start_dt/end_dt naive datetime."""
    if not end_dt or not start_dt or end_dt <= start_dt:
        return 0.0
    raw = (end_dt - start_dt).total_seconds() / 3600.0
    # "Не оплачивать закрытые часы" KAPALI → tüm süre ödenir (düşüm yok).
    if not cfg.get("unpaid", 1):
        return round(raw, 2)
    unpaid = 0.0
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
    paid = max(0.0, raw - unpaid)  # max cap KALDIRILDI
    return round(paid, 2)


def branch_pay_window(db, branch_id):
    """Bir şubenin ödeme penceresi: {open,close,unpaid,max,rate}. open/close/unpaid
    şubeye özel (branches tablosu); tanımsızsa global (get_pay_cfg). max+rate global."""
    g = get_pay_cfg(db)
    win = {"open": g["open"], "close": g["close"], "unpaid": g["unpaid"],
           "max": g["max"], "rate": g["rate"]}
    try:
        b = db.execute("SELECT open_hour,close_hour,unpaid_win FROM branches WHERE id=?",
                       (int(branch_id or 0),)).fetchone()
        if b:
            if b["open_hour"] is not None:
                win["open"] = int(b["open_hour"])
            if b["close_hour"] is not None:
                win["close"] = int(b["close_hour"])
            if b["unpaid_win"] is not None:
                win["unpaid"] = int(b["unpaid_win"])
    except Exception:
        pass
    return win


BONUS_RATES = {
    "ml100": 500,
    "ml200": 700,
    "ml300": 1000,
    "ml400": 1200,
    "ml500": 1400,
    "dome300": 1000,
    "dome400": 1300,
}

# Caffelito resmi bardak-bonus preset'i (örnek değerler; panelden düzenlenebilir).
# 100-200мл=200, 300=300, 400=700, 500=800. (Купол = ilgili boy ile eşlenir.)
# NOT: "Сезонное меню +1000" ayrı bir sayaç gerektirir (henüz vardiya kapatmada yok) → ileride.
CAFFELITO_BONUS_DEFAULTS = {
    "ml100": 200,
    "ml200": 200,
    "ml300": 300,
    "ml400": 700,
    "ml500": 800,
    "dome300": 300,
    "dome400": 700,
}

# Ürün bonusu kataloğu ilk kurulum tohumu (kullanıcının örnekleri; hepsi panelden
# düzenlenebilir/silinebilir). (kategori, [(ad, fiyat), ...]). Bonus varsayılan %5.
PRODUCT_SEED = [
    ("food",     [("Сэндвич", 38000), ("Чиабатта", 35000), ("Круассан", 22000), ("Чизкейк", 30000)]),
    ("snack",    [("Печенье", 20000), ("Вафли", 15000), ("Стропвафли", 18000), ("Шоколадные шарики", 25000),
                  ("Дубайский шоколад", 45000), ("Mini Pops", 12000)]),
    ("coffee",   [("Кофе в зёрнах", 120000), ("Дрип-кофе", 25000)]),
    ("icecream", [("Мороженое", 20000)]),
]

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


def _drop_shift_daily_pay(db, shift_id):
    """Bir vardiya silinirken, o kapanışta KASADAN ödenen günlük bardak bonusunu
    da geri alır (payments'tan siler). Böylece «kazanç gitti ama ödendi kaydı
    kaldı» durumu oluşmaz — bakiye bonus kadar eksiye düşmez.

    Güvenlik: yalnızca KENDİ KENDİNE ödeme (paid_by = user_id → günlük bonus),
    tutarı vardiyanın bonusuyla AYNI ve kapanış saatine ±2 saat yakın olan TEK
    kayıt silinir. Owner'ın elle yaptığı maaş ödemeleri (paid_by != user_id)
    ASLA silinmez. Eşleşme bulunamazsa hiçbir şey yapılmaz.
    Silinen ödemenin id'sini döner, yoksa None."""
    try:
        sh = db.execute("SELECT user_id, bonus, end_time FROM shifts WHERE id=?", (shift_id,)).fetchone()
        if not sh or not sh["end_time"]:
            return None
        amt = int(sh["bonus"] or 0)
        if amt <= 0:
            return None
        uid = sh["user_id"]
        end = datetime.fromisoformat(sh["end_time"])
        lo = (end - timedelta(hours=2)).isoformat()
        hi = (end + timedelta(hours=2)).isoformat()
        row = db.execute(
            "SELECT id FROM payments WHERE user_id=? AND paid_by=? AND amount=? "
            "AND paid_at BETWEEN ? AND ? ORDER BY id DESC LIMIT 1",
            (uid, uid, amt, lo, hi)).fetchone()
        if not row:
            return None
        db.execute("DELETE FROM payments WHERE id=?", (row["id"],))
        return row["id"]
    except Exception as e:
        logger.warning(f"_drop_shift_daily_pay({shift_id}): {e}")
        return None


def daily_bonus_pay_ids(db, user_id, period):
    """Bir dönemdeki GÜNLÜK BARDAK BONUSU ödemelerinin payments id'leri.

    İş kuralı: bardak bonusu her gün kapanışta KASADAN nakit veriliyor (yol
    parası). Yani maaşın parçası değil — ne aylık alacağa eklenir ne de
    «выплата» olarak sayılır. Ay sonu alacak SADECE saatlik ücret.

    Yeni kapanışlarda böyle bir payments kaydı ARTIK OLUŞTURULMUYOR
    (bkz. cash_report). Bu fonksiyon yalnızca kural değişmeden ÖNCE yazılmış
    kayıtları tanır: hiçbiri silinmez, sadece maaş hesabının ve «Выплаты»
    listesinin dışında tutulur. Böylece geçmiş bakiyeler de «sadece saatlik»
    olur ve karar geri alınabilir kalır.

    İki aşamalı eşleşme; ikisinde de `paid_by = user_id` şartı var, yani
    owner'ın elle yaptığı maaş ödemeleri (paid_by != user_id) ASLA eşleşmez.

    1) KESİN: bonus kaydı, kasa raporuyla AYNI istekte ve AYNI `now` ile
       yazılıyordu → `payments.paid_at` ile `cashreports.created_at` birebir
       aynı ve tutar `cashreports.daily_pay`e eşit. Vardiya saatleri sonradan
       elle düzeltilmiş olsa bile (edit_shift / «другое время ухода») bu
       eşleşme kaymaz.
    2) YEDEK: kasa raporu silinmişse eski imza — `_drop_shift_daily_pay` ile
       aynı: tutar o vardiyanın bonusuyla birebir + kapanış saatine ±2 saat.
       Her vardiya en fazla BİR ödemeyi tüketir (aynı tutarlı iki kapanış
       birbirinin kaydını yutmaz)."""
    ids = set()
    # 1) Kasa raporuyla birebir zaman+tutar eşleşmesi
    try:
        rows = db.execute(
            "SELECT p.id AS id FROM payments p "
            "JOIN cashreports c ON c.user_id = p.user_id AND c.created_at = p.paid_at "
            "AND c.daily_pay = p.amount "
            "WHERE p.user_id=? AND p.period=? AND p.paid_by = p.user_id "
            "AND COALESCE(c.daily_pay,0) > 0", (user_id, period)).fetchall()
        for r in rows:
            ids.add(r["id"])
    except Exception as e:
        logger.warning(f"daily_bonus_pay_ids/kassa({user_id},{period}): {e}")
    # 2) Yedek: vardiya bonusu + ±2 saat
    try:
        shifts = db.execute(
            "SELECT id, bonus, end_time FROM shifts WHERE user_id=? AND period=? "
            "AND end_time IS NOT NULL AND COALESCE(bonus,0) > 0 ORDER BY id",
            (user_id, period)).fetchall()
        if not shifts:
            return ids
        for sh in shifts:
            amt = int(sh["bonus"] or 0)
            try:
                end = datetime.fromisoformat(sh["end_time"])
            except Exception:
                continue
            lo = (end - timedelta(hours=2)).isoformat()
            hi = (end + timedelta(hours=2)).isoformat()
            rows = db.execute(
                "SELECT id FROM payments WHERE user_id=? AND paid_by=? AND amount=? "
                "AND paid_at BETWEEN ? AND ? ORDER BY id",
                (user_id, user_id, amt, lo, hi)).fetchall()
            for r in rows:
                if r["id"] not in ids:
                    ids.add(r["id"])
                    break          # bu vardiya için tek kayıt
    except Exception as e:
        logger.warning(f"daily_bonus_pay_ids/shift({user_id},{period}): {e}")
    return ids


def grid_week_key(week_offset):
    """Nero График'inin göreli «week» offset'ini (0=bu hafta, ±1) o haftanın
    PAZARTESİ tarihine (YYYY-MM-DD) çevirir. Böylece «week 0» bugün ile gelecek
    hafta aynı satıra yazmaz — plan mutlak tarihe bağlanır."""
    try:
        off = int(week_offset or 0)
    except Exception:
        off = 0
    today = datetime.now(TZ).replace(tzinfo=None).date()
    monday = today - timedelta(days=today.weekday()) + timedelta(days=off * 7)
    return monday.isoformat()


# ─── VARDİYA PLANI KURAL MOTORU ─────────────────────────────────────────────
# Tek kaynak: hem eylemler hem testler buradan geçer. Kurallar iki yerde ayrı
# yazılırsa er ya da geç ayrışır ve plan sessizce tutarsız hâle gelir.

def grid_templates(db):
    """{code: {branch_id, start, end, label}} — aktif vardiya şablonları."""
    out = {}
    try:
        for r in db.execute("SELECT * FROM shift_templates WHERE COALESCE(active,1)=1 "
                            "ORDER BY sort_order, code").fetchall():
            out[r["code"]] = {"branch_id": r["branch_id"], "start": r["start_t"] or "",
                              "end": r["end_t"] or ""}
    except Exception:
        pass
    return out


def _mins(hhmm):
    try:
        h, m = str(hhmm).split(":")
        return int(h) * 60 + int(m)
    except Exception:
        return None


def grid_overlap(a_start, a_end, b_start, b_end):
    """İki vardiya saati ÇAKIŞIYOR mu. Gece yarısını geçen vardiya (17:00→03:00)
    ertesi güne taşar; bitiş başlangıçtan küçükse +24 saat sayılır."""
    a1, a2 = _mins(a_start), _mins(a_end)
    b1, b2 = _mins(b_start), _mins(b_end)
    if None in (a1, a2, b1, b2):
        return False
    if a2 <= a1:
        a2 += 1440
    if b2 <= b1:
        b2 += 1440
    return a1 < b2 and b1 < a2


def grid_check(db, week_key, day, user_id, code, tpls=None):
    """Bir hücreye `code` atanabilir mi? (ok: bool, why: str) döner.

    Sırayla:
      · 'off' her zaman serbest (izin koymak kapasiteyi zorlamaz)
      · şablon tanımlı mı
      · ÇAKIŞMA: kişi aynı gün başka bir şubede çakışan saatte mi
      · KAPASİTE: şubenin o günkü çalışan sayısı sınırı aştı mı
    """
    code = (code or "").strip()
    if not code:
        return False, "Не указана смена."
    if code == "off":
        return True, ""
    tpls = tpls if tpls is not None else grid_templates(db)
    t = tpls.get(code)
    if not t:
        return False, "Такой смены нет в шаблонах."
    try:
        rows = db.execute(
            "SELECT user_id, code FROM shift_grid WHERE week_key=? AND day=? AND user_id!=?",
            (week_key, int(day), int(user_id))).fetchall()
        own = db.execute(
            "SELECT code FROM shift_grid WHERE week_key=? AND day=? AND user_id=?",
            (week_key, int(day), int(user_id))).fetchall()
    except Exception as e:
        logger.warning(f"grid_check: {e}")
        return True, ""            # şüphede ENGELLEME — plan kilitlenmesin
    # ÇAKIŞMA — aynı kişi, aynı gün, çakışan saat (kendi ikinci vardiyası)
    for r in own:
        ot = tpls.get(r["code"])
        if ot and r["code"] != code and grid_overlap(t["start"], t["end"], ot["start"], ot["end"]):
            return False, "Сотрудник уже назначен на другую смену в это время."
    # KAPASİTE — bu şubede o gün kaç kişi var
    bid = t["branch_id"]
    if bid:
        cnt = 0
        for r in rows:
            rt = tpls.get(r["code"])
            if rt and rt["branch_id"] == bid:
                cnt += 1
        try:
            br = db.execute("SELECT name, COALESCE(max_staff,2) AS mx FROM branches WHERE id=?",
                            (int(bid),)).fetchone()
        except Exception:
            br = None
        mx = int(br["mx"]) if br else 2
        if mx > 0 and cnt >= mx:
            return False, "Максимум сотрудников на филиале достигнут."
    return True, ""


def grid_off_limit(db, user_id):
    """Kişinin haftalık izin hakkı: kişiye özel değer varsa o, yoksa genel ayar."""
    try:
        r = db.execute("SELECT off_limit FROM users WHERE user_id=?", (user_id,)).fetchone()
        if r and r["off_limit"] is not None:
            return max(0, int(r["off_limit"]))
    except Exception:
        pass
    try:
        m = db.execute("SELECT val FROM meta WHERE k='weekly_off_limit'").fetchone()
        if m and m["val"]:
            return max(0, int(m["val"]))
    except Exception:
        pass
    return 2


def grid_off_used(db, week_key, user_id, exclude_day=None):
    """O haftada kişinin KAÇ izni var (plandaki 'off' hücreleri)."""
    try:
        rows = db.execute(
            "SELECT day FROM shift_grid WHERE week_key=? AND user_id=? AND code='off'",
            (week_key, int(user_id))).fetchall()
    except Exception:
        return 0
    return len([r for r in rows if exclude_day is None or int(r["day"]) != int(exclude_day)])


def grid_off_allowed(db, week_key, user_id, day):
    """Bu güne izin EKLENEBİLİR mi (haftalık limit)."""
    lim = grid_off_limit(db, user_id)
    used = grid_off_used(db, week_key, user_id, exclude_day=day)
    if used >= lim:
        return False, f"Вы достигли лимита выходных на этой неделе ({lim})."
    return True, ""


_GRID_DAYS = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]


def grid_day_label(day):
    try:
        return _GRID_DAYS[int(day)]
    except Exception:
        return "?"


def sched_body_lines(items, names=None):
    """Планлы sipariş kalemlerinden ({key: qty}) grup mesajı satırları üretir —
    anlık `order` aksiyonuyla aynı «• <b>Ad — Nx</b>» biçimi. `names` {key: ad}
    verilirse okunur adlar; yoksa anahtar basılır (Nero adları göndermeye başlayınca
    aynı handler otomatik düzgün render eder, kod değişmez)."""
    from html import escape as _esc
    names = names or {}
    out = []
    for k, v in (items or {}).items():
        try:
            q = int(v or 0)
        except Exception:
            q = 0
        if q <= 0:
            continue
        nm = names.get(k) or names.get(str(k)) or str(k)
        out.append(f"• <b>{_esc(str(nm))} — {q}x</b>")
    return out or ["• (позиции)"]


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


def device_gate(db, user_id, device_id, platform="", label=""):
    """Nero CİHAZ kapısı. Döner: 'ok' | 'pending' | 'revoked' | 'new' (yeni onay bekliyor).

    `nero_access_ok` kişinin girip giremeyeceğini söyler; bu ise HANGİ CİHAZDAN.
    Kural (TOFU — trust on first use):
      · cihaz kimliği YOKSA (eski istemci, depolama kapalı) → 'ok'. Yeni katman
        hiçbir mevcut kullanımı bozmaz.
      · owner → HER ZAMAN 'ok' (onayı veren o; kilitlenirse kimse açamaz).
        Cihazı yine de kaydedilir, listede görünür.
      · kişinin İLK cihazı → sessizce onaylı. Bugün çalışan herkes bir sonraki
        açılışta buradan geçer, kimse kapıda kalmaz.
      · sonraki her YENİ cihaz → 'new' (kayıt açılır, approved=0) → owner onaylayana
        kadar 'pending'.
      · owner tarafından çıkarılan cihaz → 'revoked'.
    HATA DURUMUNDA 'ok' döner (fail-OPEN). Kimlik kapısı fail-closed'dur; bu
    katmanın bir arızası yüzünden ekip işten kalmasın.
    """
    did = (device_id or "").strip()[:64]
    if not did:
        return "ok"
    try:
        _r = db.execute("SELECT role FROM users WHERE user_id=?", (user_id,)).fetchone()
        is_owner = bool(_r and (_r["role"] or "") == "owner")
        now = datetime.now(TZ).isoformat()
        row = db.execute("SELECT * FROM devices WHERE user_id=? AND device_id=?",
                         (user_id, did)).fetchone()
        if row:
            db.execute("UPDATE devices SET last_seen=?, seen_count=COALESCE(seen_count,0)+1, "
                       "platform=CASE WHEN ?='' THEN platform ELSE ? END WHERE id=?",
                       (now, platform or "", platform or "", row["id"]))
            db.commit()
            if is_owner:
                return "ok"
            if row["revoked"]:
                return "revoked"
            return "ok" if row["approved"] else "pending"
        # YENİ cihaz
        n = db.execute("SELECT COUNT(*) AS c FROM devices WHERE user_id=?", (user_id,)).fetchone()["c"] or 0
        auto = 1 if (is_owner or n == 0) else 0
        db.execute(
            "INSERT INTO devices (user_id,device_id,label,platform,approved,revoked,first_seen,last_seen,seen_count) "
            "VALUES (?,?,?,?,?,0,?,?,1)",
            (user_id, did, (label or "")[:64], (platform or "")[:32], auto, now, now))
        db.commit()
        try:
            log_action(db, "device_new", user_id, display_name_for(db, user_id, fallback=""),
                       user_id, display_name_for(db, user_id, fallback=""),
                       {"device": did[:12], "platform": platform or "", "auto": auto})
        except Exception:
            pass
        return "ok" if auto else "new"
    except Exception as e:
        logger.warning(f"device_gate({user_id}): {e}")
        return "ok"


def _nero_archived(db, user_id):
    """Kişi owner tarafından arşive alındı mı (= erişim isteği reddedildi)."""
    try:
        r = db.execute("SELECT COALESCE(archived,0) AS ar FROM users WHERE user_id=?",
                       (user_id,)).fetchone()
        return bool(r and r["ar"])
    except Exception:
        return False


def nero_access_ok(db, user_id):
    """Nero (Mini App) ERİŞİM KAPISI — okuma dahil.

    Eskiden `/api/state` kimseye bakmıyordu: geçerli Telegram imzası olan HERKES
    tam payload alıyordu. İstemci de yalnızca PIN TANIMLIYSA kilitliyordu, yani
    PIN'i olmayan (= kayıtsız) kişi doğrudan içeri giriyordu. İki hata birleşince
    botu açan her telefon/tablet uygulamayı görebiliyordu.

    Kural:
      · owner → her zaman
      · arşivli → asla
      · kaydı yok / owner onayı yok (approved=0) → HAYIR
      · onaylı ise `is_authorized` (PIN atanmış ya da authorized=1)
    `approved=0` yeni /start yapanların varsayılanı; mevcut personel migration'da
    approved=1 aldı, yani kimse dışarıda kalmaz. Onay owner'da:
    «Заявки на доступ» → approve_user.
    """
    try:
        row = db.execute(
            "SELECT role, COALESCE(approved,0) AS ap, COALESCE(archived,0) AS ar "
            "FROM users WHERE user_id=?", (user_id,)).fetchone()
    except Exception as e:
        logger.warning(f"nero_access_ok({user_id}): {e}")
        return False          # şüphede KAPALI (fail-closed)
    if not row:
        return False
    if (row["role"] or "") == "owner":
        return True
    if row["ar"] or not row["ap"]:
        return False
    return is_authorized(db, user_id)


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


# ─── Fazla mesai (сверхурочные) yapılandırması ───
def get_overtime_cfg(db):
    """{hours, type, value}: aylık norm saat (0=kapalı), tip ('fixed'|'percent'),
    değer (fixed=ek сум/saat, percent=% artış). Norm üstü saatlere uygulanır."""
    # `on`: AÇIK/KAPALI anahtarı. Eskiden yoktu — «Считать переработку» düğmesi
    # `enabled` gönderiyordu ama bot onu HİÇ okumuyordu, düğme her tazelemede
    # eski hâline dönüyordu. Kapalıyken yapılandırılan saat/tutar KORUNUR,
    # yalnızca hesaplama devre dışı kalır. Eski kurulumlarda anahtar yoksa
    # davranış değişmesin diye varsayılan AÇIK (hesap zaten hours>0 ile kapalı).
    cfg = {"hours": 0, "type": "fixed", "value": 0, "on": 1}
    try:
        for r in db.execute("SELECT k,val FROM meta WHERE k IN ('ot_hours','ot_type','ot_value','ot_on')").fetchall():
            k = r["k"][3:]
            if k == "type":
                cfg["type"] = r["val"] if r["val"] in ("fixed", "percent") else "fixed"
            elif k == "on":
                cfg["on"] = 1 if str(r["val"]) not in ("0", "", "None") else 0
            else:
                try:
                    cfg[k] = int(float(r["val"]))
                except Exception:
                    pass
    except Exception:
        pass
    return cfg


# NOT: Eski canlı/geriye-dönük calc_overtime KALDIRILDI. Fazla mesai artık her
# vardiyada KAPANIŞ ANINDA (end_shift) dondurulup shifts.overtime'a saklanır;
# calc_summary sadece bunları TOPLAR (ayar değişse eski vardiyalar korunur).


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
    # «Ödendi» toplamı = GERÇEK maaş ödemeleri/avanslar. Günlük bardak bonusu
    # kasadan nakit veriliyor, maaşın parçası değil → ne brüte girer ne buraya.
    # Kural değişmeden önce yazılmış bonus kayıtları silinmedi, sadece dışarıda
    # bırakılıyor (bkz. daily_bonus_pay_ids).
    _dbp = daily_bonus_pay_ids(db, user_id, period)
    if _dbp:
        _ph = ",".join("?" * len(_dbp))
        paid_row = db.execute(
            "SELECT COALESCE(SUM(amount),0) as s FROM payments WHERE user_id=? AND period=? "
            f"AND id NOT IN ({_ph})",
            (user_id, period, *sorted(_dbp))).fetchone()
    else:
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
    # Fazla mesai: her vardiyada KAPANIŞ ANINDA dondurulmuş değerler toplanır
    # (geriye dönük DEĞİL — ayar sonradan değişse eski vardiyalar korunur).
    ot_bonus = sum((s["overtime"] if "overtime" in s.keys() else 0) or 0 for s in shifts)
    ot_hours = round(sum((s["overtime_h"] if "overtime_h" in s.keys() else 0) or 0 for s in shifts), 2)
    # Ürün bonusu (ödeme anında girilen satışlardan; product_sales tablosu) — gross'a eklenir.
    try:
        _pbrow = db.execute(
            "SELECT COALESCE(SUM(bonus),0) AS b, COALESCE(SUM(revenue),0) AS r "
            "FROM product_sales WHERE user_id=? AND period=?", (user_id, period)).fetchone()
        prod_bonus = _pbrow["b"] or 0
        prod_revenue = _pbrow["r"] or 0
    except Exception:
        prod_bonus, prod_revenue = 0, 0
    # Manuel düzeltmeler (Корректировка): owner'ın eklediği +/- mutabakat kalemleri.
    # Doğrudan net'e girer (gross'a değil): «−» borcu azaltır (zaten ödendi), «+» artırır.
    try:
        _adjrow = db.execute(
            "SELECT COALESCE(SUM(amount),0) AS a FROM adjustments WHERE user_id=? AND period=?",
            (user_id, period)).fetchone()
        adj_total = _adjrow["a"] or 0
        _adj_rows = db.execute(
            "SELECT * FROM adjustments WHERE user_id=? AND period=? ORDER BY created_at",
            (user_id, period)).fetchall()
    except Exception:
        adj_total, _adj_rows = 0, []
    # BARDAK BONUSU BRÜTE GİRMEZ: her gün kapanışta kasadan nakit ödeniyor,
    # ay sonu alacak sadece saatlik (+ переработка/товары/чаевые). Eskiden brüte
    # eklenip aynı tutarda «ödendi» kaydıyla geri düşülüyordu; ikisi birbirini
    # götürüyordu ama kasa raporu gelmeyen bir kapanışta ödeme kaydı hiç
    # oluşmuyor ve bonus kişinin alacağı olarak kalıyordu (gerçek fark).
    # `bonus` bilgi olarak yine döndürülür (vardiya detayı, kasa raporu).
    gross = hourly + tips_total + ot_bonus + prod_bonus
    net = gross - fine_total - paid_total + adj_total

    return {
        "period": period,
        "hours": hours,
        "bonus": bonus,
        "hourly": hourly,
        "overtime_hours": ot_hours,
        "overtime": ot_bonus,
        "product_bonus": prod_bonus,
        "product_revenue": prod_revenue,
        "fines": fine_total,
        "paid": paid_total,
        "tips": tips_total,
        "tips_count": len(tips),
        "tips_list": [dict(t) for t in tips],
        "adjustments": adj_total,
        "adjustments_list": [dict(a) for a in _adj_rows],
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


def _closing_override(db, bid):
    """meta'daki closing_owner_{bid} transfer override'ı (taze ise) → dict, yoksa None."""
    try:
        row = db.execute("SELECT val FROM meta WHERE k=?", (f"closing_owner_{int(bid or 1)}",)).fetchone()
        if not row or not row["val"]:
            return None
        d = json.loads(row["val"])
        at = datetime.fromisoformat(d.get("at"))
        if (datetime.now(TZ).replace(tzinfo=None) - at).total_seconds() > 18 * 3600:
            return None
        return d
    except Exception:
        return None


def closing_owner_uid(db, bid):
    """Şubenin kapatma sorumlusu: transfer override (kişi hâlâ aktifse) → yoksa ROL
    önceliği (владелец>бариста>ассистент; eşitlikte en erken başlayan). Yoksa None."""
    bid = int(bid or 1)
    ov = _closing_override(db, bid)
    if ov and ov.get("uid") and get_active_shift(db, int(ov["uid"])):
        return int(ov["uid"])
    rows = db.execute(
        "SELECT s.user_id AS uid, s.shift_role AS srole, s.start_time AS st, u.role AS urole "
        "FROM shifts s LEFT JOIN users u ON u.user_id=s.user_id "
        "WHERE COALESCE(s.branch_id,1)=? AND s.start_time IS NOT NULL AND s.end_time IS NULL",
        (bid,)).fetchall()
    def _prio(r):
        if (r["urole"] or "") == "owner":
            return 3
        if (r["srole"] or "barista") == "assistant":
            return 1
        return 2
    best = None
    for r in rows:
        p = _prio(r)
        if best is None or p > best[0] or (p == best[0] and (r["st"] or "") < (best[1] or "")):
            best = (p, r["st"], r["uid"])
    return best[2] if best else None


def _norm_amt(v):
    """AKILLI tutar normalizasyonu (harcama/kasa) — HER kaynak için (app/mini-app/API):
    boşluk/ayraç temizlenir; <1000 → binlik (×1000); ≥1000 → gerçek tutar (ASLA tekrar
    ×1000). İdempotent (normalize edilmiş değer ≥1000 olduğundan tekrar bozulmaz) →
    «82 → 82.000.000» bug'ı backend'de de imkânsız."""
    try:
        digits = re.sub(r"[^0-9]", "", str(v if v is not None else ""))
        n = int(digits) if digits else 0
    except Exception:
        n = 0
    return n * 1000 if (0 < n < 1000) else n


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
    # ── SNAPSHOT: rol + kategori + ставка başlangıçta dondurulur ──
    # Kategori/ücret sonradan değişse bu vardiya ASLA yeniden hesaplanmaz.
    _pi = barista_pay_info(db, user_id, branch_id=bid)
    # ROL = kişinin (bu şubedeki etkin) KATEGORİSİNİN rolü — HER ZAMAN. Owner kimi
    # stajyer atadıysa vardiya stajyer olarak başlar; toggle rolü ASLA değiştirmez
    # (toggle sadece o şubede asistan pozisyonu VAR/YOK der — slot_block_reason bakar).
    _role = _pi.get("slot_role") or "barista"
    # ── SIKI DEVİR (handover): aynı şube + aynı rol pozisyonunda İKİ vardiyanın ödenmiş
    # süresi ASLA çakışamaz. Önceki vardiya 17:05'te kapandıysa yeni vardiya 17:00'a
    # GERİYE YAZILAMAZ → başlangıç en erken 17:05'e kaydırılır. Günün ilk açılışını
    # engellemez (önceki bitiş start'tan önceyse dokunulmaz). Şube/rol dinamik — hardcode yok.
    try:
        _pe_row = db.execute(
            "SELECT end_time FROM shifts WHERE COALESCE(branch_id,1)=? "
            "AND COALESCE(shift_role,'barista')=? AND end_time IS NOT NULL "
            "ORDER BY end_time DESC LIMIT 1",
            (int(bid or 1), _role)).fetchone()
        if _pe_row and _pe_row["end_time"]:
            _pe = datetime.fromisoformat(_pe_row["end_time"])
            if start_dt < _pe:
                start_dt = min(_pe, now)  # devir anı; geleceğe taşmaz
                period = start_dt.strftime("%Y-%m")
    except Exception:
        pass
    cur = db.execute(
        "INSERT INTO shifts (user_id, hours, drinks, bonus, hourly_pay, total, date, period, created_at, start_time, end_time, note, branch_id, "
        "shift_role, cat_id, cat_name, rate) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (user_id, 0.0, json.dumps({}), 0, 0, 0,
         start_dt.strftime("%Y-%m-%d"), period, now.isoformat(),
         start_dt.isoformat(), None, "", bid,
         _role, _pi.get("cat_id"), _pi.get("cat_name") or "", int(_pi.get("rate") or 0)))
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
    _bid = active["branch_id"] if active["branch_id"] else user_branch_id(db, user_id)
    # ── DEVİR KLAMPESİ KALDIRILDI (kullanıcı isteği — vardiyalar TAMAMEN bağımsız) ──
    # Eskiden: bir vardiyanın bitişi, aynı şube+roldeki SONRAKİ vardiyanın başlangıcına
    # otomatik kısaltılıyordu (Hasan 03:00 kapatınca 20:00'a düşüyordu). ARTIK YOK.
    # Bitiş SADECE bu çalışanın gerçek kapanış saatidir (kendi «Завершить» veya owner'ın
    # elle kapatması). Başka bir çalışanın vardiyası ASLA otomatik değiştirilmez; aynı
    # şubede birden çok AÇIK vardiya (barista+barista, ассистент+barista vb.) serbesttir.
    # Ödenecek saat: ŞUBEYE ÖZEL kapalı pencere düşülür + max ile sınırlı.
    _pc = branch_pay_window(db, _bid)
    hours = paid_hours(start, end_dt, _pc)
    # Saatlik ücret: baristanın maaş kategorisinden (yoksa global). KPI kategorisi
    # ise satış (bardak+tatlı) bonusundan etkilenmez. Bardak bonus değerleri
    # kategorinin bonus sistemine göre: 'caffelito' preset'i veya 'own' (bizim/Цены).
    _pi = barista_pay_info(db, user_id, branch_id=_bid)
    # SNAPSHOT ставка: vardiya BAŞLANGICINDA donduruldu (shifts.rate). Varsa onu kullan —
    # kategori/ücret vardiya sırasında değişse bile bu vardiya başlangıçtaki kurallarla ödenir.
    # Eski açık vardiyalarda (snapshot'sız, bu build'den önce başlamış) mevcut bilgiye düşer.
    _snap_rate = None
    try:
        if "rate" in active.keys() and active["rate"] is not None and int(active["rate"]) > 0:
            _snap_rate = int(active["rate"])
    except Exception:
        _snap_rate = None
    _rate = _snap_rate if _snap_rate is not None else int(_pi["rate"])
    _bonus_prices = get_caffelito_bonus(db) if _pi["bonus_system"] == "caffelito" else get_prices(db)
    drinks_bonus = calc_bonus(drinks, _bonus_prices)
    dessert_bonus = calc_dessert_bonus(desserts, get_dessert_prices(db))
    if _pi["use_kpi"]:
        drinks_bonus = 0
        dessert_bonus = 0
    bonus = drinks_bonus + dessert_bonus
    hourly_pay = int(hours * _rate)
    total = hourly_pay + bonus
    # ── Fazla mesai (сверхурочные): KAPANIŞ ANINDA dondurulur (geriye dönük DEĞİL) ──
    # Norm VARDİYA BAŞINA (ör. 8ч/смена): bu vardiyanın ödenecek saati (kapalı
    # pencere zaten düşülmüş `hours`) normu aşarsa fark fazla mesai. thr=0 → kapalı.
    # Ayar sonradan değişse/kapansa bu vardiyanın saklanan değeri korunur.
    ot_shift, ot_h = 0, 0.0
    try:
        _otc = get_overtime_cfg(db)
        _thr = float(_otc.get("hours") or 0)
        if _otc.get("on", 1) and _thr > 0 and hours > _thr:
            ot_h = round(hours - _thr, 2)  # bu vardiyanın norm-üstü saati
            _val = int(_otc.get("value") or 0)
            if _otc.get("type") == "percent":
                ot_shift = int(ot_h * _rate * (_val / 100.0))
            else:
                ot_shift = int(ot_h * _val)
    except Exception:
        ot_shift, ot_h = 0, 0.0
    db.execute(
        "UPDATE shifts SET end_time=?, hours=?, drinks=?, bonus=?, hourly_pay=?, total=?, note=?, "
        "desserts=?, dessert_bonus=?, overtime=?, overtime_h=? WHERE id=?",
        (end_dt.isoformat(), hours, json.dumps(drinks or {}, ensure_ascii=False),
         bonus, hourly_pay, total, note or "",
         json.dumps(desserts or {}, ensure_ascii=False), dessert_bonus, ot_shift, ot_h, active["id"]))
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


def build_hash_payload(db, user_id, name, sel_period=None):
    """URL-hash payload string'ini (uid=...&role=...&summary=...) üretir.
    Hem klavye-butonu URL'i (build_webapp_url) hem de /api/state HTTP ucu (Yol B)
    bu aynı payload'ı kullanır — böylece ana ekrandan açınca da aynı veri gelir.

    sel_period ('YYYY-MM'): owner «Все сотрудники» listesi için SEÇİLİ AY. Sadece
    baristas listesini (maaş/vardiya/ödeme/düzeltme) etkiler — kullanıcının KENDİ
    `summary`si HER ZAMAN içinde bulunulan aydır (vardiya/kapatma akışı bozulmasın)."""
    from urllib.parse import quote
    # Geçersiz/boş → içinde bulunulan ay (savunmacı: dışarıdan gelen değer).
    _selp = sel_period if (isinstance(sel_period, str) and re.fullmatch(r"\d{4}-\d{2}", sel_period or "")) else current_period()
    upsert_user(db, user_id, name, None, None)
    role = get_role(db, user_id)
    s = calc_summary(db, user_id)
    # Owner tarafından atanan display_name varsa onu kullan
    show_name = display_name_for(db, user_id, fallback=name)
    prices = get_prices(db)
    summary = {
        "hours": s["hours"], "bonus": s["bonus"], "hourly": s["hourly"],
        "overtime_hours": s.get("overtime_hours", 0), "overtime": s.get("overtime", 0),
        "product_bonus": s.get("product_bonus", 0),
        "fines": s["fines"], "paid": s["paid"], "net": s["net"],
        "tips": s["tips"], "tips_count": s["tips_count"],
        "tips_list": s["tips_list"][-5:],
        "period": s["period"], "shifts_count": s["shifts_count"],
        "fines_count": s["fines_count"],
        "shifts": s["shifts"][-5:],
        "fines_list": s["fines_list"][-5:],
        "active": s["active"],
    }
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
    # Kasa: son raporun "Осталось"u → yeni "Было" ön-doldurma için.
    # ŞUBE-BAZLI: her filial kendi bardak stoğunu taşır (C5'in "Было"su C5'in son
    # vardiyasından, Magic'inki Magic'ten). Kullanıcının o anki şubesi (açık vardiya
    # → oturum şubesi → ev şubesi) baz alınır. Eski kayıtlarda branch_id NULL → 1 (C5).
    try:
        _kb = acting_branch_id(db, user_id)
        cr = db.execute(
            "SELECT ostalos FROM cashreports WHERE COALESCE(branch_id,1)=? ORDER BY id DESC LIMIT 1",
            (_kb,)).fetchone()
        kasa_last = json.loads(cr["ostalos"]) if (cr and cr["ostalos"]) else {}
    except Exception:
        kasa_last = {}
    # Kasa raporları listesi: owner hepsini, barista kendininkini görür
    try:
        # user_id: vardiya ile kasa raporunu KİŞİ üzerinden eşleştirmek için.
        # bylo/restock/ostalos: uygulamadaki «Сменный отчёт» gruba giden mesajın
        # aynısını göstersin diye (было +завоз → осталось = продано).
        _crcols = ("SELECT id,user_id,user_name,date,created_at,cups_total,itogo,click,payme,karta,terminal,"
                   "cashless,schitano,vyshlo,kassa,bylo,restock,ostalos,sold,expenses,daily_pay,hours,"
                   "start_time,end_time,note,branch_id,edits,edited_at,edited_by_name FROM cashreports ")
        if role == "owner":
            crs = db.execute(_crcols + "ORDER BY id DESC LIMIT 15").fetchall()
        else:
            crs = db.execute(_crcols + "WHERE user_id=? ORDER BY id DESC LIMIT 10", (user_id,)).fetchall()
        kasa_reports = [dict(r) for r in crs]
        # `edits` DB'de sınırsız büyür (her düzeltme eski+yeni tam kırılımı taşır).
        # Payload'a yalnızca SON 20 düzeltme gider — geçmişin tamamı DB'de kalır.
        for _kr in kasa_reports:
            try:
                _h = json.loads(_kr.get("edits") or "[]")
                if isinstance(_h, list) and len(_h) > 20:
                    _kr["edits"] = json.dumps(_h[-20:], ensure_ascii=False)
            except Exception:
                _kr["edits"] = ""
    except Exception:
        kasa_reports = []
    # ── Yedekleme durumu — SADECE owner ──
    # «Son yedek ne zaman alındı» sorusunun cevabı ekranda dursun; sessizce
    # çalışmayı bırakan bir yedekleme, hiç olmayan yedeklemeden beterdir.
    backup_info = {}
    if role == "owner":
        try:
            _lb = db.execute("SELECT val FROM meta WHERE k='last_backup_at'").fetchone()
            backup_info = {"at": (_lb["val"] if _lb else "") or "", "hour": BACKUP_HOUR}
        except Exception:
            backup_info = {}
    # ── Kayıtlı cihazlar (Устройства) — SADECE owner ──
    # Onay bekleyenler en üstte; sonra en son görülen.
    devices_out = []
    if role == "owner":
        try:
            for _d in db.execute(
                    "SELECT d.*, u.name AS uname, u.display_name AS udisp FROM devices d "
                    "LEFT JOIN users u ON u.user_id = d.user_id "
                    "ORDER BY (CASE WHEN COALESCE(d.revoked,0)=0 AND COALESCE(d.approved,0)=0 "
                    "THEN 0 ELSE 1 END), d.last_seen DESC LIMIT 60").fetchall():
                devices_out.append({
                    "id": _d["id"], "uid": _d["user_id"],
                    "n": (_d["udisp"] or _d["uname"] or "?"),
                    "dev": (_d["device_id"] or "")[:8],
                    "pf": _d["platform"] or "", "ap": int(_d["approved"] or 0),
                    "rv": int(_d["revoked"] or 0),
                    "first": _d["first_seen"] or "", "last": _d["last_seen"] or "",
                    "cnt": int(_d["seen_count"] or 0)})
        except Exception:
            devices_out = []
    # ── Denetim günlüğü (Журнал действий) — SADECE owner ──
    # `logs` tablosu yıllardır doluyordu ama hiçbir ekran göstermiyordu.
    # Başarılı girişler (login_ok) hariç: her açılışta yazılıyorlar ve gerçek
    # işlemleri listeden süpürüyorlar. Başarısız giriş (login_fail) KALIR.
    audit_logs = []
    if role == "owner":
        try:
            _lg = db.execute(
                "SELECT id,action,actor_id,actor_name,target_name,details,created_at FROM logs "
                "WHERE action != 'login_ok' ORDER BY id DESC LIMIT 80").fetchall()
            for _r in _lg:
                _d = _r["details"] or ""
                if len(_d) > 400:      # payload şişmesin; ekranda zaten özet gösteriliyor
                    _d = ""
                audit_logs.append({"id": _r["id"], "a": _r["action"], "au": _r["actor_id"],
                                   "an": _r["actor_name"] or "", "tn": _r["target_name"] or "",
                                   "d": _d, "at": _r["created_at"] or ""})
        except Exception:
            audit_logs = []
    # ── CLOSING OWNER guard: bu şube, benim vardiyam başladıktan SONRA zaten kapatıldı mı?
    #    (biri kasa raporu verdiyse tekrar kapatılmaz — rol-öncelikli sorumlu kapattı).
    branch_closed_today = 0
    closing_override = 0  # transfer ile atanmış kapatma sorumlusu uid (varsa)
    try:
        _actsh = get_active_shift(db, user_id)
        if _actsh and _actsh["start_time"]:
            _cbid = _actsh["branch_id"] or acting_branch_id(db, user_id)
            _cc = db.execute(
                "SELECT 1 FROM cashreports WHERE COALESCE(branch_id,1)=? AND created_at > ? LIMIT 1",
                (int(_cbid or 1), _actsh["start_time"])).fetchone()
            branch_closed_today = 1 if _cc else 0
            _cov = _closing_override(db, _cbid)
            if _cov and _cov.get("uid") and get_active_shift(db, int(_cov["uid"])):
                closing_override = int(_cov["uid"])
    except Exception:
        branch_closed_today = 0
    # Bu kullanıcının SON kapatması (kasa raporu created_at) — «Изменить закрытие» 10 dk penceresi.
    last_closing_at = ""
    try:
        _lc = db.execute("SELECT created_at FROM cashreports WHERE user_id=? ORDER BY id DESC LIMIT 1",
                         (user_id,)).fetchone()
        if _lc and _lc["created_at"]:
            last_closing_at = _lc["created_at"]
    except Exception:
        last_closing_at = ""
    # Ступени обслуживания — bu kullanıcı bugün onayladı mı?
    today_str = datetime.now(TZ).strftime("%Y-%m-%d")
    std_acked = bool(db.execute("SELECT 1 FROM std_acks WHERE user_id=? AND date=?", (user_id, today_str)).fetchone())
    # ── Филиалы (şubeler): owner hepsini (grup+aktiflik yönetim için), barista sadece aktif id+ad ──
    try:
        if role == "owner":
            branches_out = [{"id": b["id"], "name": b["name"], "group": b["group_chat_id"] or "",
                             "active": int(b["active"] or 0), "sort": b["sort_order"] or 0,
                             "open": (b["open_hour"] if b["open_hour"] is not None else 7),
                             "close": (b["close_hour"] if b["close_hour"] is not None else 3),
                             "unpaid": (b["unpaid_win"] if b["unpaid_win"] is not None else 1),
                             "trainee": int((b["trainee_enabled"] if "trainee_enabled" in b.keys() else 0) or 0)}
                            for b in get_branches(db, only_active=False)]
        else:
            # BARISTA'YA DA ÇALIŞMA SAATLERİ GİDER. Eskiden sadece {id, name}
            # gönderiliyordu; istemci eksik alanları GÖRÜNCE VARSAYILANA düşüyordu
            # (07:00–03:00 + «ödenmez») → kapanış ekranı, owner şubeyi ne yaparsa
            # yapsın HER ZAMAN 03:00–07:00 = tam 4 saat kesiyordu. Owner'da doğru
            # görünüp baristada görünmemesinin sebebi buydu.
            # Bunlar gizli veri değil: kişi kendi ödenen saatini doğru görmeli.
            branches_out = [{"id": b["id"], "name": b["name"],
                             "open": (b["open_hour"] if b["open_hour"] is not None else 7),
                             "close": (b["close_hour"] if b["close_hour"] is not None else 3),
                             "unpaid": (b["unpaid_win"] if b["unpaid_win"] is not None else 1),
                             "trainee": int((b["trainee_enabled"] if "trainee_enabled" in b.keys() else 0) or 0)}
                            for b in get_branches(db, only_active=True)]
    except Exception:
        branches_out = []
    my_branch = user_branch_id(db, user_id)
    # ── Zamanlı siparişler (bekleyen): barista kendininki, owner hepsi ──
    try:
        # `items` DE GÖNDERİLİR: uygulama planlı siparişi açtığında içindekileri
        # gösterebilsin. Eskiden yalnızca toplam gidiyordu; Nero listeyi boş
        # kuruyor ve ekran «0 позиций» görünüyordu — sipariş aslında doluydu.
        # `body` yedek: katalog değiştiyse anahtarlar çözülemese bile ne
        # sipariş edildiği metin olarak okunabilsin.
        if role == "owner":
            _srows = db.execute(
                "SELECT id,user_name,total,send_at,branch_id,items,body FROM scheduled_orders "
                "WHERE COALESCE(sent,0)=0 AND COALESCE(canceled,0)=0 ORDER BY send_at LIMIT 40").fetchall()
        else:
            _srows = db.execute(
                "SELECT id,user_name,total,send_at,branch_id,items,body FROM scheduled_orders "
                "WHERE user_id=? AND COALESCE(sent,0)=0 AND COALESCE(canceled,0)=0 ORDER BY send_at LIMIT 20",
                (user_id,)).fetchall()
        scheduled_out = []
        for r in _srows:
            try:
                _it = json.loads(r["items"] or "{}")
                if not isinstance(_it, dict):
                    _it = {}
            except Exception:
                _it = {}
            scheduled_out.append({"id": r["id"], "nm": r["user_name"] or "?",
                                  "total": r["total"] or 0, "at": r["send_at"],
                                  "bid": r["branch_id"] or 1, "items": _it,
                                  "body": (r["body"] or "")[:600]})
    except Exception:
        scheduled_out = []
    # Bu kullanıcının maaş bilgisi (canlı saatlik hesap kategorisine göre olsun).
    # ŞUBE-FARKINDA: aktif/seçili şubede kişi×şube ataması varsa onun kategorisi geçerli.
    _my_pi = barista_pay_info(db, user_id, branch_id=acting_branch_id(db, user_id))
    # ── SLOT durumu (şube başına pozisyon doluluğu) — client vardiya-başlatma ön kontrolü.
    # Kesin karar backend'de (slot_block_reason); bu sadece anında dostça uyarı için.
    slots_out = {}
    try:
        for _sb in get_branches(db, only_active=True):
            _sbid = _sb["id"]
            _spi = barista_pay_info(db, user_id, branch_id=_sbid)
            _str = branch_trainee_enabled(db, _sbid)
            slots_out[str(_sbid)] = {
                "trainee": 1 if _str else 0,
                # my_role HER ZAMAN kategoriden (toggle rolü değiştirmez)
                "my_role": _spi.get("slot_role") or "barista",
                "barista": slot_occupant(db, _sbid, "barista"),
                "assistant": slot_occupant(db, _sbid, "assistant"),
            }
    except Exception:
        slots_out = {}
    # Bu kullanıcının şubesinin ödeme penceresi (open/close/unpaid/max) — client
    # canlı vardiya saatini backend paid_hours ile AYNI hesaplasın (raw değil).
    _my_win = branch_pay_window(db, acting_branch_id(db, user_id))
    parts = [
        f"uid={user_id}",
        f"role={role}",
        f"name={quote(show_name or '')}",
        f"pwh={pwh}",
        f"std_ack={1 if std_acked else 0}",
        f"summary={quote(json.dumps(summary, ensure_ascii=False))}",
        f"prices={quote(json.dumps(prices, ensure_ascii=False))}",
        f"rt={quote(json.dumps(rt_self, ensure_ascii=False))}",
        f"exam={quote(json.dumps(pending_exam, ensure_ascii=False) if pending_exam else '')}",
        f"loans={quote(json.dumps(loans_data, ensure_ascii=False))}",
        f"kasa_last={quote(json.dumps(kasa_last, ensure_ascii=False))}",
        f"kasa_reports={quote(json.dumps(kasa_reports, ensure_ascii=False))}",
        f"audit={quote(json.dumps(audit_logs, ensure_ascii=False))}",
        f"devices={quote(json.dumps(devices_out, ensure_ascii=False))}",
        f"backup={quote(json.dumps(backup_info, ensure_ascii=False))}",
        f"my_branch_closed_today={branch_closed_today}",
        f"closing_override_uid={closing_override}",
        f"my_last_closing_at={quote(last_closing_at)}",
        f"sel_period={_selp}",
        f"branches={quote(json.dumps(branches_out, ensure_ascii=False))}",
        f"my_branch={my_branch}",
        # ÇALIŞILAN şube: açık vardiya → girişte seçilen oturum şubesi → ev şubesi.
        # my_branch yalnızca EV şubesidir; istemci onu kullanınca kullanıcının
        # seçtiği şube unutuluyor ve sipariş yanlış şubenin grubuna gidiyordu.
        f"acting_branch={acting_branch_id(db, user_id)}",
        f"scheduled={quote(json.dumps(scheduled_out, ensure_ascii=False))}",
        f"pay_cfg={quote(json.dumps(get_pay_cfg(db) if role=='owner' else {}, ensure_ascii=False))}",
        f"pay_rate={int(get_pay_cfg(db).get('rate', HOURLY_RATE))}",
        f"my_rate={int(_my_pi['rate'])}",
        f"my_cat={quote(json.dumps({'id': _my_pi['cat_id'], 'name': _my_pi['cat_name'], 'kpi': _my_pi['use_kpi'], 'does_kasa': _my_pi['does_kasa']}, ensure_ascii=False))}",
        f"my_does_kasa={int(_my_pi['does_kasa'])}",
        f"my_bonus_sys={_my_pi['bonus_system']}",
        f"my_pay_window={quote(json.dumps(_my_win, ensure_ascii=False))}",
        f"caffelito_bonus={quote(json.dumps(get_caffelito_bonus(db), ensure_ascii=False))}",
        f"sal_cats={quote(json.dumps(get_salary_categories(db) if role == 'owner' else [], ensure_ascii=False))}",
        f"products={quote(json.dumps(get_product_catalog(db) if role == 'owner' else get_product_catalog(db, only_active=True), ensure_ascii=False))}",
        f"my_product_ok={_my_pi['product_ok']}",
        f"prod_report={quote(json.dumps(get_product_report(db) if role == 'owner' else {}, ensure_ascii=False))}",
        f"ot_cfg={quote(json.dumps(get_overtime_cfg(db), ensure_ascii=False))}",
        f"slots={quote(json.dumps(slots_out, ensure_ascii=False))}",
        f"ts={ts}",
    ]
    # ── График смен: kaydedilen plan + выходной заявкаları geri gönderilir.
    #    shift_grid_set / dayoff_decide bunları YAZIYORDU ama payload OKUMUYORDU →
    #    uygulama her açılışta boş/örnek tablo gösteriyordu. Önceki, bu ve sonraki
    #    hafta (week -1/0/+1) taşınır; anahtar client'ın beklediği "uid-day" ya da
    #    "uid-wN-day" biçimidir. Her hata yolu boş döner — payload asla patlamaz.
    try:
        _grid, _wkmap = {}, {}
        for _w in (-1, 0, 1):
            _wkmap[grid_week_key(_w)] = _w
        _gq = ",".join("?" for _ in _wkmap)
        for _r in db.execute(
                f"SELECT week_key, day, user_id, code FROM shift_grid WHERE week_key IN ({_gq})",
                tuple(_wkmap.keys())).fetchall():
            _w = _wkmap.get(_r["week_key"], 0)
            _key = f"{_r['user_id']}-{_r['day']}" if _w == 0 else f"{_r['user_id']}-w{_w}-{_r['day']}"
            _grid[_key] = _r["code"] or "off"
    except Exception:
        _grid, _wkmap = {}, {}
    try:
        _reqs = [
            {"id": _r["id"], "who": _r["user_id"], "day": _r["day"],
             "note": _r["note"] or "", "status": _r["status"] or "pending",
             "week": _wkmap.get(_r["week_key"], 0)}
            for _r in db.execute(
                "SELECT id, user_id, week_key, day, note, status FROM dayoff_requests "
                "WHERE status='pending'" + ("" if role == "owner" else " AND user_id=?"),
                () if role == "owner" else (user_id,)).fetchall()]
    except Exception:
        _reqs = []
    parts.append(f"shift_grid={quote(json.dumps(_grid, ensure_ascii=False))}")
    parts.append(f"dayoff_reqs={quote(json.dumps(_reqs, ensure_ascii=False))}")
    # ── Vardiya şablonları · plan kuralları · açık vardiyalar ──
    # Üçü de eskiden YALNIZCA uygulamanın içinde sabitti (ya da hiç yoktu):
    # owner değiştiremiyordu ve uygulama yenilenince kayboluyordu.
    try:
        _tpl_out = {}
        for _t in db.execute("SELECT * FROM shift_templates ORDER BY sort_order, code").fetchall():
            _tb = get_branch(db, _t["branch_id"]) if _t["branch_id"] else None
            _tpl_out[_t["code"]] = {"b": (_tb["name"] if _tb else ""), "bid": _t["branch_id"],
                                    "s": _t["start_t"] or "", "e": _t["end_t"] or "",
                                    "act": int(_t["active"] or 0)}
    except Exception:
        _tpl_out = {}
    try:
        _caps = {str(_b["id"]): int(_b["max_staff"] if _b["max_staff"] is not None else 2)
                 for _b in db.execute("SELECT id, max_staff FROM branches").fetchall()}
    except Exception:
        _caps = {}
    try:
        _plim = {}
        for _u in db.execute("SELECT user_id, off_limit, off_self FROM users "
                             "WHERE COALESCE(archived,0)=0").fetchall():
            _plim[str(_u["user_id"])] = {"off": grid_off_limit(db, _u["user_id"]),
                                         "self": 0 if _u["off_self"] == 0 else 1}
    except Exception:
        _plim = {}
    _rules = {"weekly_off_limit": grid_off_limit(db, 0) if False else None, "caps": _caps, "people": _plim}
    try:
        _m = db.execute("SELECT val FROM meta WHERE k='weekly_off_limit'").fetchone()
        _rules["weekly_off_limit"] = int(_m["val"]) if (_m and _m["val"]) else 2
    except Exception:
        _rules["weekly_off_limit"] = 2
    try:
        _open_out = []
        for _o in db.execute(
                "SELECT * FROM open_shifts WHERE week_key IN ({}) AND status IN ('open','claimed') "
                "ORDER BY day".format(",".join("?" for _ in _wkmap)), tuple(_wkmap.keys())).fetchall():
            _open_out.append({"id": _o["id"], "week": _wkmap.get(_o["week_key"], 0), "day": _o["day"],
                              "code": _o["code"] or "", "bid": _o["branch_id"],
                              "from": _o["from_uid"], "from_nm": _o["from_name"] or "",
                              "st": _o["status"] or "open",
                              "cl": _o["claim_uid"], "cl_nm": _o["claim_name"] or "",
                              "why": _o["reason"] or ""})
    except Exception:
        _open_out = []
    parts.append(f"shift_tpl={quote(json.dumps(_tpl_out, ensure_ascii=False))}")
    parts.append(f"shift_rules={quote(json.dumps(_rules, ensure_ascii=False))}")
    parts.append(f"open_shifts={quote(json.dumps(_open_out, ensure_ascii=False))}")
    # ── Отчёт odaları için kayıtlar (owner: hepsi · barista: sadece kendi vardiya+sipariş) ──
    def _repq(sql, params=(), n=120):
        try:
            return db.execute(sql + f" LIMIT {n}", params).fetchall()
        except Exception:
            return []
    if role == "owner":
        _sh = _repq("SELECT s.id AS sid, s.start_time, s.end_time, s.hours, s.total, "
                    "COALESCE(s.hourly_pay,0) AS hourly_pay, COALESCE(s.bonus,0) AS bonus, "
                    "COALESCE(s.overtime,0) AS overtime, s.branch_id AS bid, "
                    "s.rate AS rate, s.cat_name AS cat_name, s.shift_role AS shift_role, "
                    "s.drinks AS drinks, s.note AS note, COALESCE(s.dessert_bonus,0) AS dessert_bonus, "
                    "COALESCE(u.display_name,u.name) AS nm, s.user_id AS uid "
                    "FROM shifts s LEFT JOIN users u ON u.user_id=s.user_id "
                    "WHERE s.start_time IS NOT NULL ORDER BY s.start_time DESC", (), 150)
        _or = _repq("SELECT id, user_name AS nm, items, created_at, branch_id AS bid FROM orders ORDER BY id DESC", (), 60)
        _ti = _repq("SELECT t.id, t.amount, t.note, t.created_at, t.period AS per, COALESCE(u.display_name,u.name) AS nm "
                    "FROM tips t LEFT JOIN users u ON u.user_id=t.user_id ORDER BY t.id DESC", (), 60)
        # KENDİNE ÖDEME DIŞLANMIYOR ARTIK. Eski `p.paid_by != p.user_id` filtresi
        # kaba bir aletti: amacı kural değişmeden önce yazılmış GÜNLÜK BARDAK
        # BONUSU kayıtlarını listeden çıkarmaktı (onlar da paid_by=user_id'dir),
        # ama aynı anda owner'ın KENDİNE yaptığı GERÇEK maaş ödemelerini de
        # siliyordu → «Выплачено» listesinde herkes vardı, owner yoktu ve
        # işletmeden çıkan toplam para eksik görünüyordu.
        # Artık kendine ödemeler de geliyor; yalnızca bonus imzasına UYAN kayıtlar
        # (daily_bonus_pay_ids — aynı kişi, tutar vardiyanın bonusuyla birebir,
        # ödeme anı vardiya kapanışının ±2 saatinde) ayıklanıyor.
        _pa = _repq("SELECT p.id, p.amount, p.kind, p.note, p.paid_at, p.period AS per, p.user_id AS puid, "
                    "COALESCE(u.display_name,u.name) AS nm "
                    "FROM payments p LEFT JOIN users u ON u.user_id=p.user_id "
                    "ORDER BY p.id DESC", (), 120)
        try:
            _skip = set()
            _seen = set()
            for _r in _pa:
                _u = _r["puid"]
                _pr = str(_r["paid_at"] or "")[:7]
                if not _u or not _pr or (_u, _pr) in _seen:
                    continue
                _seen.add((_u, _pr))
                _skip |= set(daily_bonus_pay_ids(db, _u, _pr) or [])
            if _skip:
                _pa = [_r for _r in _pa if _r["id"] not in _skip]
            _pa = _pa[:60]
        except Exception as e:
            logger.warning(f"pays bonus filtresi: {e}")
            _pa = _pa[:60]
        _fi = _repq("SELECT f.id, f.amount, f.reason, f.created_at, f.period AS per, COALESCE(u.display_name,u.name) AS nm "
                    "FROM fines f LEFT JOIN users u ON u.user_id=f.user_id ORDER BY f.id DESC", (), 60)
        _lo = _repq("SELECT l.id, l.amount, l.reason, l.status, l.created_at, COALESCE(u.display_name,u.name) AS nm "
                    "FROM loans l LEFT JOIN users u ON u.user_id=l.barista_id ORDER BY l.id DESC", (), 60)
    else:
        _sh = _repq("SELECT s.id AS sid, s.start_time, s.end_time, s.hours, s.total, "
                    "COALESCE(s.hourly_pay,0) AS hourly_pay, COALESCE(s.bonus,0) AS bonus, "
                    "COALESCE(s.overtime,0) AS overtime, s.branch_id AS bid, "
                    "s.rate AS rate, s.cat_name AS cat_name, s.shift_role AS shift_role, "
                    "s.drinks AS drinks, s.note AS note, COALESCE(s.dessert_bonus,0) AS dessert_bonus, "
                    "? AS nm, s.user_id AS uid FROM shifts s "
                    "WHERE s.user_id=? AND s.start_time IS NOT NULL ORDER BY s.start_time DESC",
                    (show_name, user_id), 90)
        _or = _repq("SELECT id, user_name AS nm, items, created_at, branch_id AS bid FROM orders WHERE user_id=? ORDER BY id DESC",
                    (user_id,), 50)
        _ti = _pa = _fi = _lo = []
    rep = {
        "shifts": [{"sid": r["sid"], "nm": r["nm"] or "?", "uid": r["uid"], "start_time": r["start_time"], "end_time": r["end_time"], "hours": r["hours"] or 0, "total": r["total"] or 0, "hourly_pay": r["hourly_pay"] or 0, "bonus": r["bonus"] or 0, "overtime": r["overtime"] or 0, "bid": r["bid"] or 1, "rate": r["rate"], "cat_name": r["cat_name"] or "", "shift_role": r["shift_role"] or "", "drinks": r["drinks"] or "{}", "note": r["note"] or "", "dessert_bonus": r["dessert_bonus"] or 0} for r in _sh],
        "orders": [{"id": r["id"], "nm": r["nm"] or "?", "items": r["items"] or "", "at": r["created_at"], "bid": r["bid"] or 1} for r in _or],
        "tips": [{"id": r["id"], "nm": r["nm"] or "?", "amount": r["amount"] or 0, "note": r["note"] or "", "at": r["created_at"], "per": r["per"] or ""} for r in _ti],
        "pays": [{"id": r["id"], "nm": r["nm"] or "?", "amount": r["amount"] or 0, "kind": r["kind"] or "", "note": r["note"] or "", "at": r["paid_at"], "per": r["per"] or ""} for r in _pa],
        "fines": [{"id": r["id"], "nm": r["nm"] or "?", "amount": r["amount"] or 0, "reason": r["reason"] or "", "at": r["created_at"], "per": r["per"] or ""} for r in _fi],
        "loans": [{"id": r["id"], "nm": r["nm"] or "?", "amount": r["amount"] or 0, "reason": r["reason"] or "", "status": r["status"] or "", "at": r["created_at"]} for r in _lo],
    }
    parts.append(f"rep={quote(json.dumps(rep, ensure_ascii=False))}")
    if role == "owner":
        rows = db.execute(
            "SELECT user_id, name, username, role, display_name, password, authorized, "
            "COALESCE(archived,0) AS archived, archived_at, COALESCE(branch_id,1) AS branch_id, "
            "salary_cat_id "
            "FROM users WHERE COALESCE(approved,0)=1 "
            "ORDER BY COALESCE(archived,0), COALESCE(display_name,name)").fetchall()
        baristas = []
        for b in rows:
            # SEÇİLİ AY (_selp): owner geçmiş ayın maaşını görebilsin/ödeyebilsin.
            bs = calc_summary(db, b["user_id"], _selp)
            # Bu baristanın SEÇİLİ AYdaki bitmiş vardiyaları (owner "kim, ne zaman çalıştı" görsün)
            _rsh = db.execute(
                "SELECT id, start_time, end_time, hours, hourly_pay, bonus, "
                "COALESCE(overtime,0) AS ot FROM shifts "
                "WHERE user_id=? AND period=? AND end_time IS NOT NULL "
                "ORDER BY COALESCE(start_time, created_at) DESC LIMIT 40",
                (b["user_id"], _selp)).fetchall()
            _recent = [{"sid": r["id"], "start_time": r["start_time"], "end_time": r["end_time"],
                        "hours": r["hours"] or 0, "hp": r["hourly_pay"] or 0,
                        "b": r["bonus"] or 0, "ot": r["ot"] or 0} for r in _rsh]
            # Bu ayki ödeme kayıtları — owner yanlış/fazla ödemeyi buradan görüp siler (balans düzelir).
            # Günlük bardak bonusu kayıtları LİSTEYE GİRMEZ: maaş ödemesi değil, kasadan
            # verilen nakit. Karışınca «bir tanesi eksik» gibi görünüyordu (bkz. daily_bonus_pay_ids).
            _dbp_b = daily_bonus_pay_ids(db, b["user_id"], _selp)
            _pays = db.execute(
                "SELECT id, amount, paid_at FROM payments WHERE user_id=? AND period=? ORDER BY id DESC",
                (b["user_id"], _selp)).fetchall()
            _pay_list = [{"id": r["id"], "amount": r["amount"] or 0, "at": r["paid_at"]}
                         for r in _pays if r["id"] not in _dbp_b]
            # Bu ayki manuel düzeltmeler (Корректировка) — kişi kartında gösterilir/silinir
            try:
                _adjs = db.execute(
                    "SELECT id, amount, note, created_at FROM adjustments WHERE user_id=? AND period=? ORDER BY id DESC",
                    (b["user_id"], _selp)).fetchall()
                _adj_list = [{"id": r["id"], "amount": r["amount"] or 0, "note": r["note"] or "", "at": r["created_at"]} for r in _adjs]
            except Exception:
                _adj_list = []
            # Kişi × şube kategori atamaları (owner UI: «bu şubede farklı kategori»)
            try:
                _broles = {str(r["branch_id"]): r["salary_cat_id"] for r in db.execute(
                    "SELECT branch_id, salary_cat_id FROM branch_staff WHERE user_id=?",
                    (b["user_id"],)).fetchall()}
            except Exception:
                _broles = {}
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
                "adj": bs["adjustments"],
                "sc": bs["shifts_count"], "fc": bs["fines_count"],
                "active": bs["active"],
                "bid": b["branch_id"] or 1,
                "cat": b["salary_cat_id"],
                "recent": _recent,
                "pays": _pay_list,
                "adjs": _adj_list,
                "broles": _broles,
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
                    "u": p["username"] or "", "at": p["created_at"] or "", "req": 0} for p in pend_rows]
        # ERİŞİM İSTEYENLER de bu listeye girer. Owner bir baristanın PIN'ini
        # kaldırınca kişi kilitlenir AMA `approved` 1 kalır → yukarıdaki sorgu
        # onu HİÇ görmez. Sonuç: Telegram'a haber gidiyordu, Nero'da hiçbir şey
        # çıkmıyordu. Ölçüt «onaylı mı» değil, GERÇEKTEN GİREBİLİYOR MU.
        # Kişi girebilir hâle gelince (PIN verilince) listeden kendiliğinden düşer.
        try:
            _seen = {p["id"] for p in pending}
            for _r in db.execute(
                    "SELECT u.user_id AS uid, u.name AS nm, u.username AS un, m.val AS at "
                    "FROM meta m JOIN users u ON ('accessreq_' || u.user_id) = m.k "
                    "WHERE m.k LIKE 'accessreq_%' AND COALESCE(u.archived,0)=0 "
                    "AND COALESCE(u.role,'') != 'owner' ORDER BY m.val DESC LIMIT 40").fetchall():
                if _r["uid"] in _seen or nero_access_ok(db, _r["uid"]):
                    continue
                _seen.add(_r["uid"])
                pending.append({"id": _r["uid"], "n": _r["nm"] or "?",
                                "u": _r["un"] or "", "at": _r["at"] or "", "req": 1})
        except Exception as e:
            logger.warning(f"access istekleri listelenemedi: {e}")
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


def _nero_flags():
    """flags.json'u 60 sn cache ile getir.

    ÖNCE YEREL DOSYA (nero/flags.json). Kendi sunucumuza HTTP atmak YASAK:
    bot ile aiohttp aynı event loop'ta çalışıyor; urlopen bloklayıcı olduğu için
    loop kilitlenir, sunucu kendi isteğine cevap veremez → her seferinde timeout
    → config None → herkes eski uygulamada kalır. Diskten okumak anında ve güvenli.
    Yerel dosya yoksa (ör. Gist'te barındırılıyorsa) URL'den çekilir."""
    import time, urllib.request
    now = time.time()
    if _nero_cache["cfg"] is not None and now - _nero_cache["at"] <= 60:
        return _nero_cache["cfg"]
    local = os.path.join(os.path.dirname(os.path.abspath(__file__)), "nero", "flags.json")
    try:
        if os.path.isfile(local):
            with open(local, "r", encoding="utf-8") as fh:
                _nero_cache["cfg"] = json.load(fh)
                _nero_cache["at"] = now
                return _nero_cache["cfg"]
    except Exception as e:
        logger.warning(f"nero flags local read failed: {e}")
    if NERO_FLAGS_URL:
        try:
            with urllib.request.urlopen(NERO_FLAGS_URL, timeout=3) as r:
                _nero_cache["cfg"] = json.loads(r.read().decode("utf-8"))
                _nero_cache["at"] = now
        except Exception as e:
            logger.warning(f"nero flags fetch failed: {e}")
    return _nero_cache["cfg"]


def nero_app_url():
    """Nero'nun SABİT adresi: `<domain>/app` → her zaman `nero/index.html`.

    Sürüm klasörü YOK. Eskiden her değişiklik için `nero/<tarih-N>/` klasörü
    üretilip Railway'de `NERO_WEBAPP_URL` elle güncelleniyordu; bu, her küçük
    düzeltmeyi iki adımlık bir işe çeviriyordu. Artık yeni sürüm =
    `nero/index.html`'i değiştir + push. Geri alma = git'te o dosyayı geri al.

    `NERO_WEBAPP_URL` artık sürüm seçmek için KULLANILMIYOR; yalnızca uygulamayı
    başka bir yerden servis etmek gerekirse (tam http(s) adresi) devreye girer.
    """
    env = (NERO_WEBAPP_URL or "").strip()
    if env.startswith("http") and "/nero/" not in env:
        return env                       # açık dış adres (acil durum)
    return (WEBAPP_URL or "").rstrip("/") + "/app"


def nero_base_url(user_id, db=None):
    """Bu kullanıcı Nero'yu mu görecek? Evet → Nero adresi, hayır/şüphe → None.
    Öncelik: kill > deny.user > deny.branch > allow.user > allow.branch > yüzde > None.
    HER hata yolu None döner (fail-closed) → eski uygulama."""
    cfg = _nero_flags()
    if not cfg:
        logger.info("NERO kapali: flags.json okunamadi")
        return None
    try:
        uid = int(user_id)
        allow = cfg.get("allow") or {}
        deny = cfg.get("deny") or {}
        allow_u = [int(x) for x in (allow.get("users") or [])]
        deny_u = [int(x) for x in (deny.get("users") or [])]

        bid = None
        if db is not None:
            try:
                bid = int(acting_branch_id(db, uid))
            except Exception:
                bid = None

        # Tek karar noktası + TEK log satırı: hangi uid geldi, hangi listede var, sonuç ne.
        if cfg.get("kill") is True:
            res, why = None, "kill-switch"
        elif uid in deny_u:
            res, why = None, "deny.user"
        elif bid is not None and bid in [int(x) for x in (deny.get("branches") or [])]:
            res, why = None, "deny.branch"
        elif uid in allow_u:
            res, why = nero_app_url(), "allow.user"
        elif bid is not None and bid in [int(x) for x in (allow.get("branches") or [])]:
            res, why = nero_app_url(), "allow.branch"
        else:
            pct = int((cfg.get("rollout") or {}).get("percent") or 0)
            res, why = None, "listede-yok"
            if pct > 0:
                # nero-flags.js ile AYNI FNV-1a bucket — pult ile bot aynı kararı vermeli
                h = 2166136261
                for ch in str(uid):
                    h ^= ord(ch)
                    h = (h + ((h << 1) + (h << 4) + (h << 7) + (h << 8) + (h << 24))) & 0xFFFFFFFF
                if (h % 100) < min(100, pct):
                    res, why = nero_app_url(), f"rollout-{pct}%"
        logger.info(f"NERO uid={uid} bid={bid} allow={allow_u} sonuc={'NERO' if res else 'ESKI'} ({why})")
        return res
    except Exception as e:
        logger.warning(f"nero_base_url failed: {e}")
        return None


def build_webapp_url(base_url, user_id, name, db):
    """Yol B: URL'e DEV hash GÖMÜLMEZ. State artık HTTP /api/state'ten geliyor.
    Hash'i gömmek owner'da (çok barista) Telegram buton-URL limitini aşıyordu
    ('Слишком много данных' hatası) ve URL kırpılınca aktif vardiya kayboluyordu.
    Sadece cache-buster ?v= ekliyoruz ki her açılışta TAZE HTML yüklensin."""
    base_url = nero_base_url(user_id, db) or base_url
    ts = int(datetime.now(TZ).timestamp())
    sep = "&" if "?" in base_url else "?"
    url = base_url + f"{sep}v={ts}"
    # ── KİMLİK JETONU (initData yedeği) ──────────────────────────────────────────
    # Telegram bu bot için tgWebAppData göndermiyor → initData boş. Kimliği FRAGMENT'e
    # gömüyoruz (#t=): fragment sunucuya gitmez; uygulama okuyup /api/state ve
    # /api/action'a `token=` olarak POST eder. Hata olursa jetonsuz URL döner.
    try:
        if BOT_TOKEN:
            url += f"#t={make_web_token(user_id)}"
    except Exception as e:
        logger.warning(f"web token embed failed for {user_id}: {e}")
    return url

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

    role_now = get_role(db, user.id)
    if auto_owner:
        msg = "👑 Вы — владелец. Caffelito готов."
    elif role_now == "owner":
        msg = "☕ Caffelito готов."
    else:
        msg = "☕ Caffelito."
    # ── OWNER: kalıcı web_app KLAVYE BUTONU (kritik) ──────────────────────────────
    # Owner'ın menü butonu MenuButtonCommands (komut listesi) olarak KALIR — ona
    # dokunmuyoruz. Ama owner'da başka hiçbir web_app girişi yoktu: Nero'yu sadece
    # inline butondan açabiliyordu ve Telegram INLINE modda `tgWebAppData` GÖNDERMEZ
    # (teşhis: hasData=NO, keys=...tgWebAppBotInline...) → initData boş → uygulama
    # demo veriye düşüyordu. Çözüm: /start'ta web_app'li ReplyKeyboardMarkup ver →
    # bu butondan açılınca initData dolu gelir (gerçek veri + tg.sendData çalışır).
    _kb = None
    if role_now == "owner" and WEBAPP_URL:
        try:
            _u = build_webapp_url(WEBAPP_URL, user.id, user.first_name, db)
            _kb = ReplyKeyboardMarkup(
                [[KeyboardButton("☕ Nero", web_app=WebAppInfo(url=_u))]],
                resize_keyboard=True)
        except Exception as e:
            logger.warning(f"start owner webapp keyboard failed: {e}")
            _kb = None
    # Barista akışı AYNEN: ≡ menü butonu WebApp (sync_user_ui) → klavye temizlenir.
    try:
        await update.message.reply_text(msg, reply_markup=(_kb or ReplyKeyboardRemove()))
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
            f"⏱️ Часы: *{s['hours']:g}* ч → *{fmt_sum(s['hourly'])}* сум\n"
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
    """Sipariş mesajını gruba gönder (uzunsa parçalara bölerek). Задачи gibi DÜZ
    biçimli liste (kod-bloğu yok). esc_lines zaten HTML (bullet + <b> içerir)."""
    full = header + "\n".join(esc_lines) + footer
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
        msg = (header if i == 0 else "") + "\n".join(b) + (footer if i == n - 1 else "")
        await bot.send_message(chat_id=int(group_id), text=msg, parse_mode="HTML")


async def handle_webapp_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Mini App'ten gelen veriyi işle ve gruba ilet"""
    logger.info("=== WEBAPP DATA RECEIVED ===")
    db = get_db()
    user = update.effective_user
    upsert_user(db, user.id, user.first_name, user.username, update.effective_chat.id)
    # Mini App'ten gelen HER eylem aynı kapıdan geçer (okuma /api/state'te aynı
    # kontrolü yapıyor). Onay bekleyen (approved=0) kimse işlem yapamaz.
    # TEK İSTİSNA: «доступ isteği». Kilitli ekrandaki kişi owner'a haber
    # gönderebilmeli — kapının ARDINDAN hiçbir veri almadan. Başka her eylem
    # kapıdan geçer.
    _early = ""
    try:
        _early = (json.loads(update.effective_message.web_app_data.data) or {}).get("action") or ""
    except Exception:
        _early = ""
    if _early != "access_request" and not nero_access_ok(db, user.id):
        logger.info(f"NERO eylem reddedildi uid={user.id}")
        await update.message.reply_text(
            "🔒 У вас нет доступа. Попросите владельца подтвердить вас и назначить PIN-код.",
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

        # CİHAZ KAPISI — okuma tarafıyla (/api/state) aynı kural. Onaylanmamış
        # cihazdan gelen EYLEM işlenmez; yoksa kişi veriyi göremez ama yine de
        # vardiya başlatıp kasa gönderebilirdi.
        _dg = device_gate(db, user.id, str(data.get("device") or "")[:64],
                          platform=str(data.get("dev_platform") or "")[:32])
        if action == "access_request":
            _dg = "ok"   # erişim isteği cihaz onayından da önce gelir
        if _dg in ("new", "pending", "revoked"):
            logger.info(f"NERO cihaz eylemi reddedildi uid={user.id} durum={_dg}")
            await update.message.reply_text(
                "📱 Это устройство не подтверждено. Обратитесь к владельцу."
                if _dg != "revoked" else
                "📱 Доступ с этого устройства отключён владельцем.")
            return

        # Rapor grubu: kullanıcının açık vardiyasının / ev şubesinin grubu (çok şube).
        # Tek şubede branch 1'in grubu = eski active_group olduğundan davranış değişmez.
        group_id = resolve_group_id(db, user.id, context)
        # Uygulama hangi ŞUBE için işlem yaptığını AÇIKÇA bildiriyorsa (Заказ ekranındaki
        # şube seçimi gibi) rapor O şubenin grubuna gitsin. Önceden sipariş her zaman
        # «oturum şubesinin» grubuna düşüyordu: kullanıcı C5 seçse bile Magic'e gidiyordu.
        # Payload şube taşımıyorsa (eski uygulama) davranış hiç değişmez.
        _payload_bid = None
        try:
            _pb = data.get("branch_id")
            if _pb is None or str(_pb).strip() == "":
                _pb = data.get("branch")
            if _pb is not None and str(_pb).strip() != "" and get_branch(db, _pb):
                _payload_bid = int(_pb)
                group_id = resolve_group_id(db, user.id, context, branch_id=_payload_bid)
        except Exception:
            _payload_bid = None
        # TEŞHİS: grup mesajı gitmiyorsa ilk bakılacak satır — grup GERÇEKTEN çözüldü mü?
        # (None ise şubede group_chat_id yok + active_group/GROUP_CHAT_ID de boş demektir.)
        logger.info(f"WEBAPP action={action} uid={user.id} branch={acting_branch_id(db, user.id)} "
                    f"payload_branch={_payload_bid} group_id={group_id}")

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

            # Задачи gibi temiz liste: her satır TAMAMEN bold, miktar "Nx" formatında
            esc_lines = []
            for r in rows:
                if r is None:
                    esc_lines.append("")  # kategori arası boşluk
                    continue
                nm, q = r
                if q is None:
                    esc_lines.append(f"<b>{esc_html(nm)}</b>")  # miktarsız satır (başlık gibi)
                else:
                    qn = q[:-1] if str(q).endswith("x") else q  # "3x" → "3"
                    esc_lines.append(f"• <b>{esc_html(nm)} — {esc_html(str(qn))}x</b>")

            # ── Zamanlı sipariş mı? (send_at gelecekteyse gruba ŞİMDİ gönderme, sakla) ──
            _sa_raw = (data.get("send_at") or "").strip()
            _sched = _parse_user_time(_sa_raw) if _sa_raw else None
            if _sched and _sched > datetime.now(TZ).replace(tzinfo=None) + timedelta(minutes=1):
                _bid = _payload_bid or acting_branch_id(db, user.id)
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

            _DIV = "━━━━━━━━━━━━━━━━━━━━"
            header = (f"<b>📦 ЗАКАЗ — CAFFELITO</b>\n{_DIV}\n"
                      f"👤 <b>{esc_html(shown)}</b>   ·   {now.strftime('%d.%m.%Y  %H:%M')}\n{_DIV}\n")
            footer = f"\n{_DIV}\n<b>Итого: {total} позиций</b>"
            if group_id:
                try:
                    await deliver_order(context.bot, group_id, header, esc_lines, footer)
                    logger.info("Order forwarded to group OK")
                except Exception as e:
                    logger.exception(f"GROUP FORWARD FAILED (group_id={group_id}): {e}")
            # Siparişi DB'ye kaydet (Отчёт → Заказы odası)
            try:
                _dbo = get_db()
                _dbo.execute(
                    "INSERT INTO orders (chat_id, user_id, user_name, items, created_at, branch_id) VALUES (?,?,?,?,?,?)",
                    (int(group_id) if group_id else 0, user.id, shown,
                     json.dumps(order_items, ensure_ascii=False), now.isoformat(),
                     _payload_bid or acting_branch_id(_dbo, user.id)))
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
                    logger.exception(f"GROUP FORWARD FAILED (group_id={group_id}): {e}")

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
            # ── SLOT kontrolü: bu şubede rol pozisyonu doluysa başlatma (dostça mesaj) ──
            _bid_chk = None
            try:
                if sel_branch and get_branch(db, sel_branch):
                    _bid_chk = int(sel_branch)
            except Exception:
                _bid_chk = None
            if not _bid_chk:
                _bid_chk = user_branch_id(db, user.id)
            # ── PARALEL VARDİYA: aynı rol pozisyonu DOLUYSA artık otomatik engellemez.
            # Kullanıcı client'ta seçti (shift_mode=takeover|parallel) → "занята" engeli
            # bypass edilir; her ikisi de BAĞIMSIZ yeni vardiya başlatır (kayıt birleşmez,
            # kimse kapanmaya zorlanmaz). «Нет позиции» (asistan pozisyonu yok) bypass EDİLMEZ.
            _mode = (data.get("shift_mode") or "").strip()
            _blk = slot_block_reason(db, user.id, _bid_chk)
            if _blk:
                _bypass = (_mode in ("takeover", "parallel")) and ("Нет позиции" not in _blk)
                if not _bypass:
                    await update.message.reply_text(_blk)
                    await refresh_webapp_keyboard(update, context, db, user,
                        "🔄 Обновите приложение — смена не была начата 👇")
                    return
            sh = start_shift(db, user.id, custom_start=custom_start, branch_id=sel_branch)
            if _mode in ("takeover", "parallel"):
                try:
                    db.execute("UPDATE shifts SET note=? WHERE id=?",
                               ("параллельная смена" if _mode == "parallel" else "передача смены", sh["id"]))
                    db.commit()
                except Exception:
                    pass
            # Vardiya artık açık → duyuru bu vardiyanın şubesinin grubuna gitsin.
            group_id = resolve_group_id(db, user.id, context, branch_id=sh["branch_id"])
            start_dt = datetime.fromisoformat(sh["start_time"])
            note_back = ""
            # «Ручное время» notu SADECE gerçekten geçmiş bir saat girildiğinde. Uygulama
            # artık normal başlatmada da start_time (dokunuş anı) gönderiyor → şimdiye yakın
            # (≤3 dk) ise bu normal başlatmadır, «вручную» yazma.
            _near_now = False
            if custom_start:
                try:
                    _req0 = _parse_user_time(custom_start)
                    if _req0:
                        _near_now = abs((datetime.now(TZ).replace(tzinfo=None) - _req0).total_seconds()) <= 180
                except Exception:
                    _near_now = False
            if custom_start and not _near_now:
                note_back = f"\n_(время указано вручную)_"
                # Devir kaydırması olduysa şeffafça söyle (istenen saat < kaydedilen saat)
                try:
                    _req = _parse_user_time(custom_start)
                    if _req and _req < start_dt:
                        note_back = (f"\n_(время скорректировано: предыдущая смена на этой позиции "
                                     f"закрылась в {start_dt.strftime('%H:%M')} — передача смены)_")
                except Exception:
                    pass
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
                    logger.exception(f"GROUP FORWARD FAILED (group_id={group_id}): {e}")

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
            _rate = barista_pay_info(db, user.id)["rate"]  # kategorinin gerçek ставка'sı
            _bname = (get_branch(db, sh["branch_id"]) or {}).get("name", "") if sh["branch_id"] else ""
            s = calc_summary(db, user.id, period)
            # DM cevabı: kişisel — saatlik dahil net
            text = (f"🔴 *Смена закрыта!*"
                    + (f" · 🏢 {_bname}" if _bname else "") + "\n"
                    f"━━━━━━━━━━━━━━━━━━\n"
                    f"⏰ {start_dt.strftime('%H:%M')} → {end_dt.strftime('%H:%M')}  ({fmt_hm(hours)})\n"
                    f"🥤 Напитков: *{cups}* шт · 💰 {fmt_sum(drinks_bonus)} сум\n")
            if sweets:
                text += f"🍰 Десерты: *{sweets}* шт · 💰 {fmt_sum(dessert_bonus)} сум\n"
            text += (f"💵 Часы ({fmt_sum(_rate)}/ч): {fmt_sum(hourly_pay)} _(в конце месяца)_\n")
            # Fazla mesai — saatlik ücret (fixed=сум, percent=ставка'nın %'i)
            _otc = get_overtime_cfg(db)
            _ot_perh = int(_rate * (_otc.get("value") or 0) / 100.0) if _otc.get("type") == "percent" else int(_otc.get("value") or 0)
            if (sh["overtime"] or 0) > 0:
                text += (f"⏱ Переработка (эта смена): {fmt_hm(sh['overtime_h'])} × "
                         f"{fmt_sum(_ot_perh)}/ч = *+{fmt_sum(sh['overtime'])}* сум\n")
            text += (f"💎 За смену: *{fmt_sum(total + (sh['overtime'] or 0))}* сум\n"
                     f"━━━━━━━━━━━━━━━━━━\n"
                     f"📊 *Месяц {period}:*\n"
                     f"Часы: {fmt_hm(s['hours'])} | Смен: {s['shifts_count']}\n")
            if (s.get('overtime') or 0) > 0:
                text += (f"⏱ Переработка за месяц ({fmt_hm(s['overtime_hours'])} × "
                         f"{fmt_sum(_ot_perh)}/ч): +{fmt_sum(s['overtime'])} сум\n")
            text += f"💎 *НЕТТО: {fmt_sum(s['net'])} сум*"
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
                    logger.exception(f"GROUP FORWARD FAILED (group_id={group_id}): {e}")
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
            _pi = barista_pay_info(db, user.id)
            _bp = get_caffelito_bonus(db) if _pi["bonus_system"] == "caffelito" else get_prices(db)
            bonus = 0 if _pi["use_kpi"] else calc_bonus(drinks, _bp)
            hourly_pay = int(hours * int(_pi["rate"]))
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
                    f"⏱️ {hours:g}h × {fmt_sum(_pi['rate'])} = *{fmt_sum(hourly_pay)}* сум\n"
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
                    logger.exception(f"GROUP FORWARD FAILED (group_id={group_id}): {e}")

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
            # ── GRUP BİLDİRİMİ (штраф) — eskiden YOKTU: ceza sadece kişiye DM gidiyordu ──
            if group_id and sent_to:
                try:
                    from html import escape as esc_html
                    _who = ", ".join(esc_html(display_name_for(db, t["user_id"], fallback="?")) for t in sent_to)
                    gtext = (f"⚠️ <b>ШТРАФ</b>\n"
                             f"👤 {_who}\n"
                             f"💸 -{fmt_sum(per_target)} сум"
                             + (f" × {len(sent_to)} чел." if (split and len(sent_to) > 1) else "") + "\n"
                             f"📝 {esc_html(reason)}\n"
                             f"👮 {esc_html(shown)}")
                    await context.bot.send_message(chat_id=int(group_id), text=gtext, parse_mode="HTML")
                except Exception as e:
                    logger.exception(f"GROUP FORWARD FAILED (group_id={group_id}): {e}")

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
            # Ürün satışları (ödeme anında girildi) → bonus'a çevir, product_sales'e KAYDET
            # (calc_summary'den ÖNCE → net bu bonusu içerir ve toplam ödenir).
            _psales = data.get("product_sales")
            _pbon = 0
            if isinstance(_psales, dict) and _psales:
                _elig = bool(barista_pay_info(db, target_id)["product_ok"])
                _pc = calc_product_bonus(db, _psales, eligible=_elig)
                if _pc["revenue"] > 0:
                    db.execute(
                        "INSERT INTO product_sales (user_id, period, sales, revenue, bonus, created_at, paid_by) "
                        "VALUES (?,?,?,?,?,?,?)",
                        (target_id, period, json.dumps(_psales, ensure_ascii=False),
                         _pc["revenue"], _pc["bonus"], now.isoformat(), user.id))
                    db.commit()
                    _pbon = _pc["bonus"]
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
            _pbline = f"🛍 Бонус за товары: +{fmt_sum(_pbon)} сум\n" if _pbon > 0 else ""
            await update.message.reply_text(
                f"✅ Выплата записана\n\n"
                f"Кому: {display_name_for(db, target_id)}\n"
                f"Период: {period}\n"
                f"{_pbline}"
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
            # ── GRUP BİLDİRİMİ (чаевые) — eskiden YOKTU: sadece alan kişiye DM gidiyordu ──
            if group_id and recipients:
                try:
                    from html import escape as esc_html
                    gtext = (f"💝 <b>ЧАЕВЫЕ РАСПРЕДЕЛЕНЫ</b>\n"
                             f"💰 Всего: <b>{fmt_sum(total_dist)}</b> сум · {len(recipients)} чел.\n")
                    for _tid, _amt in recipients:
                        gtext += f"  • {esc_html(display_name_for(db, _tid, fallback='?'))}: +{fmt_sum(_amt)}\n"
                    if note:
                        gtext += f"📝 {esc_html(note)}\n"
                    gtext += f"👤 {esc_html(shown)}"
                    await context.bot.send_message(chat_id=int(group_id), text=gtext, parse_mode="HTML")
                except Exception as e:
                    logger.exception(f"GROUP FORWARD FAILED (group_id={group_id}): {e}")

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
                # Açık vardiyanın şubesi değişti → o vardiya artık başka şubenin
                # raporlarına ve «Было» zincirine sayılır. İz bırakmalı.
                if act is not None:
                    log_action(db, "move_shift_branch", user.id, user.first_name, user.id, shown,
                               {"shift_id": act["id"],
                                "branch": (get_branch(db, bid) or {})["name"] if get_branch(db, bid) else str(bid)})

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
                    log_action(db, "cancel_scheduled", user.id, user.first_name,
                               row["user_id"], display_name_for(db, row["user_id"], fallback="?"),
                               {"order_id": sid})

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
            rate = _clampi(data.get("rate"), 0, 5000000, HOURLY_RATE)
            for k, v in (("pay_open", op), ("pay_close", cl), ("pay_max", mx), ("pay_unpaid", un), ("pay_rate", rate)):
                db.execute("INSERT OR REPLACE INTO meta (k,val) VALUES (?,?)", (k, str(v)))
            db.commit()
            # Bu ayar ÖDENEN SAATİ etkiler → izi kalmalı.
            log_action(db, "pay_settings", user.id, user.first_name, None, None,
                       {"rate": rate, "open": op, "close": cl, "max_shift": mx})
            await update.message.reply_text(
                f"✅ *Часы работы обновлены*\n"
                f"Ставка: {fmt_sum(rate)} сум/час\n"
                f"Открытие: {op:02d}:00 · Закрытие: {cl:02d}:00\n"
                f"Макс. смена: {mx} ч\n"
                f"Закрытое окно ({cl:02d}:00–{op:02d}:00) не оплачивается: {'да' if un else 'нет'}",
                parse_mode="Markdown")

        elif action == "salcat_save":
            # Owner: maaş kategorisi oluştur/güncelle (id verilirse update)
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец.")
                return
            nm = (data.get("name") or "").strip()[:60]
            if not nm:
                await update.message.reply_text("❌ Укажите название категории.")
                return
            def _ci(v, lo, hi, dflt=0):
                try:
                    return max(lo, min(hi, int(v)))
                except Exception:
                    return dflt
            def _cf(v, dflt=0.0):
                try:
                    return max(0.0, float(v))
                except Exception:
                    return dflt
            rate = _ci(data.get("rate"), 0, 5000000, 0)
            mn = _cf(data.get("min_months"), 0.0)
            nxt = data.get("next_cat_id")
            nxt = int(nxt) if (nxt not in (None, "", 0, "0")) else None
            desc = (data.get("description") or "").strip()[:300]
            kpi = 1 if int(data.get("use_kpi", 0) or 0) else 0
            act = 0 if int(data.get("active", 1) or 0) == 0 else 1
            bsys = "caffelito" if (data.get("bonus_system") == "caffelito") else "own"
            pb = 1 if int(data.get("product_bonus", 0) or 0) else 0
            dk = 0 if int(data.get("does_kasa", 1) or 0) == 0 else 1  # varsayılan 1 (kasa sayar)
            sr = "assistant" if (data.get("slot_role") == "assistant") else "barista"
            cid = data.get("id")
            if cid:
                db.execute(
                    "UPDATE salary_categories SET name=?, hourly_rate=?, min_months=?, "
                    "next_cat_id=?, description=?, use_kpi=?, active=?, bonus_system=?, product_bonus=?, does_kasa=?, slot_role=? WHERE id=?",
                    (nm, rate, mn, nxt, desc, kpi, act, bsys, pb, dk, sr, int(cid)))
                msg = f"✅ Категория «{nm}» обновлена."
            else:
                _mo = db.execute("SELECT COALESCE(MAX(sort_order),-1)+1 AS s FROM salary_categories").fetchone()
                db.execute(
                    "INSERT INTO salary_categories (name, hourly_rate, min_months, next_cat_id, "
                    "description, use_kpi, active, bonus_system, product_bonus, does_kasa, slot_role, sort_order, created_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (nm, rate, mn, nxt, desc, kpi, act, bsys, pb, dk, sr, (_mo["s"] if _mo else 0),
                     datetime.now(TZ).isoformat()))
                msg = f"✅ Категория «{nm}» создана."
            db.commit()
            # Saat ücreti burada belirlenir → günlükte tutar da görünsün.
            log_action(db, "salcat_save", user.id, user.first_name, None, nm,
                       {"name": nm, "rate": rate, "amount": rate, "bonus_system": bsys,
                        "does_kasa": dk, "active": act})
            await update.message.reply_text(msg)

        elif action == "product_save":
            # Owner: ürün kataloğu — oluştur/güncelle (id verilirse update)
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец.")
                return
            nm = (data.get("name") or "").strip()[:60]
            if not nm:
                await update.message.reply_text("❌ Укажите название товара.")
                return
            def _pi2(v, lo, hi, d=0):
                try:
                    return max(lo, min(hi, int(v)))
                except Exception:
                    return d
            price = _pi2(data.get("price"), 0, 100000000, 0)
            btype = "fixed" if (data.get("bonus_type") == "fixed") else "percent"
            bval = _pi2(data.get("bonus_value"), 0, (100000000 if btype == "fixed" else 100), 0)
            pcat = (data.get("category") or "food").strip()[:20]
            pact = 0 if int(data.get("active", 1) or 0) == 0 else 1
            pid = data.get("id")
            if pid:
                db.execute("UPDATE product_catalog SET name=?, price=?, bonus_type=?, bonus_value=?, "
                           "category=?, active=? WHERE id=?",
                           (nm, price, btype, bval, pcat, pact, int(pid)))
                _pmsg = f"✅ Товар «{nm}» обновлён."
            else:
                _pmo = db.execute("SELECT COALESCE(MAX(sort_order),-1)+1 AS s FROM product_catalog").fetchone()
                db.execute("INSERT INTO product_catalog (name, price, bonus_type, bonus_value, category, "
                           "active, sort_order, created_at) VALUES (?,?,?,?,?,?,?,?)",
                           (nm, price, btype, bval, pcat, pact, (_pmo["s"] if _pmo else 0),
                            datetime.now(TZ).isoformat()))
                _pmsg = f"✅ Товар «{nm}» добавлен."
            db.commit()
            # Ürün bonusu maaşa girer → fiyat ve bonus değişikliğinin izi kalmalı.
            log_action(db, "product_save", user.id, user.first_name, None, nm,
                       {"name": nm, "price": price, "bonus_type": btype, "bonus": bval, "active": pact})
            await update.message.reply_text(_pmsg)

        elif action == "product_delete":
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец.")
                return
            _pd = data.get("id")
            if _pd:
                _pnm_del = db.execute("SELECT name FROM product_catalog WHERE id=?", (int(_pd),)).fetchone()
                db.execute("DELETE FROM product_catalog WHERE id=?", (int(_pd),))
                db.commit()
                log_action(db, "product_delete", user.id, user.first_name, None, None,
                           {"id": int(_pd), "name": (_pnm_del["name"] if _pnm_del else "")})
            await update.message.reply_text("🗑 Товар удалён.")

        elif action == "overtime_settings":
            # Owner: fazla mesai (сверхурочные) — aylık norm + ek ödeme
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец.")
                return
            try:
                oh = max(0, int(data.get("hours") or 0))
            except Exception:
                oh = 0
            ot = "percent" if (data.get("type") == "percent") else "fixed"
            try:
                ov = max(0, int(data.get("value") or 0))
            except Exception:
                ov = 0
            # `enabled` ARTIK SAKLANIYOR. Gönderilmezse mevcut değer korunur
            # (yalnızca saat/tutar düzenleyen ekranlar anahtarı sıfırlamasın).
            _oon = data.get("enabled")
            _oon = get_overtime_cfg(db).get("on", 1) if _oon is None else (1 if str(_oon) not in ("0", "", "False", "false") else 0)
            for k, v in (("ot_hours", oh), ("ot_type", ot), ("ot_value", ov), ("ot_on", _oon)):
                db.execute("INSERT OR REPLACE INTO meta (k,val) VALUES (?,?)", (k, str(v)))
            db.commit()
            log_action(db, "overtime_settings", user.id, user.first_name, None, None,
                       {"hours": oh, "type": ot, "value": ov, "on": _oon})
            if not _oon:
                await update.message.reply_text("⏸ Переработка выключена — не начисляется.")
            elif oh > 0:
                _d = f"+{fmt_sum(ov)}%/ч" if ot == "percent" else f"+{fmt_sum(ov)} сум/ч"
                await update.message.reply_text(f"✅ Переработка: свыше {oh} ч за смену → {_d}")
            else:
                await update.message.reply_text("✅ Переработка отключена.")

        elif action == "caffelito_bonus_save":
            # Owner: Caffelito bardak-bonus preset değerlerini güncelle {drink_id: amount}
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец.")
                return
            vals = data.get("values") or {}
            if isinstance(vals, dict):
                for _did, _amt in vals.items():
                    try:
                        db.execute("INSERT OR REPLACE INTO caffelito_bonus (drink_id, amount) VALUES (?,?)",
                                   (str(_did)[:20], max(0, int(_amt))))
                    except Exception:
                        pass
                db.commit()
                # Caffelito tarifesi = bardak bonusunun para karşılığı.
                log_action(db, "caffelito_bonus_save", user.id, user.first_name, None, None,
                           {"count": len(vals)})

        elif action == "salcat_delete":
            # Owner: kategori sil (bağlı baristalar → NULL = global ставка)
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец.")
                return
            cid = data.get("id")
            if cid:
                # Ad, SİLMEDEN ÖNCE alınmalı — sonra okunamaz.
                _scr = db.execute("SELECT name FROM salary_categories WHERE id=?", (int(cid),)).fetchone()
                _scnm_del = (_scr["name"] if _scr else "") or ""
                db.execute("UPDATE users SET salary_cat_id=NULL WHERE salary_cat_id=?", (int(cid),))
                db.execute("UPDATE salary_categories SET next_cat_id=NULL WHERE next_cat_id=?", (int(cid),))
                db.execute("DELETE FROM salary_categories WHERE id=?", (int(cid),))
                db.commit()
                log_action(db, "salcat_delete", user.id, user.first_name, None, _scnm_del,
                           {"id": int(cid), "name": _scnm_del})
            await update.message.reply_text("🗑 Категория удалена.")

        elif action == "salcat_assign":
            # Owner: baristayı bir maaş kategorisine bağla (0/None = kaldır → global)
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец.")
                return
            tgt = data.get("target")
            cid = data.get("cat_id")
            if tgt:
                cid_val = int(cid) if (cid not in (None, "", 0, "0")) else None
                if cid_val is not None and not db.execute(
                        "SELECT 1 FROM salary_categories WHERE id=?", (cid_val,)).fetchone():
                    cid_val = None
                db.execute("UPDATE users SET salary_cat_id=? WHERE user_id=?", (cid_val, int(tgt)))
                db.commit()
                # Kategori = saat ücreti + bonus sistemi + kasa yetkisi. Kimin
                # hangi kategoriye alındığı para etkisi olan bir karardır.
                _sca = db.execute("SELECT name FROM salary_categories WHERE id=?", (cid_val,)).fetchone() if cid_val else None
                log_action(db, "salcat_assign", user.id, user.first_name, int(tgt),
                           display_name_for(db, int(tgt), fallback="?"),
                           {"cat_id": cid_val, "cat": (_sca["name"] if _sca else "без категории")})
                # Kişinin AÇIK vardiyası varsa rol/ставка snapshot'ı anında güncel
                # kategoriye göre tazelenir (tarihsel vardiyalara dokunulmaz).
                refresh_open_shift_snapshot(db, int(tgt))

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
            # Yeni şube varsayılanları: работа 07:00–03:00, kapalı pencere (03:00–07:00)
            # ÖDENMEZ (unpaid_win=1 → toggle açık). Owner istediğinde Филиалы'de değiştirir.
            cur = db.execute(
                "INSERT INTO branches (name, group_chat_id, sort_order, active, created_at, "
                "open_hour, close_hour, unpaid_win) VALUES (?,?,?,1,?,7,3,1)",
                (bname, None, (mx or 0) + 1, now.isoformat()))
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

        elif action == "branch_hours":
            # Şube-bazlı çalışma saatleri (kapalı pencere) — owner
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец.")
                return
            try:
                bid = int(data.get("branch_id") or 0)
            except Exception:
                bid = 0
            if not get_branch(db, bid):
                await update.message.reply_text("❌ Филиал не найден.")
                return
            def _ci(v, lo, hi, d):
                """Saat değeri. Nero «ЧЧ:ММ» (time input) gönderir, eski istemci
                düz sayı. Eskiden yalnızca int() deneniyordu: «02:00» patlıyor,
                sessizce VARSAYILANA (07/03) düşülüyordu → owner kapanışı 03:00'ten
                02:00'a çekse de kaydolmuyor, ekranda hep 03:00 kalıyordu ve
                ödenmeyen pencere 4 saat olarak duruyordu."""
                try:
                    s = str(v).strip()
                    if ":" in s:
                        s = s.split(":")[0]
                    return max(lo, min(hi, int(float(s))))
                except Exception:
                    return d
            oh = _ci(data.get("open"), 0, 23, 7)
            ch = _ci(data.get("close"), 0, 23, 3)
            uw = 1 if int(data.get("unpaid", 1) or 0) else 0
            db.execute("UPDATE branches SET open_hour=?, close_hour=?, unpaid_win=? WHERE id=?",
                       (oh, ch, uw, bid))
            db.commit()
            # «Saatlerim eksik» şikâyetlerinin kaynağı bu ayardır — kim ne zaman
            # değiştirdi görünmeli.
            log_action(db, "branch_hours", user.id, user.first_name, None,
                       (get_branch(db, bid) or {})["name"] if get_branch(db, bid) else "",
                       {"branch_id": bid, "open": oh, "close": ch, "unpaid": uw})

        elif action == "branch_trainee":
            # Owner: bu şubede «Ассистент/стажёр» pozisyonunu aç/kapat (şube-bazlı işgücü ayarı)
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец.")
                return
            try:
                bid = int(data.get("branch_id") or 0)
            except Exception:
                bid = 0
            if not get_branch(db, bid):
                await update.message.reply_text("❌ Филиал не найден.")
                return
            en = 1 if int(data.get("enabled", 0) or 0) else 0
            db.execute("UPDATE branches SET trainee_enabled=? WHERE id=?", (en, bid))
            db.commit()
            log_action(db, "branch_trainee", user.id, user.first_name, None, None,
                       {"branch_id": bid, "enabled": en})
            # Bu şubedeki AÇIK vardiyaların rol snapshot'ları güncel kategoriye göre
            # tazelenir (ör. toggle açılınca stajyer kategorili kişinin açık vardiyası
            # yanlışlıkla barista rolündeyse → anında stajyere döner, barista slotu boşalır).
            try:
                _open_rows = db.execute(
                    "SELECT DISTINCT user_id FROM shifts WHERE end_time IS NULL "
                    "AND start_time IS NOT NULL AND COALESCE(branch_id,1)=?", (bid,)).fetchall()
                for _orr in _open_rows:
                    refresh_open_shift_snapshot(db, _orr["user_id"])
            except Exception:
                pass
            await update.message.reply_text(
                ("🎓 Позиция ассистента/стажёра включена." if en else "Позиция ассистента/стажёра выключена.")
                + " Настройка сохранена.")

        elif action == "branch_role_save":
            # Owner: kişi × şube kategori ataması. cat_id boş → override silinir (global geçerli).
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец.")
                return
            try:
                target_id = int(data.get("target") or 0)
                bid = int(data.get("branch_id") or 0)
            except Exception:
                target_id, bid = 0, 0
            if not target_id or not get_branch(db, bid):
                await update.message.reply_text("❌ Сотрудник/филиал не найден.")
                return
            cid = data.get("cat_id")
            if cid in (None, "", 0, "0"):
                db.execute("DELETE FROM branch_staff WHERE user_id=? AND branch_id=?", (target_id, bid))
            else:
                if not db.execute("SELECT 1 FROM salary_categories WHERE id=?", (int(cid),)).fetchone():
                    await update.message.reply_text("❌ Категория не найдена.")
                    return
                db.execute("INSERT OR REPLACE INTO branch_staff (user_id, branch_id, salary_cat_id) VALUES (?,?,?)",
                           (target_id, bid, int(cid)))
            db.commit()
            log_action(db, "branch_role_save", user.id, user.first_name, target_id,
                       display_name_for(db, target_id), {"branch_id": bid, "cat_id": cid})
            # Kişinin AÇIK vardiyası bu şubedeyse rol/ставка snapshot'ı anında tazelenir
            refresh_open_shift_snapshot(db, target_id)

        elif action == "force_end_shift":
            # Owner: çalışanın vardiyasını elle kapat (kapatmayı unuttuysa).
            # Snapshot kurallarıyla hesaplanır (end_shift); bardak/kasa girilmez.
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец.")
                return
            try:
                target_id = int(data.get("target") or 0)
            except Exception:
                target_id = 0
            if not target_id or not get_active_shift(db, target_id):
                await update.message.reply_text("ℹ️ У сотрудника нет открытой смены.")
                return
            # Opsiyonel kapanış saati (owner düzeltmesi): 'HH:MM' veya ISO. end_shift
            # doğrular: başlangıçtan önce olamaz + SONRAKİ aynı-rol vardiyasına taşamaz
            # (devir bütünlüğü) + ödeme şube kapanış penceresine göre.
            _req_end = data.get("end_time") or None
            sh = end_shift(db, target_id, {}, note="закрыто владельцем", custom_end=_req_end)
            if not sh:
                await update.message.reply_text("❌ Не удалось закрыть смену.")
                return
            _nm = display_name_for(db, target_id, fallback="?")
            log_action(db, "force_end_shift", user.id, user.first_name, target_id, _nm,
                       {"shift_id": sh["id"], "branch_id": sh["branch_id"],
                        "requested_end": _req_end, "actual_end": sh["end_time"],
                        "hours": sh["hours"], "total": sh["total"]})
            _adj = ""
            try:
                _rq = _parse_user_time(_req_end)
                _ae = datetime.fromisoformat(sh["end_time"])
                if _rq and abs((_rq - _ae).total_seconds()) > 60:
                    _adj = (f"\n⚠️ Время скорректировано до {_ae.strftime('%H:%M')} — "
                            "позиция передана следующей смене (пересечение недопустимо).")
            except Exception:
                pass
            await update.message.reply_text(
                f"🔴 Смена *{_nm}* закрыта владельцем.\n"
                f"⏱ {fmt_hm(sh['hours'] or 0)} · 💰 {fmt_sum(sh['total'] or 0)} сум{_adj}",
                parse_mode="Markdown")
            try:
                await context.bot.send_message(
                    chat_id=target_id,
                    text=f"ℹ️ Ваша смена была закрыта владельцем.\n⏱ Часы: {fmt_hm(sh['hours'] or 0)} · 💰 {fmt_sum(sh['total'] or 0)} сум")
            except Exception:
                pass

        elif action == "force_start_shift":
            # Owner: çalışan adına vardiya başlat (slot kuralları yine geçerli).
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец.")
                return
            try:
                target_id = int(data.get("target") or 0)
            except Exception:
                target_id = 0
            if not target_id:
                await update.message.reply_text("❌ Выберите сотрудника.")
                return
            if get_active_shift(db, target_id):
                await update.message.reply_text("ℹ️ У сотрудника уже есть открытая смена.")
                return
            _fbid = None
            try:
                if data.get("branch_id") and get_branch(db, data.get("branch_id")):
                    _fbid = int(data.get("branch_id"))
            except Exception:
                _fbid = None
            if not _fbid:
                _fbid = user_branch_id(db, target_id)
            _blk = slot_block_reason(db, target_id, _fbid)
            if _blk:
                await update.message.reply_text(_blk)
                return
            # Opsiyonel başlangıç saati (çalışan Nero'yu açmayı unuttuysa gerçek geliş saati).
            # start_shift doğrular: gelecek clamp + AYNI ROL devir klampesi (önceki bitişten
            # önceye yazılamaz) + rol/kategori/ставка şube-etkin konfigürasyondan SNAPSHOT.
            _req_st = data.get("start_time") or None
            sh = start_shift(db, target_id, custom_start=_req_st, branch_id=_fbid)
            # Audit: owner-elle-başlatma vardiya notuna da işlensin (log_action'a ek)
            try:
                db.execute("UPDATE shifts SET note=? WHERE id=?", ("начато владельцем", sh["id"]))
                db.commit()
            except Exception:
                pass
            _nm = display_name_for(db, target_id, fallback="?")
            _stdt = datetime.fromisoformat(sh["start_time"])
            _st = _stdt.strftime("%H:%M")
            log_action(db, "force_start_shift", user.id, user.first_name, target_id, _nm,
                       {"shift_id": sh["id"], "branch_id": _fbid,
                        "requested_start": _req_st, "actual_start": sh["start_time"],
                        "role": sh["shift_role"], "cat": sh["cat_name"], "rate": sh["rate"]})
            _adj = ""
            try:
                _rq = _parse_user_time(_req_st)
                if _rq and (_stdt - _rq).total_seconds() > 60:
                    _adj = (f"\n⚠️ Позиция была занята — время скорректировано до {_st} "
                            "(передача смены, пересечение недопустимо).")
            except Exception:
                pass
            await update.message.reply_text(
                f"🟢 Смена *{_nm}* начата владельцем в {_st}.\n"
                f"🏷️ {sh['cat_name'] or '—'} · {fmt_sum(sh['rate'] or 0)}/ч"
                f"{_adj}", parse_mode="Markdown")
            try:
                await context.bot.send_message(
                    chat_id=target_id,
                    text=f"ℹ️ Владелец начал вашу смену в {_st}. Не забудьте закрыть её в конце дня.")
            except Exception:
                pass

        elif action == "delete_shift":
            # Owner: yanlış/kazara vardiyayı sil (barista hatalarını düzeltme)
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец.")
                return
            try:
                sid = int(data.get("id") or 0)
            except Exception:
                sid = 0
            if sid:
                row = db.execute("SELECT user_id FROM shifts WHERE id=?", (sid,)).fetchone()
                # Vardiya ile birlikte, o kapanışta kasadan ödenen günlük bardak
                # bonusunu da geri al (yoksa bakiye bonus kadar eksiye düşüyordu).
                _rm = _drop_shift_daily_pay(db, sid)
                if _rm:
                    logger.info(f"vardiya {sid} silindi → gunluk bonus odemesi {_rm} de silindi")
                db.execute("DELETE FROM shifts WHERE id=?", (sid,))
                db.commit()
                if row:
                    log_action(db, "delete_shift", user.id, user.first_name, row["user_id"],
                               display_name_for(db, row["user_id"]), {"shift_id": sid})

        elif action == "edit_shift":
            # Owner: bir vardiyanın giriş/çıkış saatini ELLE düzelt (yanlış kayıtları
            # onarmak için). SNAPSHOT ставка korunur; saat + saatlik ücret + fazla mesai
            # YENİDEN hesaplanır; bardak/tatlı bonusu (satış) DEĞİŞMEZ. Başka vardiya
            # etkilenmez — klampe yok, sadece bu kaydı günceller.
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец.")
                return
            try:
                sid = int(data.get("id") or 0)
            except Exception:
                sid = 0
            sh = db.execute("SELECT * FROM shifts WHERE id=?", (sid,)).fetchone() if sid else None
            if not sh:
                await update.message.reply_text("❌ Смена не найдена.")
                return
            _ns = _parse_user_time(data.get("start_time")) if data.get("start_time") else None
            _ne = _parse_user_time(data.get("end_time")) if data.get("end_time") else None
            start_dt = _ns or (datetime.fromisoformat(sh["start_time"]) if sh["start_time"] else None)
            if not start_dt:
                await update.message.reply_text("❌ Неверное время начала.")
                return
            _existing_end = datetime.fromisoformat(sh["end_time"]) if sh["end_time"] else None
            end_dt = _ne or _existing_end  # yeni çıkış verilmezse mevcut çıkışı koru
            if end_dt and end_dt < start_dt:
                await update.message.reply_text("❌ Уход не может быть раньше прихода.")
                return
            _bid = sh["branch_id"] if sh["branch_id"] else user_branch_id(db, sh["user_id"])
            try:
                _rate = int(sh["rate"]) if sh["rate"] else int(barista_pay_info(db, sh["user_id"], branch_id=_bid)["rate"])
            except Exception:
                _rate = int(sh["rate"] or 0)
            _bonus = int(sh["bonus"] or 0)  # bardak/tatlı bonusu KORUNUR (saat düzenlemesi satışı değiştirmez)
            if end_dt:
                _pc = branch_pay_window(db, _bid)
                hours = paid_hours(start_dt, end_dt, _pc)
                hourly_pay = int(hours * _rate)
                ot_shift, ot_h = 0, 0.0
                try:
                    _otc = get_overtime_cfg(db)
                    _thr = float(_otc.get("hours") or 0)
                    if _otc.get("on", 1) and _thr > 0 and hours > _thr:
                        ot_h = round(hours - _thr, 2)
                        _val = int(_otc.get("value") or 0)
                        ot_shift = int(ot_h * _rate * (_val / 100.0)) if _otc.get("type") == "percent" else int(ot_h * _val)
                except Exception:
                    ot_shift, ot_h = 0, 0.0
                total = hourly_pay + _bonus
                db.execute(
                    "UPDATE shifts SET start_time=?, end_time=?, hours=?, hourly_pay=?, total=?, "
                    "overtime=?, overtime_h=?, period=? WHERE id=?",
                    (start_dt.isoformat(), end_dt.isoformat(), hours, hourly_pay, total,
                     ot_shift, ot_h, start_dt.strftime("%Y-%m"), sid))
            else:
                # Açık vardiya → sadece giriş saatini güncelle (hâlâ açık kalır).
                db.execute("UPDATE shifts SET start_time=?, period=? WHERE id=?",
                           (start_dt.isoformat(), start_dt.strftime("%Y-%m"), sid))
            db.commit()
            _sh2 = db.execute("SELECT * FROM shifts WHERE id=?", (sid,)).fetchone()
            _nm = display_name_for(db, sh["user_id"], fallback="?")
            log_action(db, "edit_shift", user.id, user.first_name, sh["user_id"], _nm,
                       {"shift_id": sid, "start": _sh2["start_time"], "end": _sh2["end_time"],
                        "hours": _sh2["hours"], "total": _sh2["total"]})
            if _sh2["end_time"]:
                await update.message.reply_text(
                    f"✏️ Смена *{_nm}* обновлена.\n"
                    f"⏰ {start_dt.strftime('%d.%m %H:%M')} → {end_dt.strftime('%H:%M')} · "
                    f"{fmt_hm(_sh2['hours'] or 0)} · {fmt_sum(_sh2['total'] or 0)} сум",
                    parse_mode="Markdown")
            else:
                await update.message.reply_text(
                    f"✏️ Время начала смены *{_nm}* изменено на {start_dt.strftime('%d.%m %H:%M')}.",
                    parse_mode="Markdown")

        elif action == "adjust_balance":
            # Owner: kişinin bakiyesine MANUEL düzeltme (Корректировка, +/-) ekle.
            # Mutabakat için: «−» borcu azaltır (zaten ödendi/telafi düzeltmesi), «+» artırır.
            # net'e doğrudan girer (calc_summary'de toplanır); başka kalem etkilenmez.
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец.")
                return
            try:
                target_id = int(data.get("target") or 0)
            except Exception:
                target_id = 0
            try:
                amount = int(round(float(data.get("amount") or 0)))
            except Exception:
                amount = 0
            note = (data.get("note") or "").strip()
            if not target_id or amount == 0:
                await update.message.reply_text("❌ Укажите сотрудника и сумму (не 0).")
                return
            # Düzeltme SEÇİLİ AYa yazılır (owner geçmiş ayı denkleştirebilsin); geçersizse güncel ay.
            _rp = data.get("period")
            _per = _rp if (isinstance(_rp, str) and re.fullmatch(r"\d{4}-\d{2}", _rp or "")) else current_period()
            _now = datetime.now(TZ).replace(tzinfo=None)
            try:
                _abid = user_branch_id(db, target_id)
            except Exception:
                _abid = 1
            db.execute(
                "INSERT INTO adjustments (user_id, amount, note, period, branch_id, added_by, added_by_name, created_at) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (target_id, amount, note, _per, _abid, user.id, user.first_name, _now.isoformat()))
            db.commit()
            _anm = display_name_for(db, target_id, fallback="?")
            log_action(db, "adjust_balance", user.id, user.first_name, target_id, _anm,
                       {"amount": amount, "note": note, "period": _per})
            _sign = "+" if amount > 0 else ""
            await update.message.reply_text(
                f"⚖️ Корректировка для *{_anm}*: {_sign}{fmt_sum(amount)} сум"
                + (f"\n📝 {note}" if note else ""), parse_mode="Markdown")

        elif action == "transfer_closing":
            # Kapatma sorumluluğunu (closing owner) başka bir AKTİF çalışana devret.
            # Sadece PERMISSION değişir — saat/maaş/bonus DEĞİŞMEZ. Yetki: mevcut
            # sorumlu VEYA владелец. Hedef aynı şubede açık vardiyada olmalı.
            db = get_db()
            try:
                target_id = int(data.get("target") or 0)
            except Exception:
                target_id = 0
            _me_act = get_active_shift(db, user.id)
            _bid = (_me_act["branch_id"] if _me_act else None) or acting_branch_id(db, user.id)
            _cur_owner = closing_owner_uid(db, _bid)
            if get_role(db, user.id) != "owner" and _cur_owner != user.id:
                await update.message.reply_text("❌ Передать закрытие может только текущий ответственный или владелец.")
                return
            _t_act = get_active_shift(db, target_id) if target_id else None
            if not _t_act:
                await update.message.reply_text("❌ Сотрудник не на смене.")
                return
            _tbid = _t_act["branch_id"] or _bid
            if int(_tbid or 1) != int(_bid or 1):
                await update.message.reply_text("❌ Сотрудник работает в другом филиале.")
                return
            _tnm = display_name_for(db, target_id, fallback="?")
            db.execute("INSERT OR REPLACE INTO meta(k,val) VALUES(?,?)",
                       (f"closing_owner_{int(_bid or 1)}",
                        json.dumps({"uid": target_id, "nm": _tnm, "by": user.id, "at": now.isoformat()}, ensure_ascii=False)))
            db.commit()
            log_action(db, "transfer_closing", user.id, user.first_name, target_id, _tnm,
                       {"branch_id": int(_bid or 1)})
            await update.message.reply_text(f"🔄 Закрытие передано: *{_tnm}*", parse_mode="Markdown")
            try:
                await context.bot.send_message(
                    chat_id=target_id,
                    text="🔄 Вам передали закрытие смены. Теперь стаканы/кассу/отчёт закрываете вы.")
            except Exception:
                pass

        elif action == "reopen_closing":
            # FAZ C: kapatmayı 10 DK içinde yeniden aç (düzeltme için). Sorumlu kendi
            # kapatmasını, владелец herhangi birininkini açabilir. Vardiya yeniden aktive
            # edilir (hesap sıfırlanır), kasa raporu + otomatik günlük-bonus ödemesi geri
            # alınır → kişi düzeltip TEKRAR kapatır. 10 dk sonra KİLİT.
            db = get_db()
            try:
                target_id = int(data.get("target") or user.id)
            except Exception:
                target_id = user.id
            if target_id != user.id and get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Открыть чужое закрытие может только владелец.")
                return
            cr = db.execute("SELECT * FROM cashreports WHERE user_id=? ORDER BY id DESC LIMIT 1",
                            (target_id,)).fetchone()
            if not cr:
                await update.message.reply_text("ℹ️ Нет закрытия для повторного открытия.")
                return
            try:
                _cat = datetime.fromisoformat(cr["created_at"])
            except Exception:
                _cat = None
            if not _cat or (now - _cat).total_seconds() > 10 * 60:
                await update.message.reply_text("🔒 Закрытие заблокировано — прошло больше 10 минут.")
                return
            # İlgili vardiyayı yeniden aktive et (aynı start_time'lı kapanmış vardiya)
            sh = db.execute(
                "SELECT * FROM shifts WHERE user_id=? AND start_time=? AND end_time IS NOT NULL ORDER BY id DESC LIMIT 1",
                (target_id, cr["start_time"])).fetchone()
            if sh:
                db.execute("UPDATE shifts SET end_time=NULL, hours=0, bonus=0, hourly_pay=0, total=0, "
                           "overtime=0, overtime_h=0 WHERE id=?", (sh["id"],))
            # Otomatik günlük-bonus ödemesini geri al (kasa raporuyla AYNI paid_at ile eşleşir)
            if (cr["daily_pay"] or 0) > 0:
                db.execute("DELETE FROM payments WHERE user_id=? AND paid_at=? AND amount=?",
                           (target_id, cr["created_at"], cr["daily_pay"]))
            # Kasa raporunu sil (yeniden kapatınca taze oluşur; duplicate/guard temizlenir)
            db.execute("DELETE FROM cashreports WHERE id=?", (cr["id"],))
            db.commit()
            _rnm = display_name_for(db, target_id, fallback="?")
            log_action(db, "reopen_closing", user.id, user.first_name, target_id, _rnm,
                       {"cashreport_id": cr["id"], "start_time": cr["start_time"]})
            await update.message.reply_text(
                "🔄 Закрытие открыто заново. Внесите правки и закройте смену ещё раз.")
            if target_id != user.id:
                try:
                    await context.bot.send_message(
                        chat_id=target_id,
                        text="🔄 Владелец открыл ваше закрытие для правок. Закройте смену ещё раз.")
                except Exception:
                    pass

        elif action == "delete_record":
            # Owner: Отчёт odalarından tek kayıt sil (maaşa/kasaya yansır)
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец.")
                return
            kind = (data.get("kind") or "").strip()
            try:
                rid = int(data.get("id") or 0)
            except Exception:
                rid = 0
            _TBL = {"tip": "tips", "pay": "payments", "fine": "fines", "loan": "loans",
                    "order": "orders", "cash": "cashreports", "shift": "shifts",
                    "adjustment": "adjustments"}
            tbl = _TBL.get(kind)
            if tbl and rid:
                try:
                    # VARDİYA siliniyorsa: o kapanışta KASADAN ödenen günlük bardak
                    # bonusunu da geri al. Yoksa kazanç kayboluyor ama «ödendi»
                    # kaydı kalıyordu → bakiye bonus kadar eksiye düşüyordu.
                    # Sadece KENDİ KENDİNE ödeme (paid_by=user_id → günlük bonus),
                    # tutarı vardiyanın bonusuyla aynı ve kapanış saatine yakın olan
                    # TEK kayıt silinir; owner'ın yaptığı gerçek ödemelere dokunulmaz.
                    if kind == "shift":
                        _rm = _drop_shift_daily_pay(db, rid)
                        if _rm:
                            logger.info(f"vardiya {rid} silindi → gunluk bonus odemesi {_rm} de silindi")
                    db.execute(f"DELETE FROM {tbl} WHERE id=?", (rid,))
                    db.commit()
                    log_action(db, "delete_record", user.id, user.first_name, None, None, {"kind": kind, "id": rid})
                except Exception as ex:
                    logger.warning(f"delete_record {kind}/{rid}: {ex}")

        # ─── Şifre yönetimi (owner-only) ───
        elif action == "change_my_pin":
            # Çalışan KENDİ PIN'ini değiştirir (Профиль → Безопасность). Erişim yetkisi
            # (owner'ın PIN vermesi) ile kimlik doğrulama AYRI: burada sadece kimlik —
            # mevcut PIN doğrulanır. PIN değerleri LOGLANMAZ (düz metin sızmaz).
            db = get_db()
            _row = db.execute("SELECT password FROM users WHERE user_id=?", (user.id,)).fetchone()
            _cur_pw = ((_row["password"] or "").strip() if _row else "")
            if not _cur_pw:
                await update.message.reply_text("❌ Доступ ещё не выдан владельцем.")
                return
            _old = str(data.get("old") or "").strip()
            _new = str(data.get("pin") or "").strip()
            if _old != _cur_pw:
                await update.message.reply_text("❌ Текущий PIN-код неверен.")
                return
            if not (_new.isdigit() and len(_new) == 4):
                await update.message.reply_text("❌ Новый PIN-код должен состоять из 4 цифр.")
                return
            db.execute("UPDATE users SET password=? WHERE user_id=?", (_new, user.id))
            db.commit()
            log_action(db, "change_my_pin", user.id, user.first_name, None, None, {})
            await update.message.reply_text("🔐 PIN-код обновлён.")

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

        # ─── Erişim isteği (kilitli ekrandaki kişi → owner) ───
        # Eskiden kilit ekranı yalnızca owner'ın ID'sini yazıyordu ve kişi onu
        # elle bulup yazmak zorundaydı. Artık tek dokunuşla haber gidiyor.
        # ARŞİVDEKİ KİŞİ HABER GÖNDEREMEZ: owner reddettiyse konu kapanmıştır,
        # arşivden çıkarılana kadar tekrar rahatsız edemez.
        elif action == "access_request":
            db = get_db()
            _me = db.execute("SELECT * FROM users WHERE user_id=?", (user.id,)).fetchone()
            if _me and (_me["archived"] or 0):
                await update.message.reply_text(
                    "🚫 Доступ закрыт владельцем. Обратитесь к нему лично.")
                return
            # KONTROL `approved` DEĞİL, GERÇEK ERİŞİM olmalı. `approved=1` olup da
            # giremeyen bir durum var: owner PIN'i kaldırınca (authorized=0) kişi
            # kilitleniyor ama onayı duruyor. Eski kontrol o kişiye «доступ уже
            # есть» deyip owner'a HABER GÖNDERMİYORDU — kişi kapıda kalıyordu.
            if nero_access_ok(db, user.id):
                await update.message.reply_text("✅ Доступ у вас уже есть. Откройте приложение заново.")
                return
            _nm = display_name_for(db, user.id, fallback=user.first_name or "?")
            _un = ("@" + user.username) if getattr(user, "username", None) else ""
            # Spam kalkanı: aynı kişi 10 dakikada bir kez haber gönderebilir.
            _k = f"accessreq_{user.id}"
            try:
                _last = db.execute("SELECT val FROM meta WHERE k=?", (_k,)).fetchone()
                if _last and _last["val"]:
                    _dt = (now - datetime.fromisoformat(_last["val"])).total_seconds()
                    if _dt < 600:
                        await update.message.reply_text("⏳ Запрос уже отправлен. Владелец скоро ответит.")
                        return
            except Exception:
                pass
            db.execute("INSERT OR REPLACE INTO meta (k,val) VALUES (?,?)", (_k, now.isoformat()))
            db.commit()
            log_action(db, "access_request", user.id, user.first_name, user.id, _nm, {})
            _sent = 0
            for _o in db.execute("SELECT user_id FROM users WHERE role='owner'").fetchall():
                try:
                    await context.bot.send_message(
                        _o["user_id"],
                        f"🙋 *Запрос доступа*\n{md_safe(_nm)} {md_safe(_un)}\n"
                        f"ID `{user.id}`\n\n"
                        "Управление → Заявки на доступ — подтвердить или отклонить.",
                        parse_mode="Markdown")
                    _sent += 1
                except Exception as e:
                    logger.warning(f"access_request notify: {e}")
            await update.message.reply_text(
                "📨 Запрос отправлен владельцу. Он подтвердит вас и выдаст PIN-код."
                if _sent else
                "📨 Запрос записан. Обратитесь к владельцу — уведомление не доставилось.")

        # ─── Erişim isteğini REDDET (owner) ───
        # Reddedilen kişi ARŞİVE düşer: uygulamaya giremez, «Заявки на доступ»
        # listesinden kaybolur (o sorgu arşivlileri zaten dışlıyor) ve arşivden
        # çıkarılmadıkça yeni istek gönderemez.
        elif action == "reject_user":
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
                await update.message.reply_text("❌ Нельзя отклонить владельца.")
                return
            _rnm = target_row["display_name"] or target_row["name"] or "?"
            db.execute(
                "UPDATE users SET approved=0, archived=1, archived_at=?, authorized=0 WHERE user_id=?",
                (now.isoformat(), target_id))
            db.commit()
            log_action(db, "reject_user", user.id, user.first_name, target_id, _rnm, {})
            try:
                await context.bot.send_message(
                    target_id, "🚫 Владелец отклонил запрос на доступ.")
            except Exception:
                pass
            await update.message.reply_text(
                f"🚫 *{md_safe(_rnm)}* отклонён и перенесён в архив.\n"
                "Новые запросы от него не придут, пока вы не вернёте его из архива.",
                parse_mode="Markdown")
            await refresh_webapp_keyboard(update, context, db, user, "🔄 Готово 👇")

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
            # ── ÇİFT KAPATMA ENGELİ ─────────────────────────────────────────
            # BU KORUMA UZUN SÜRE ÖLÜYDÜ. Eskiden referans olarak `get_active_shift`
            # kullanılıyordu; oysa Nero kapanışta ÖNCE `shift_end` yolluyor, yani
            # buraya gelindiğinde kişinin açık vardiyası ARTIK YOK → sorgu None
            # dönüyor ve blok komple atlanıyordu. Koruma hiç çalışmıyordu.
            #
            # Neden önemli: ikinci bir kasa raporu «Осталось» zincirine yanlış bir
            # halka ekler. Bir sonraki vardiyanın «Было»su o yanlış halkadan
            # doldurulur ve hata günler boyu sürüklenir — hem de sessizce.
            #
            # Referans artık: açık vardiya YOKSA AZ ÖNCE KAPANAN vardiya.
            _my_act_cr = get_active_shift(db, user.id)
            if not _my_act_cr:
                _my_act_cr = db.execute(
                    "SELECT * FROM shifts WHERE user_id=? AND start_time IS NOT NULL "
                    "AND end_time IS NOT NULL ORDER BY id DESC LIMIT 1", (user.id,)).fetchone()
            # Şube de o vardiyadan gelir. `acting_branch_id` kapanıştan sonra açık
            # vardiyadan çözemez, oturum/ev şubesine düşer — rapor yazımında aynı
            # hatayı daha önce düzeltmiştik, kontrol de aynı önceliği kullanmalı.
            _cr_bid_chk = None
            try:
                _pb_chk = int(data.get("branch_id") or data.get("branch") or 0)
                if _pb_chk and get_branch(db, _pb_chk):
                    _cr_bid_chk = _pb_chk
            except Exception:
                _cr_bid_chk = None
            if not _cr_bid_chk and _my_act_cr and _my_act_cr["branch_id"]:
                _cr_bid_chk = int(_my_act_cr["branch_id"])
            if not _cr_bid_chk:
                _cr_bid_chk = acting_branch_id(db, user.id)
            # PENCEREYİ SINIRLA: referans vardiya çok eskiyse (ör. kişi günlerdir
            # çalışmadı ama owner onun adına kapatıyor) «vardiya başlangıcından
            # sonra» ölçütü günleri kapsar ve alakasız bir raporla HAKSIZ engel
            # doğar. Açık vardiya ya da son 24 saatte kapanmış vardiya dışında
            # kontrol uygulanmaz — koruma dar ve kesin kalsın.
            if _my_act_cr and _my_act_cr["end_time"]:
                try:
                    if (now - datetime.fromisoformat(_my_act_cr["end_time"])) > timedelta(hours=24):
                        _my_act_cr = None
                except Exception:
                    pass
            if _my_act_cr and _my_act_cr["start_time"]:
                # (a) BAŞKASI bu şubeyi benim vardiyam başladıktan sonra kapattı mı?
                _prev_cr = db.execute(
                    "SELECT user_name, created_at FROM cashreports WHERE COALESCE(branch_id,1)=? "
                    "AND created_at > ? AND user_id != ? ORDER BY id DESC LIMIT 1",
                    (int(_cr_bid_chk or 1), _my_act_cr["start_time"], user.id)).fetchone()
                if _prev_cr:
                    try:
                        _pt = datetime.fromisoformat(_prev_cr["created_at"]).strftime("%H:%M")
                    except Exception:
                        _pt = "?"
                    logger.info(f"cift kapatma engellendi (baskasi) uid={user.id} sube={_cr_bid_chk}")
                    await update.message.reply_text(
                        f"⚠️ Смена уже закрыта: *{_prev_cr['user_name'] or '?'}* в {_pt}.\n"
                        "Повторное закрытие не требуется — ваши часы будут учтены.",
                        parse_mode="Markdown")
                    return
                # (b) BEN bu vardiya için zaten rapor gönderdim mi? (çift dokunuş,
                #     ağ tekrarı, uygulamayı kapatıp yeniden onaylama). İstemcideki
                #     kilit sayfa yenilenince kaybolur — asıl koruma burada olmalı.
                _mine_cr = db.execute(
                    "SELECT created_at FROM cashreports WHERE COALESCE(branch_id,1)=? "
                    "AND created_at > ? AND user_id = ? ORDER BY id DESC LIMIT 1",
                    (int(_cr_bid_chk or 1), _my_act_cr["start_time"], user.id)).fetchone()
                if _mine_cr:
                    try:
                        _mt = datetime.fromisoformat(_mine_cr["created_at"]).strftime("%H:%M")
                    except Exception:
                        _mt = "?"
                    logger.info(f"cift kapatma engellendi (ayni kisi) uid={user.id} sube={_cr_bid_chk}")
                    await update.message.reply_text(
                        f"✅ Касса за эту смену уже сдана в {_mt}.\n"
                        "Второй отчёт не нужен. Если в нём ошибка — попросите владельца исправить.",
                        parse_mode="Markdown")
                    return
            cups = data.get("cups", [])  # [{n,b,r,o,s}]
            # Para alanları AKILLI normalize (her kaynak için; «82→82 000», asla ×1000000).
            itg = _norm_amt(data.get("itogo", 0))
            clk = _norm_amt(data.get("click", 0))
            pay = _norm_amt(data.get("payme", 0))
            kar = _norm_amt(data.get("karta", 0))
            term = _norm_amt(data.get("terminal", 0))
            vsh = _norm_amt(data.get("vyshlo", 0))
            sdachi = _norm_amt(data.get("na_sdachi", 0))
            exps = data.get("expenses", [])  # [{n,a}]
            # Harcama tutarları da normalize (in-place) — girdi/kaynak ne olursa olsun.
            for _e in exps:
                if isinstance(_e, dict):
                    _e["a"] = _norm_amt(_e.get("a", 0))
            note = (data.get("note") or "").strip()
            daily_pay = int(data.get("daily_pay", 0) or 0)  # günlük bonus (satılan bardak) — kasadan alınır
            # PARA GÜVENLİĞİ: kasadan alınan bonus, vardiyaya GERÇEKTEN yazılan
            # bonusu ASLA aşamaz. İstemci yanlış tarife kullanırsa (ör. kategorisi
            # Caffelito olan kişi için «Наша» fiyatları) kasadan fazla para
            # alınıyor, hesabına azı yazılıyordu → kasa açık veriyordu.
            #
            # ⚠️ REFERANS VARDİYA — 2026-08-16'da bu satır PARA KAYBETTİRDİ.
            # Eskiden referans «id'ce son kapanan vardiya» idi ve yorum «shift_end
            # cash_report'tan ÖNCE gelir» diye VARSAYIYORDU. Oysa istemci ikisini
            # ayrı isteklerle, birbirini beklemeden yolluyordu: rapor önce
            # işlenince kapanmakta olan vardiya henüz açıktı ve sorgu GÜNLER
            # ÖNCEKİ vardiyayı buldu (Хусейин 16.08: kazanç 120.200, referans
            # 12.08'in 77.600'ü → kasadan 77.600 çıktı, 42.600 hiçbir kayda
            # girmedi; bonus ağustostan beri maaşa da dahil değil).
            # Artık referans, kapanış ANINA en yakın kapanmış vardiya (±6 saat).
            # Bulunamazsa KIRPMA YAPILMAZ — alakasız bir vardiyaya kırpmaktansa
            # istemcinin değerini geçmek yeğdir (istemci sırası düzeltildi,
            # bu katman yalnızca ikinci savunma).
            if daily_pay > 0:
                try:
                    _ref, _refd = None, None
                    for _r in db.execute(
                            "SELECT COALESCE(bonus,0) AS b, end_time FROM shifts "
                            "WHERE user_id=? AND end_time IS NOT NULL ORDER BY id DESC LIMIT 8",
                            (user.id,)).fetchall():
                        try:
                            _et = datetime.fromisoformat(_r["end_time"])
                        except Exception:
                            continue
                        if _et.tzinfo is None:
                            _et = _et.replace(tzinfo=TZ)
                        _dd = abs((now - _et).total_seconds())
                        if _dd <= 6 * 3600 and (_refd is None or _dd < _refd):
                            _ref, _refd = _r, _dd
                    if _ref is None:
                        logger.warning(
                            f"daily_pay kirpilmadi uid={user.id}: kapanisa yakin vardiya yok "
                            f"(istemci={daily_pay}) — shift_end gecikmis olabilir")
                    else:
                        _earned = int(_ref["b"] or 0)
                        if daily_pay > _earned:
                            logger.warning(
                                f"daily_pay kirpildi uid={user.id}: istemci={daily_pay} vardiya_bonusu={_earned}")
                            daily_pay = _earned
                except Exception as e:
                    logger.warning(f"daily_pay dogrulama basarisiz: {e}")
            cashless = clk + pay + kar + term
            schitano = itg - cashless
            exp_total = sum(int(e.get("a", 0) or 0) for e in exps)
            kassa = vsh - sdachi - daily_pay
            cups_total = sum(int(c.get("s", 0) or 0) for c in cups)
            # Günlük bonus için ARTIK «ödendi» KAYDI YAZILMIYOR.
            # Eskiden bonus aylık brüte eklenip burada aynı tutarda bir payments
            # kaydıyla geri düşülüyordu. İkisi normalde birbirini götürüyordu, ama:
            #   · kasa raporu gelmezse (ya da kasayı BAŞKASI gönderirse) kayıt hiç
            #     oluşmuyor ve bonus kişinin ay sonu alacağında kalıyordu;
            #   · oluşan kayıtlar «Выплаты» listesini maaş ödemeleriyle karıştırıyordu.
            # Artık bonus maaş hesabının hiçbir yerine girmiyor (calc_summary brütten
            # de çıkardı). Kasadan çıkan para `kassa` hesabında ve cashreports.daily_pay
            # alanında kayıtlı — bilgi kaybı yok.
            ostalos = {str(c.get("n", "")): int(c.get("o", 0) or 0) for c in cups}
            shift_hours = float(data.get("hours", 0) or 0)
            shift_start = (data.get("start_time") or "")
            shift_end = (data.get("end_time") or "")
            coffee_kg = float(data.get("coffee_kg", 0) or 0)  # kalan kahve çekirdeği (kg) — stok uyarısı için
            # ŞUBE: kasa raporu KAPANAN VARDİYANIN şubesine yazılmalı. Bu istek
            # shift_end'den SONRA geldiği için vardiya artık kapalıdır →
            # acting_branch_id() onu açık vardiyadan çözemez ve «oturum şubesi»ne
            # (cur_branch) ya da EV şubesine düşer. Owner başkası adına vardiya
            # başlattığında veya kapatma devredildiğinde rapor yanlış şubeye
            # yazılıyor, «Было» zinciri iki şube arasında karışıyordu.
            # Öncelik: istemcinin gönderdiği şube → az önce kapanan vardiyanın
            # şubesi → eski davranış (acting_branch_id).
            _cr_branch = None
            try:
                _pb = int(data.get("branch_id") or data.get("branch") or 0)
                if _pb and get_branch(db, _pb):
                    _cr_branch = _pb
            except Exception:
                pass
            if not _cr_branch:
                try:
                    _lb = db.execute(
                        "SELECT branch_id FROM shifts WHERE user_id=? AND end_time IS NOT NULL "
                        "ORDER BY id DESC LIMIT 1", (user.id,)).fetchone()
                    if _lb and _lb["branch_id"]:
                        _cr_branch = int(_lb["branch_id"])
                except Exception:
                    pass
            if not _cr_branch:
                _cr_branch = acting_branch_id(db, user.id)
            # Сменный отчёт + stok uyarısı da O şubenin grubuna gitsin.
            group_id = resolve_group_id(db, user.id, context, branch_id=_cr_branch) or group_id
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
            # DENETİM İZİ. Raporun DÜZELTİLMESİ günlüğe yazılıyordu ama İLK
            # GÖNDERİLMESİ yazılmıyordu — yani paranın kaydedildiği an,
            # «kim ne yaptı» sayfasında hiç görünmüyordu. Kayıt cashreports'ta
            # zaten var; buradaki satır onu ZAMAN ÇİZGİSİNE koyar.
            try:
                _cr_id = db.execute("SELECT id FROM cashreports WHERE user_id=? ORDER BY id DESC LIMIT 1",
                                    (user.id,)).fetchone()
                log_action(db, "cash_report", user.id, user.first_name, user.id, shown,
                           {"report_id": (_cr_id["id"] if _cr_id else 0),
                            "branch": (get_branch(db, _cr_branch) or {}).get("name", "") if _cr_branch else "",
                            "cups": cups_total, "kassa": kassa, "daily_pay": daily_pay,
                            "expenses": exp_total})
            except Exception as e:
                logger.warning(f"cash_report log: {e}")
            # Kapatma yapıldı → bu şubenin transfer override'ını temizle (oturum bitti).
            try:
                db.execute("DELETE FROM meta WHERE k=?", (f"closing_owner_{int(_cr_branch or 1)}",))
                db.commit()
            except Exception:
                pass
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

        # ─── Kasa raporu DÜZELTMESİ (owner) ───
        # Kapanış raporu sonradan düzeltilebilir: bardak zinciri (было/завоз/осталось),
        # расходы ve заметка. «Продано» ve toplamlar TÜRETİLİR, elle girilmez.
        # PARA alanlarına (итого/click/payme/karta/terminal/вышло/на сдачу/касса/
        # дневной бонус) DOKUNULMAZ — onlar POS ve ödenmiş nakitle bağlı.
        # ESKİ VERİ SİLİNMEZ: her düzeltme `cashreports.edits` dizisine {ne zaman,
        # kim, hangi alan eski→yeni} olarak eklenir + audit log'a yazılır.
        # Zincir etkisi: bu şubenin EN SON raporunun «осталось»u düzeltilirse
        # sonraki vardiyanın «Было» ön-dolumu (kasa_last) otomatik düzelir.
        elif action == "cash_report_edit":
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец.")
                return
            try:
                rid = int(data.get("id") or 0)
            except Exception:
                rid = 0
            rep = db.execute("SELECT * FROM cashreports WHERE id=?", (rid,)).fetchone() if rid else None
            if not rep:
                await update.message.reply_text("❌ Отчёт не найден.")
                return
            _rk = rep.keys()

            def _cr_j(v, d):
                try:
                    x = json.loads(v) if isinstance(v, str) and v else v
                    return x if isinstance(x, type(d)) else d
                except Exception:
                    return d

            old_bylo = _cr_j(rep["bylo"], {})
            old_rest = _cr_j(rep["restock"], {})
            old_ost = _cr_j(rep["ostalos"], {})
            old_sold = _cr_j(rep["sold"], {})
            old_exps = _cr_j(rep["expenses"], [])
            old_note = rep["note"] or ""
            # Bardaklar: istemci TÜM satırları [{n,b,r,o}] gönderir. «Завоз» negatif
            # olabilir (başka noktaya verildi) → max(0,…) UYGULANMAZ.
            cups_in = data.get("cups")
            if isinstance(cups_in, list) and cups_in:
                new_bylo, new_rest, new_ost, new_sold = {}, {}, {}, {}
                for _c in cups_in:
                    if not isinstance(_c, dict):
                        continue
                    _n = str(_c.get("n", "") or "")
                    if not _n:
                        continue
                    _b = max(0, int(_c.get("b", 0) or 0))
                    _r = int(_c.get("r", 0) or 0)
                    _o = max(0, int(_c.get("o", 0) or 0))
                    new_bylo[_n] = _b
                    new_rest[_n] = _r
                    new_ost[_n] = _o
                    new_sold[_n] = max(0, _b + _r - _o)
            else:
                new_bylo, new_rest, new_ost, new_sold = old_bylo, old_rest, old_ost, old_sold
            exps_in = data.get("expenses")
            if isinstance(exps_in, list):
                new_exps = [{"n": str(_e.get("n", "") or "").strip(), "a": _norm_amt(_e.get("a", 0))}
                            for _e in exps_in if isinstance(_e, dict)]
                new_exps = [_e for _e in new_exps if _e["n"] or _e["a"]]
            else:
                new_exps = old_exps
            new_note = data.get("note")
            new_note = old_note if new_note is None else str(new_note).strip()
            cups_total = sum(int(v or 0) for v in new_sold.values())
            exp_total = sum(int(_e.get("a", 0) or 0) for _e in new_exps)

            ch = {}

            def _cr_norm(x):
                # Karşılaştırma için: SIFIR = «yok». Eski raporlarda bir boy hiç
                # yazılmamış olabilir ({} vs {"300 мл":0}); istemci ise her boyu
                # gönderir → aksi hâlde hiçbir şey değişmeden «değişti» sanılırdı.
                return {k: int(v or 0) for k, v in x.items() if int(v or 0) != 0} if isinstance(x, dict) else x

            def _cr_cmp(key, o, n):
                _o, _n = _cr_norm(o), _cr_norm(n)
                if json.dumps(_o, ensure_ascii=False, sort_keys=True) != json.dumps(_n, ensure_ascii=False, sort_keys=True):
                    ch[key] = [o, n]

            _cr_cmp("bylo", old_bylo, new_bylo)
            _cr_cmp("restock", old_rest, new_rest)
            _cr_cmp("ostalos", old_ost, new_ost)
            _cr_cmp("sold", old_sold, new_sold)
            _cr_cmp("expenses", old_exps, new_exps)
            _cr_cmp("note", old_note, new_note)
            if not ch:
                await update.message.reply_text("ℹ️ Изменений нет.")
                return
            hist = _cr_j(rep["edits"] if "edits" in _rk else None, [])
            hist.append({"at": now.isoformat(), "by": user.id, "by_name": shown, "ch": ch})
            db.execute(
                "UPDATE cashreports SET bylo=?,restock=?,ostalos=?,sold=?,cups_total=?,"
                "expenses=?,expenses_total=?,note=?,edits=?,edited_at=?,edited_by=?,edited_by_name=? "
                "WHERE id=?",
                (json.dumps(new_bylo, ensure_ascii=False), json.dumps(new_rest, ensure_ascii=False),
                 json.dumps(new_ost, ensure_ascii=False), json.dumps(new_sold, ensure_ascii=False),
                 cups_total, json.dumps(new_exps, ensure_ascii=False), exp_total, new_note,
                 json.dumps(hist, ensure_ascii=False), now.isoformat(), user.id, shown, rid))
            db.commit()
            log_action(db, "cash_report_edit", user.id, user.first_name,
                       rep["user_id"], rep["user_name"] or "",
                       {"report_id": rid, "changes": ch})
            _lbl = {"bylo": "было", "restock": "завоз", "ostalos": "осталось",
                    "sold": "продано", "expenses": "расходы", "note": "заметка"}
            _what = " · ".join(_lbl.get(k, k) for k in ch)
            try:
                _rd = datetime.fromisoformat(rep["created_at"]).strftime("%d.%m %H:%M")
            except Exception:
                _rd = rep["date"] or "?"
            await update.message.reply_text(
                f"✏️ Отчёт исправлен — *{rep['user_name'] or '?'}* · {_rd}\n"
                f"Изменено: {_what}\n"
                f"🥤 Продано: {cups_total} шт · 💸 Расходы: {fmt_sum(exp_total)} сум\n"
                "_Прежние значения сохранены в истории отчёта._",
                parse_mode="Markdown")
            await refresh_webapp_keyboard(update, context, db, user, "🔄 Отчёт обновлён 👇")

        # ─── Yedeği ŞİMDİ gönder (owner) ───
        # Otomatiği beklemeden kopya almak için. Önemli bir değişiklikten önce
        # (toplu silme, ay kapanışı, riskli bir düzeltme) elle çekilir.
        elif action == "backup_now":
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец.")
                return
            await update.message.reply_text("🗄 Готовлю резервную копию…")
            try:
                _n = await send_backup(context.bot, "по запросу")
            except Exception as e:
                logger.error(f"backup_now: {e}")
                await update.message.reply_text(f"❌ Не удалось создать копию: {e}")
                return
            if not _n:
                await update.message.reply_text("⚠️ Копия не отправлена — проверьте логи.")
            log_action(db, "backup_now", user.id, user.first_name, None, None, {"sent": _n})

        # ─── Ödemeyi başka bir maaş ayına taşı (owner) ───
        # Ödeme YAPILDIĞI ay ile AİT OLDUĞU ay farklı olabilir: temmuz maaşı
        # 1 Ağustos'ta ödenir. Nero eskiden ayı hiç göndermediği için bot
        # «ödemenin yapıldığı ay» diye damgalıyordu → o ay fazla ödenmiş,
        # önceki ay hâlâ borçlu görünüyordu. Bu eylem o damgayı düzeltir.
        # Tutar ve tarih DEĞİŞMEZ; yalnızca hangi ayın hesabına sayılacağı değişir.
        elif action == "pay_move_period":
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец.")
                return
            try:
                _pid = int(data.get("id") or 0)
            except Exception:
                _pid = 0
            _newp = str(data.get("period") or "").strip()[:7]
            if not re.match(r"^\d{4}-\d{2}$", _newp or ""):
                await update.message.reply_text("❌ Неверный месяц.")
                return
            prow = db.execute("SELECT * FROM payments WHERE id=?", (_pid,)).fetchone() if _pid else None
            if not prow:
                await update.message.reply_text("❌ Выплата не найдена.")
                return
            _oldp = prow["period"] or ""
            if _oldp == _newp:
                await update.message.reply_text("ℹ️ Выплата уже в этом месяце.")
                return
            db.execute("UPDATE payments SET period=? WHERE id=?", (_newp, _pid))
            db.commit()
            _pnm = display_name_for(db, prow["user_id"], fallback="?")
            log_action(db, "pay_move_period", user.id, user.first_name,
                       prow["user_id"], _pnm,
                       {"pay_id": _pid, "amount": prow["amount"] or 0,
                        "from": _oldp, "to": _newp, "paid_at": prow["paid_at"] or ""})
            await update.message.reply_text(
                f"↔️ Выплата *{md_safe(_pnm)}* — {fmt_sum(prow['amount'] or 0)} сум\n"
                f"перенесена: {md_safe(_oldp or '—')} → *{md_safe(_newp)}*\n"
                "_Сумма и дата не изменились — изменился только месяц, к которому она относится._",
                parse_mode="Markdown")
            await refresh_webapp_keyboard(update, context, db, user, "🔄 Готово 👇")

        # ─── Cihaz kararı (owner): onayla · çıkar · geri al ───
        elif action == "device_decide":
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец.")
                return
            try:
                _did = int(data.get("id") or 0)
            except Exception:
                _did = 0
            dec = str(data.get("decision") or "").strip()
            drow = db.execute("SELECT * FROM devices WHERE id=?", (_did,)).fetchone() if _did else None
            if not drow or dec not in ("approve", "revoke", "delete"):
                await update.message.reply_text("❌ Устройство не найдено.")
                return
            _dnm = display_name_for(db, drow["user_id"], fallback="?")
            if dec == "approve":
                db.execute("UPDATE devices SET approved=1, revoked=0 WHERE id=?", (_did,))
                _txt = f"✅ Устройство подтверждено — *{md_safe(_dnm)}*"
            elif dec == "revoke":
                db.execute("UPDATE devices SET revoked=1, approved=0 WHERE id=?", (_did,))
                _txt = f"🚫 Устройство отключено — *{md_safe(_dnm)}*"
            else:
                # Kaydı tamamen sil: o cihaz bir daha girerse YENİDEN sıfırdan
                # değerlendirilir (kişinin başka cihazı varsa yine onay bekler).
                db.execute("DELETE FROM devices WHERE id=?", (_did,))
                _txt = f"🗑 Устройство удалено — *{md_safe(_dnm)}*"
            db.commit()
            log_action(db, "device_" + dec, user.id, user.first_name,
                       drow["user_id"], _dnm,
                       {"device": (drow["device_id"] or "")[:12], "platform": drow["platform"] or ""})
            # Sahibine haber ver — beklediği onay geldiyse uygulamayı yeniden açsın.
            try:
                if dec == "approve":
                    await context.bot.send_message(
                        drow["user_id"], "✅ Ваше устройство подтверждено. Откройте приложение заново.")
                elif dec == "revoke":
                    await context.bot.send_message(
                        drow["user_id"], "🚫 Доступ с этого устройства отключён владельцем.")
            except Exception as e:
                logger.warning(f"device notify user failed: {e}")
            await update.message.reply_text(_txt, parse_mode="Markdown")

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

        # ═══════════════════════════════════════════════════════════════════
        # НЕРО (yeni uygulama) — eski index.html'de OLMAYAN 7 action.
        # Bot bunları tanımazsa değişiklik sessizce kaybolur (bkz HANDOFF.md).
        # ═══════════════════════════════════════════════════════════════════
        elif action == "shift_grid_set":
            # График: bir güne vardiya/выходной ata. Kendine выходной koyulabilir;
            # başkasına atama SADECE owner. Göreli hafta mutlak Пн tarihine bağlanır.
            db = get_db()
            try:
                target_id = int(data.get("target_uid") or 0)
            except Exception:
                target_id = 0
            code = (data.get("code") or "").strip()
            if not target_id or not code:
                await update.message.reply_text("❌ Не указан сотрудник или смена.")
                return
            if target_id != user.id and get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Изменять чужой график может только владелец.")
                return
            # 'none' = HÜCREYİ BOŞALT. İzin alındıktan sonra onu geri almanın yolu
            # yoktu: plan yalnızca «vardiya» ya da «выходной» tutabiliyordu, üçüncü
            # bir durum (henüz atanmadı) yazılamıyordu. Artık izin iptal edilince
            # gün boşa döner ve owner istediği vardiyayı atar.
            if code == "none":
                wk0 = grid_week_key(data.get("week"))
                try:
                    day0 = int(data.get("day"))
                except Exception:
                    day0 = 0
                db.execute("DELETE FROM shift_grid WHERE week_key=? AND day=? AND user_id=?",
                           (wk0, day0, target_id))
                db.commit()
                _nm0 = display_name_for(db, target_id, fallback="?")
                log_action(db, "shift_grid_set", user.id, user.first_name, target_id, _nm0,
                           {"week_key": wk0, "day": day0, "code": "none"})
                _dl0 = grid_day_label(day0)
                await update.message.reply_text(
                    f"🗓 {md_safe(_nm0)} · {_dl0} — день освобождён (смена не назначена).",
                    parse_mode="Markdown")
                if target_id != user.id:
                    try:
                        await context.bot.send_message(
                            chat_id=target_id, text=f"🗓 Ваш график изменён: {_dl0} — смена не назначена.")
                    except Exception:
                        pass
                return
            wk = grid_week_key(data.get("week"))
            try:
                day = int(data.get("day"))
            except Exception:
                day = 0
            # KURALLAR — kapasite ve çakışma. Eskiden hiçbir kontrol yoktu:
            # aynı kişi aynı saatte iki şubeye, bir şubeye sınırsız kişi
            # yazılabiliyordu ve plan sessizce imkânsız hâle geliyordu.
            _ok, _why = grid_check(db, wk, day, target_id, code)
            if not _ok:
                await update.message.reply_text("⚠️ " + _why)
                return
            # HAFTALIK İZİN LİMİTİ yalnızca KENDİNE izin koyan kişiye uygulanır.
            # Limiti owner belirliyor; onu kendi kararında da bağlamak yanlıştı:
            # biri hastalandığında ya da acil bir durumda owner üçüncü izni
            # veremiyordu. Owner sınırın üstünde de atayabilir — sorumluluk onda.
            if code == "off" and get_role(db, user.id) != "owner":
                _ok2, _why2 = grid_off_allowed(db, wk, target_id, day)
                if not _ok2:
                    await update.message.reply_text("⚠️ " + _why2)
                    return
            db.execute(
                "INSERT OR REPLACE INTO shift_grid (week_key, day, user_id, code, updated_by, updated_by_name, updated_at) "
                "VALUES (?,?,?,?,?,?,?)",
                (wk, day, target_id, code, user.id, user.first_name, now.isoformat()))
            # TUTARLILIK: bu gün için AÇIK bir vardiya varsa ve owner doğrudan
            # birini atadıysa o ilan kapanır. Yoksa vardiya dolu olduğu hâlde
            # «открытая смена» olarak asılı kalır ve özet yanlış alarm verir.
            if code and code != "off":
                try:
                    db.execute(
                        "UPDATE open_shifts SET status='done', decided_by=?, decided_by_name=?, decided_at=? "
                        "WHERE week_key=? AND day=? AND code=? AND status IN ('open','claimed')",
                        (user.id, shown, now.isoformat(), wk, day, code))
                except Exception as e:
                    logger.warning(f"open_shift auto-close: {e}")
            db.commit()
            _nm = display_name_for(db, target_id, fallback="?")
            log_action(db, "shift_grid_set", user.id, user.first_name, target_id, _nm,
                       {"week_key": wk, "day": day, "code": code})
            _dl = grid_day_label(day)
            _what = "выходной" if code == "off" else f"смена «{code}»"
            await update.message.reply_text(
                f"🗓 График обновлён: *{_nm}* · {_dl} → {_what}", parse_mode="Markdown")
            if target_id != user.id:
                try:
                    await context.bot.send_message(
                        chat_id=target_id, text=f"🗓 Ваш график изменён: {_dl} — {_what}.")
                except Exception:
                    pass

        elif action == "shift_reassign":
            # График: bir günün vardiyası bir personelden diğerine devredilir.
            # from_uid → выходной, to_uid → code. Sadece owner.
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец.")
                return
            try:
                from_id = int(data.get("from_uid") or 0)
                to_id = int(data.get("to_uid") or 0)
            except Exception:
                from_id = to_id = 0
            code = (data.get("code") or "").strip()
            if not from_id or not to_id or not code:
                await update.message.reply_text("❌ Не хватает данных для передачи смены.")
                return
            wk = grid_week_key(data.get("week"))
            try:
                day = int(data.get("day"))
            except Exception:
                day = 0
            # DEVREDİLEN kişi bu vardiyayı gerçekten alabiliyor mu? Devreden
            # kişi çıkacağı için kapasite hesabında onu SAYMA — yoksa dolu bir
            # şubede devir hiç mümkün olmazdı.
            db.execute("DELETE FROM shift_grid WHERE week_key=? AND day=? AND user_id=?",
                       (wk, day, from_id))
            _ok, _why = grid_check(db, wk, day, to_id, code)
            if not _ok:
                # Devreden kişiyi geri koy — kontrol için kaldırmıştık.
                db.execute(
                    "INSERT OR REPLACE INTO shift_grid (week_key, day, user_id, code, updated_by, updated_by_name, updated_at) "
                    "VALUES (?,?,?,?,?,?,?)",
                    (wk, day, from_id, code, user.id, user.first_name, now.isoformat()))
                db.commit()
                await update.message.reply_text("⚠️ " + _why)
                return
            for _uid, _c in ((from_id, "off"), (to_id, code)):
                db.execute(
                    "INSERT OR REPLACE INTO shift_grid (week_key, day, user_id, code, updated_by, updated_by_name, updated_at) "
                    "VALUES (?,?,?,?,?,?,?)",
                    (wk, day, _uid, _c, user.id, user.first_name, now.isoformat()))
            db.commit()
            _fn = display_name_for(db, from_id, fallback="?")
            _tn = display_name_for(db, to_id, fallback="?")
            log_action(db, "shift_reassign", user.id, user.first_name, to_id, _tn,
                       {"week_key": wk, "day": day, "code": code, "from": from_id})
            _dl = grid_day_label(day)
            await update.message.reply_text(
                f"🔄 Смена передана: {_dl} · *{_fn}* → *{_tn}*", parse_mode="Markdown")
            for _uid, _txt in ((from_id, f"🔄 Ваша смена {_dl} передана {_tn}. У вас выходной."),
                               (to_id, f"🔄 Вам передали смену {_dl} (смена «{code}»).")):
                try:
                    await context.bot.send_message(chat_id=_uid, text=_txt)
                except Exception:
                    pass

        # ─── Vardiya ŞABLONU kaydet/sil (owner) ───
        elif action == "shift_tpl_save":
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец.")
                return
            _tc = re.sub(r"[^a-z0-9_]", "", str(data.get("code") or "").strip().lower())[:16]
            if not _tc:
                await update.message.reply_text("❌ Укажите код смены.")
                return
            try:
                _tb = int(data.get("branch_id") or 0)
            except Exception:
                _tb = 0
            if not _tb or not get_branch(db, _tb):
                await update.message.reply_text("❌ Укажите филиал.")
                return
            _ts = str(data.get("start") or "").strip()[:5]
            _te = str(data.get("end") or "").strip()[:5]
            if _mins(_ts) is None or _mins(_te) is None:
                await update.message.reply_text("❌ Время в формате ЧЧ:ММ.")
                return
            _ta = 0 if str(data.get("active", 1)) in ("0", "False", "false") else 1
            db.execute(
                "INSERT OR REPLACE INTO shift_templates (code, branch_id, start_t, end_t, active, sort_order, updated_by, updated_at) "
                "VALUES (?,?,?,?,?,COALESCE((SELECT sort_order FROM shift_templates WHERE code=?),0),?,?)",
                (_tc, _tb, _ts, _te, _ta, _tc, user.id, now.isoformat()))
            db.commit()
            log_action(db, "shift_tpl_save", user.id, user.first_name, None, _tc,
                       {"code": _tc, "branch_id": _tb, "start": _ts, "end": _te, "active": _ta})
            await update.message.reply_text(
                f"🗓 Шаблон «{md_safe(_tc)}» сохранён: {md_safe(_ts)}–{md_safe(_te)} · "
                f"{md_safe((get_branch(db, _tb) or {})['name'])}", parse_mode="Markdown")

        elif action == "shift_tpl_delete":
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец.")
                return
            _tc = str(data.get("code") or "").strip()[:16]
            _used = db.execute("SELECT COUNT(*) AS c FROM shift_grid WHERE code=?", (_tc,)).fetchone()
            if _used and (_used["c"] or 0) > 0:
                # Planda KULLANILIYORSA silme — pasife al. Silmek geçmiş haftaların
                # hücrelerini anlamsız kodlara çevirirdi.
                db.execute("UPDATE shift_templates SET active=0, updated_at=? WHERE code=?",
                           (now.isoformat(), _tc))
                db.commit()
                log_action(db, "shift_tpl_delete", user.id, user.first_name, None, _tc,
                           {"code": _tc, "mode": "deactivated", "used": _used["c"]})
                await update.message.reply_text(
                    f"🗓 Шаблон «{md_safe(_tc)}» отключён (используется в графике — записи сохранены).",
                    parse_mode="Markdown")
                return
            db.execute("DELETE FROM shift_templates WHERE code=?", (_tc,))
            # Tohum listesindeki bir kodu owner sildiyse bir daha geri gelmesin.
            try:
                _kr2 = db.execute("SELECT val FROM meta WHERE k='seed_tpl_removed'").fetchone()
                _ks = set(json.loads(_kr2["val"])) if (_kr2 and _kr2["val"]) else set()
                _ks.add(_tc)
                db.execute("INSERT OR REPLACE INTO meta (k,val) VALUES ('seed_tpl_removed',?)",
                           (json.dumps(sorted(_ks)),))
            except Exception:
                pass
            db.commit()
            log_action(db, "shift_tpl_delete", user.id, user.first_name, None, _tc, {"code": _tc})
            await update.message.reply_text(f"🗑 Шаблон «{md_safe(_tc)}» удалён.", parse_mode="Markdown")

        # ─── Plan KURALLARI (owner): haftalık izin limiti · şube kapasitesi ───
        elif action == "shift_rules_save":
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец.")
                return
            _parts = []
            if data.get("weekly_off_limit") is not None:
                try:
                    _wl = max(0, min(7, int(data.get("weekly_off_limit"))))
                    db.execute("INSERT OR REPLACE INTO meta (k,val) VALUES ('weekly_off_limit',?)", (str(_wl),))
                    _parts.append(f"выходных в неделю: {_wl}")
                except Exception:
                    pass
            _bc = data.get("branch_caps")
            if isinstance(_bc, dict):
                for _bid, _mx in _bc.items():
                    try:
                        db.execute("UPDATE branches SET max_staff=? WHERE id=?",
                                   (max(0, min(20, int(_mx))), int(_bid)))
                    except Exception:
                        pass
                _parts.append("лимиты филиалов обновлены")
            _po = data.get("person_off")
            if isinstance(_po, dict):
                for _uid, _lim in _po.items():
                    try:
                        _v = None if _lim in (None, "", "auto") else max(0, min(7, int(_lim)))
                        db.execute("UPDATE users SET off_limit=? WHERE user_id=?", (_v, int(_uid)))
                    except Exception:
                        pass
                _parts.append("личные лимиты обновлены")
            # «Kendi izin gününü koyabilir mi» — kişi bazlı yetki.
            _ps = data.get("person_self")
            if isinstance(_ps, dict):
                for _uid, _sv in _ps.items():
                    try:
                        db.execute("UPDATE users SET off_self=? WHERE user_id=?",
                                   (0 if str(_sv) in ("0", "False", "false") else 1, int(_uid)))
                    except Exception:
                        pass
                _parts.append("права на выходной обновлены")
            db.commit()
            log_action(db, "shift_rules_save", user.id, user.first_name, None, None,
                       {"weekly_off_limit": data.get("weekly_off_limit"),
                        "branch_caps": _bc if isinstance(_bc, dict) else None})
            await update.message.reply_text(
                "✅ Правила графика обновлены" + (":\n· " + "\n· ".join(_parts) if _parts else "."))

        # ─── İZİN TALEBİ (barista → owner) ───
        # BU EYLEM HİÇ YOKTU. Talep yalnızca istemcinin hafızasında duruyor,
        # hiçbir yere kaydedilmiyordu: owner uygulamayı yeniden açtığında talep
        # yok oluyordu. Yani izin akışı gerçekte hiç çalışmıyordu.
        elif action == "dayoff_request":
            db = get_db()
            try:
                day = int(data.get("day"))
            except Exception:
                day = 0
            wk = grid_week_key(data.get("week"))
            note = (data.get("note") or "").strip()[:200]
            # HAFTALIK LİMİT — talep aşamasında kontrol edilir, onayda tekrar.
            _ok, _why = grid_off_allowed(db, wk, user.id, day)
            if not _ok:
                await update.message.reply_text("⚠️ " + _why)
                return
            _dup = db.execute(
                "SELECT id FROM dayoff_requests WHERE user_id=? AND week_key=? AND day=? AND status='pending'",
                (user.id, wk, day)).fetchone()
            if _dup:
                await update.message.reply_text("⏳ Заявка на этот день уже отправлена.")
                return
            _cur = db.execute("SELECT code FROM shift_grid WHERE week_key=? AND day=? AND user_id=?",
                              (wk, day, user.id)).fetchone()
            _curcode = (_cur["code"] if _cur else "") or ""
            db.execute(
                "INSERT INTO dayoff_requests (user_id, week_key, day, note, status, created_at) "
                "VALUES (?,?,?,?,'pending',?)",
                (user.id, wk, day, note, now.isoformat()))
            db.commit()
            log_action(db, "dayoff_request", user.id, user.first_name, user.id, shown,
                       {"week_key": wk, "day": day, "note": note, "code": _curcode})
            _dl = grid_day_label(day)
            # VARDİYA SİLİNMEZ — onaya kadar planda aynen kalır.
            await update.message.reply_text(
                f"📨 Заявка на выходной отправлена: {_dl}.\n"
                "Смена останется в графике, пока владелец не одобрит.")
            for _o in db.execute("SELECT user_id FROM users WHERE role='owner'").fetchall():
                try:
                    await context.bot.send_message(
                        _o["user_id"],
                        f"🔔 *Новый запрос на выходной*\n{md_safe(shown)} · {_dl}"
                        + (f"\nПричина: {md_safe(note)}" if note else "")
                        + "\n\nУправление → График смен",
                        parse_mode="Markdown")
                except Exception:
                    pass

        # ─── AÇIK VARDİYAYA TALİP OLMA (barista) ───
        elif action == "open_shift_claim":
            db = get_db()
            try:
                osid = int(data.get("id") or 0)
            except Exception:
                osid = 0
            row = db.execute("SELECT * FROM open_shifts WHERE id=?", (osid,)).fetchone() if osid else None
            if not row or (row["status"] or "") != "open":
                await update.message.reply_text("❌ Смена уже занята или отменена.")
                return
            # Talip olan kişi bu vardiyayı gerçekten alabiliyor mu?
            _ok, _why = grid_check(db, row["week_key"], row["day"], user.id, row["code"])
            if not _ok:
                await update.message.reply_text("⚠️ " + _why)
                return
            db.execute("UPDATE open_shifts SET status='claimed', claim_uid=?, claim_name=?, claim_at=? WHERE id=?",
                       (user.id, shown, now.isoformat(), osid))
            db.commit()
            log_action(db, "open_shift_claim", user.id, user.first_name, user.id, shown,
                       {"open_id": osid, "day": row["day"], "code": row["code"]})
            _dl = grid_day_label(row["day"])
            # KESİNLEŞMEZ — owner onayına gider.
            await update.message.reply_text(
                f"📨 Заявка отправлена: {_dl} · смена «{row['code']}».\n"
                "Смена станет вашей после подтверждения владельца.")
            for _o in db.execute("SELECT user_id FROM users WHERE role='owner'").fetchall():
                try:
                    await context.bot.send_message(
                        _o["user_id"],
                        f"🔔 *Запрос на замену*\n{md_safe(shown)} хочет взять смену "
                        f"{_dl} «{md_safe(row['code'])}»"
                        + (f" (от {md_safe(row['from_name'] or '?')})" if row["from_name"] else "")
                        + "\n\nУправление → График смен",
                        parse_mode="Markdown")
                except Exception:
                    pass

        # ─── AÇIK VARDİYA KARARI (owner) ───
        elif action == "open_shift_decide":
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец.")
                return
            try:
                osid = int(data.get("id") or 0)
            except Exception:
                osid = 0
            dec = "ok" if (data.get("decision") == "ok") else "no"
            row = db.execute("SELECT * FROM open_shifts WHERE id=?", (osid,)).fetchone() if osid else None
            if not row:
                await update.message.reply_text("❌ Смена не найдена.")
                return
            _dl = grid_day_label(row["day"])
            if dec == "no":
                # Reddedildi → vardiya YİNE AÇIK kalır, başkası talip olabilir.
                db.execute("UPDATE open_shifts SET status='open', claim_uid=NULL, claim_name=NULL, "
                           "decided_by=?, decided_by_name=?, decided_at=? WHERE id=?",
                           (user.id, shown, now.isoformat(), osid))
                db.commit()
                log_action(db, "open_shift_decide", user.id, user.first_name,
                           row["claim_uid"], row["claim_name"] or "", {"open_id": osid, "decision": "no"})
                if row["claim_uid"]:
                    try:
                        await context.bot.send_message(
                            row["claim_uid"], f"❌ Заявка на смену {_dl} отклонена.")
                    except Exception:
                        pass
                await update.message.reply_text("❌ Заявка отклонена. Смена снова открыта.")
                return
            if not row["claim_uid"]:
                await update.message.reply_text("❌ На эту смену никто не претендует.")
                return
            _ok, _why = grid_check(db, row["week_key"], row["day"], row["claim_uid"], row["code"])
            if not _ok:
                await update.message.reply_text("⚠️ " + _why)
                return
            db.execute(
                "INSERT OR REPLACE INTO shift_grid (week_key, day, user_id, code, updated_by, updated_by_name, updated_at) "
                "VALUES (?,?,?,?,?,?,?)",
                (row["week_key"], row["day"], row["claim_uid"], row["code"], user.id, shown, now.isoformat()))
            db.execute("UPDATE open_shifts SET status='done', decided_by=?, decided_by_name=?, decided_at=? WHERE id=?",
                       (user.id, shown, now.isoformat(), osid))
            db.commit()
            log_action(db, "open_shift_decide", user.id, user.first_name,
                       row["claim_uid"], row["claim_name"] or "",
                       {"open_id": osid, "decision": "ok", "day": row["day"], "code": row["code"],
                        "from": row["from_uid"], "from_name": row["from_name"] or ""})
            await update.message.reply_text(
                f"✅ Смена {_dl} «{md_safe(row['code'])}» закреплена за *{md_safe(row['claim_name'] or '?')}*",
                parse_mode="Markdown")
            for _uid, _txt in ((row["claim_uid"], f"✅ Ваша смена на {_dl} подтверждена."),
                               (row["from_uid"], f"🔄 Вашу смену {_dl} принял {row['claim_name'] or '?'}.")):
                if not _uid:
                    continue
                try:
                    await context.bot.send_message(_uid, _txt)
                except Exception:
                    pass

        elif action == "dayoff_decide":
            # Выходной заявкasına owner kararı. ok → grid'e выходной + заявка 'ok';
            # aksi → 'no'. request_id = client Date.now() (INTEGER).
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец.")
                return
            try:
                req_id = int(data.get("request_id") or 0)
            except Exception:
                req_id = 0
            decision = "ok" if (data.get("decision") == "ok") else "no"
            try:
                target_id = int(data.get("target_uid") or 0)
            except Exception:
                target_id = 0
            wk = grid_week_key(data.get("week"))
            try:
                day = int(data.get("day"))
            except Exception:
                day = 0
            _ex = db.execute("SELECT * FROM dayoff_requests WHERE id=?", (req_id,)).fetchone() if req_id else None
            if _ex:
                db.execute(
                    "UPDATE dayoff_requests SET status=?, decided_by=?, decided_by_name=?, decided_at=? WHERE id=?",
                    (decision, user.id, user.first_name, now.isoformat(), req_id))
                if not target_id:
                    target_id = _ex["user_id"]
                if not wk:
                    wk = _ex["week_key"]
            elif req_id:
                db.execute(
                    "INSERT OR REPLACE INTO dayoff_requests "
                    "(id, user_id, week_key, day, note, status, decided_by, decided_by_name, decided_at, created_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (req_id, target_id, wk, day, "", decision, user.id, user.first_name, now.isoformat(), now.isoformat()))
            if decision == "ok" and target_id:
                # Onaydan ÖNCEKİ vardiya kodunu al: boşalan vardiya AÇIK VARDİYA
                # olarak ilan edilsin, kimse görmeden kaybolmasın.
                _was = db.execute("SELECT code FROM shift_grid WHERE week_key=? AND day=? AND user_id=?",
                                  (wk, day, target_id)).fetchone()
                _wascode = (_was["code"] if _was else "") or ""
                db.execute(
                    "INSERT OR REPLACE INTO shift_grid (week_key, day, user_id, code, updated_by, updated_by_name, updated_at) "
                    "VALUES (?,?,?,?,?,?,?)",
                    (wk, day, target_id, "off", user.id, user.first_name, now.isoformat()))
                if _wascode and _wascode != "off":
                    _tpl = grid_templates(db).get(_wascode) or {}
                    _dupo = db.execute(
                        "SELECT id FROM open_shifts WHERE week_key=? AND day=? AND code=? "
                        "AND status IN ('open','claimed')", (wk, day, _wascode)).fetchone()
                    if not _dupo:
                        db.execute(
                            "INSERT INTO open_shifts (week_key, day, code, branch_id, from_uid, from_name, "
                            "status, reason, created_at) VALUES (?,?,?,?,?,?,'open',?,?)",
                            (wk, day, _wascode, _tpl.get("branch_id"), target_id,
                             display_name_for(db, target_id, fallback="?"),
                             "выходной согласован", now.isoformat()))
            db.commit()
            _nm = display_name_for(db, target_id, fallback="?") if target_id else "?"
            log_action(db, "dayoff_decide", user.id, user.first_name, target_id or None, _nm,
                       {"request_id": req_id, "decision": decision, "week_key": wk, "day": day})
            _dl = grid_day_label(day)
            if decision == "ok":
                await update.message.reply_text(f"✅ Выходной согласован: *{_nm}* · {_dl}", parse_mode="Markdown")
                _msg = f"✅ Ваш выходной на {_dl} согласован."
            else:
                await update.message.reply_text(f"❌ Выходной отклонён: *{_nm}* · {_dl}", parse_mode="Markdown")
                _msg = f"❌ Ваша заявка на выходной ({_dl}) отклонена."
            if target_id and target_id != user.id:
                try:
                    await context.bot.send_message(chat_id=target_id, text=_msg)
                except Exception:
                    pass

        elif action == "schedule_order":
            # Планлы sipariş oluştur → scheduled_orders'a yaz; scheduled_orders_loop
            # zamanı gelince gruba gönderir. items = {key: qty}; names varsa okunur satırlar.
            db = get_db()
            _at = _parse_user_time((data.get("at") or "").strip())
            if not _at or _at <= datetime.now(TZ).replace(tzinfo=None) + timedelta(minutes=1):
                await update.message.reply_text("❌ Выберите время в будущем.")
                return
            items = data.get("items") or {}
            try:
                total = sum(int(v or 0) for v in items.values())
            except Exception:
                total = 0
            if total <= 0:
                await update.message.reply_text("❌ В заказе нет позиций.")
                return
            _bid = None
            _br = data.get("branch")
            if _br is not None and str(_br) != "":
                if str(_br).isdigit():
                    _bid = int(_br)
                else:
                    _row = db.execute(
                        "SELECT id FROM branches WHERE name=? AND COALESCE(active,1)=1", (str(_br),)).fetchone()
                    _bid = int(_row["id"]) if _row else None
            if not _bid:
                _bid = acting_branch_id(db, user.id)
            _gid = branch_group_id(db, _bid) or ""
            _lines = sched_body_lines(items, data.get("names"))
            _shown = display_name_for(db, user.id, fallback=user.first_name)
            db.execute(
                "INSERT INTO scheduled_orders (user_id,user_name,group_id,branch_id,body,total,items,send_at,created_at,sent,canceled) "
                "VALUES (?,?,?,?,?,?,?,?,?,0,0)",
                (user.id, _shown, str(_gid) if _gid else "", _bid,
                 "\n".join(_lines), total, json.dumps(items, ensure_ascii=False),
                 _at.isoformat(), now.isoformat()))
            db.commit()
            log_action(db, "schedule_order", user.id, user.first_name, None, None,
                       {"at": _at.isoformat(), "total": total, "branch_id": _bid})
            await update.message.reply_text(
                f"⏰ Заказ запланирован на *{_at.strftime('%d.%m.%Y %H:%M')}* ({total} поз.).\n"
                f"Он автоматически уйдёт в группу в это время.\n"
                f"_(Отменить — в приложении: Заказ → Запланированные.)_",
                parse_mode="Markdown")

        elif action == "schedule_update":
            # Планлы siparişi düzenle: send_at + items + body güncelle (gönderilmemişse).
            db = get_db()
            try:
                sid = int(data.get("id") or 0)
            except Exception:
                sid = 0
            row = db.execute("SELECT * FROM scheduled_orders WHERE id=?", (sid,)).fetchone() if sid else None
            if not row:
                await update.message.reply_text("❌ Запланированный заказ не найден.")
                return
            if row["user_id"] != user.id and get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Изменять чужой заказ может только владелец.")
                return
            if int(row["sent"] or 0) or int(row["canceled"] or 0):
                await update.message.reply_text("ℹ️ Заказ уже отправлен или отменён.")
                return
            _at = _parse_user_time((data.get("at") or "").strip())
            if not _at or _at <= datetime.now(TZ).replace(tzinfo=None) + timedelta(minutes=1):
                await update.message.reply_text("❌ Выберите время в будущем.")
                return
            items = data.get("items") or {}
            try:
                total = sum(int(v or 0) for v in items.values())
            except Exception:
                total = 0
            if total <= 0:
                await update.message.reply_text("❌ В заказе нет позиций.")
                return
            _lines = sched_body_lines(items, data.get("names"))
            db.execute(
                "UPDATE scheduled_orders SET send_at=?, items=?, body=?, total=? WHERE id=?",
                (_at.isoformat(), json.dumps(items, ensure_ascii=False), "\n".join(_lines), total, sid))
            db.commit()
            log_action(db, "schedule_update", user.id, user.first_name, None, None,
                       {"id": sid, "at": _at.isoformat(), "total": total})
            await update.message.reply_text(
                f"✏️ Заказ обновлён — уйдёт *{_at.strftime('%d.%m.%Y %H:%M')}* ({total} поз.).",
                parse_mode="Markdown")

        elif action == "category_save":
            # Sipariş kataloğuna kategori başlığı ekle. Nero'da id = ad (setState catalog).
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец.")
                return
            name = (data.get("name") or "").strip()
            if not name:
                await update.message.reply_text("❌ Укажите название категории.")
                return
            _cid = name
            _mx = db.execute("SELECT COALESCE(MAX(sort_order),0) AS m FROM order_categories").fetchone()
            _so = int((_mx["m"] if _mx else 0) or 0) + 1
            db.execute(
                "INSERT OR REPLACE INTO order_categories (id, name, sort_order, created_by, created_at, deleted) "
                "VALUES (?,?,?,?,?,0)",
                (_cid, name, _so, user.id, now.isoformat()))
            db.commit()
            log_action(db, "category_save", user.id, user.first_name, None, None, {"id": _cid, "name": name})
            await update.message.reply_text(f"✅ Категория добавлена: *{name}*", parse_mode="Markdown")

        elif action == "category_delete":
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец.")
                return
            _cid = str(data.get("id") or "").strip()
            if not _cid:
                await update.message.reply_text("❌ Категория не указана.")
                return
            db.execute("UPDATE order_categories SET deleted=1 WHERE id=?", (_cid,))
            db.commit()
            log_action(db, "category_delete", user.id, user.first_name, None, None, {"id": _cid})
            await update.message.reply_text("🗑 Категория удалена.")

        elif action == "order_item_delete":
            # Kategori içinden ürün silindi. Sipariş kataloğunun ürünleri Nero tarafında
            # (nero-data.js + client state) tutuluyor — bot'ta ürün deposu yok, bu yüzden
            # karar AUDIT olarak kaydedilir (owner işlemi log_action'da izlenebilir).
            # category = kategori id, index = kategori içindeki sıra.
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец.")
                return
            _cat = str(data.get("category") or "").strip()
            try:
                _idx = int(data.get("index"))
            except Exception:
                _idx = -1
            log_action(db, "order_item_delete", user.id, user.first_name, None, None,
                       {"category": _cat, "index": _idx})
            # Client zaten «Товар удалён» toast'ı gösterir; ek reply gerekmiyor
            # (auto /start yine de klavyeyi tazeler). cancel_scheduled ile aynı sessiz desen.

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
            # Menü butonu da Nero yönlendirmesinden geçsin — yoksa listedeki barista
            # ≡ menüden açtığında eski uygulamayı görür.
            _mu = build_webapp_url(WEBAPP_URL, user_id, "", db)
            await bot.set_chat_menu_button(
                chat_id=user_id,
                menu_button=MenuButtonWebApp(text="☕ Caffelito", web_app=WebAppInfo(url=_mu))
            )
    except Exception as e:
        logger.warning(f"sync_user_ui failed for {user_id}: {e}")


# ─── YEDEKLEME ──────────────────────────────────────────────────────────────
# Tüm işletme verisi TEK bir sqlite dosyasında duruyor: vardiyalar, ödemeler,
# cezalar, kasa raporları. O dosya giderse geri dönüş yok. Bu yüzden bot her
# gün veritabanının tutarlı bir kopyasını owner'a Telegram'dan DOSYA olarak
# gönderir — kopya owner'ın telefonunda, sunucudan bağımsız durur.
BACKUP_HOUR = int(os.getenv("BACKUP_HOUR", "5") or 5)   # 03:00 kapanışından sonra
TG_DOC_LIMIT = 45 * 1024 * 1024                          # Telegram sınırı 50MB, pay bırak


def _db_counts(db):
    """Yedeğin BOŞ olmadığını gözle doğrulamak için özet."""
    out = {}
    for t in ("users", "shifts", "payments", "cashreports", "fines", "tips"):
        try:
            out[t] = db.execute(f"SELECT COUNT(*) AS c FROM {t}").fetchone()["c"] or 0
        except Exception:
            out[t] = 0
    return out


def make_backup_file():
    """TUTARLI kopya üret ve yolunu döndür.

    Dosyayı kopyalamak YETMEZ: tam o anda bir yazma sürüyorsa yarım/bozuk bir
    kopya çıkar ve bunu ancak geri yüklerken anlarsın. sqlite'ın kendi online
    backup API'si açık bağlantıyla tutarlı bir anlık görüntü alır.
    """
    stamp = datetime.now(TZ).strftime("%Y-%m-%d-%H%M")
    path = os.path.join(tempfile.gettempdir(), f"caffelito-{stamp}.db")
    src = sqlite3.connect(DB_PATH)
    try:
        dst = sqlite3.connect(path)
        try:
            src.backup(dst)          # ← tutarlı anlık görüntü
        finally:
            dst.close()
    finally:
        src.close()
    return path


async def send_backup(bot, reason="ежедневная"):
    """Yedeği TÜM owner'lara DM ile gönderir. Gönderilen kişi sayısını döndürür.
    Gruba ASLA gönderilmez — içinde herkesin maaşı var."""
    db = get_db()
    owners = db.execute("SELECT user_id FROM users WHERE role='owner'").fetchall()
    if not owners:
        logger.warning("yedek: owner yok, gonderilmedi")
        return 0
    path = make_backup_file()
    try:
        size = os.path.getsize(path)
        if size > TG_DOC_LIMIT:
            for o in owners:
                try:
                    await bot.send_message(
                        o["user_id"],
                        f"⚠️ Резервная копия слишком большая ({size // 1024 // 1024} МБ) "
                        "— Telegram не примет файл. Нужен другой способ хранения.")
                except Exception:
                    pass
            logger.error(f"yedek cok buyuk: {size} bayt")
            return 0
        c = _db_counts(db)
        cap = (f"🗄 *Резервная копия* · {reason}\n"
               f"{datetime.now(TZ).strftime('%d.%m.%Y %H:%M')} · {max(1, size // 1024)} КБ\n\n"
               f"Сотрудников: {c['users']} · Смен: {c['shifts']}\n"
               f"Выплат: {c['payments']} · Касс: {c['cashreports']}\n\n"
               "_Сохраните этот файл. По нему восстанавливается вся база._")
        sent = 0
        for o in owners:
            try:
                # Her alıcı için dosyayı YENİDEN aç — tüketilmiş dosya nesnesi
                # ikinci gönderimde boş gider.
                with open(path, "rb") as fh:
                    await bot.send_document(
                        chat_id=o["user_id"], document=fh,
                        filename=os.path.basename(path),
                        caption=cap, parse_mode="Markdown")
                sent += 1
            except Exception as e:
                logger.warning(f"yedek DM {o['user_id']}: {e}")
        if sent:
            db.execute("INSERT OR REPLACE INTO meta (k,val) VALUES ('last_backup_at',?)",
                       (datetime.now(TZ).isoformat(),))
            db.commit()
        logger.info(f"yedek gonderildi: {sent} owner, {size} bayt")
        return sent
    finally:
        try:
            os.remove(path)
        except Exception:
            pass


async def backup_loop(app):
    """Her gün BACKUP_HOUR'da bir kez yedek gönderir (meta ile takip: gün içinde tekrar yok)."""
    await asyncio.sleep(40)
    while True:
        try:
            now = datetime.now(TZ)
            if now.hour == BACKUP_HOUR:
                db = get_db()
                today = now.strftime("%Y-%m-%d")
                row = db.execute("SELECT val FROM meta WHERE k='last_backup_day'").fetchone()
                if not row or row["val"] != today:
                    n = await send_backup(app.bot, "ежедневная")
                    if n:
                        db.execute("INSERT OR REPLACE INTO meta (k,val) VALUES ('last_backup_day',?)",
                                   (today,))
                        db.commit()
        except Exception as e:
            logger.warning(f"backup_loop: {e}")
        await asyncio.sleep(1800)   # yarım saatte bir kontrol


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
                _dv = "━━━━━━━━━━━━━━━━━━━━"
                header = (f"<b>📦 ЗАКАЗ — CAFFELITO</b> ⏰\n{_dv}\n"
                          f"👤 <b>{_html.escape(str(shown))}</b>   ·   {sent_now.strftime('%d.%m.%Y  %H:%M')}\n{_dv}\n")
                footer = f"\n{_dv}\n<b>Итого: {total} позиций</b>"
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
    asyncio.create_task(backup_loop(app))


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


# ═══ İMZALI WEB JETONU (initData yedeği) ═══════════════════════════════════════
# Telegram bu bot için hiçbir açılışta `tgWebAppData` göndermiyor (iOS + tdesktop'ta
# doğrulandı: hasData=NO, keys=...tgWebAppBotInline...) → initData boş → uygulama demo
# veriye düşüyordu. Çözüm: kimliği adrese imzalı jetonla gömüyoruz.
#   format : {uid}.{exp}.{sig}   ·  sig = HMAC_SHA256(BOT_TOKEN, "{uid}.{exp}")[:32]
#   taşıma : URL fragment (#t=...) → sunucuya gitmez, uygulama okuyup POST eder.
# NOT: jeton, süresi dolana dek o kullanıcı adına tam yetki verir; bu yüzden kısa TTL
# (24s) + sabit-zamanlı karşılaştırma + hatada fail-closed.
WEB_TOKEN_TTL = 86400  # 24 saat


def make_web_token(uid, ttl=WEB_TOKEN_TTL):
    """İmzalı jeton üret: '{uid}.{exp}.{sig}'."""
    exp = int(datetime.now(TZ).timestamp()) + int(ttl)
    body = f"{int(uid)}.{exp}"
    sig = hmac.new(BOT_TOKEN.encode(), body.encode(), hashlib.sha256).hexdigest()[:32]
    return f"{body}.{sig}"


def verify_web_token(tok):
    """Jetonu doğrula (imza + süre). Geçerliyse uid (int), değilse None."""
    try:
        if not tok:
            return None
        parts = str(tok).strip().split(".")
        if len(parts) != 3:
            return None
        uid = int(parts[0])
        exp = int(parts[1])
        body = f"{uid}.{exp}"
        calc = hmac.new(BOT_TOKEN.encode(), body.encode(), hashlib.sha256).hexdigest()[:32]
        if not hmac.compare_digest(calc, parts[2]):
            return None
        if int(datetime.now(TZ).timestamp()) > exp:
            return None  # süresi dolmuş → yeni açılışta taze jeton gelir
        return uid
    except Exception as e:
        logger.warning(f"web token validation error: {e}")
        return None


def web_auth_user(body, db=None):
    """API ucu için kimlik: önce initData (Telegram imzası), olmazsa imzalı jeton.
    Dönen dict rest of the code'un beklediği şekilde {'id','first_name','username'}."""
    u = validate_init_data((body or {}).get("initData", ""))
    if u and u.get("id"):
        return u
    uid = verify_web_token((body or {}).get("token"))
    if not uid:
        return None
    fn, un = "Бариста", None
    try:
        _d = db or get_db()
        r = _d.execute("SELECT name, username, display_name FROM users WHERE user_id=?", (uid,)).fetchone()
        if r:
            fn = (r["display_name"] or r["name"] or "Бариста")
            un = r["username"]
    except Exception:
        pass
    return {"id": uid, "first_name": fn, "username": un}


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
        # reply_text → kullanıcının özel sohbetine GERÇEK mesaj (no-op DEĞİL).
        kwargs.pop("reply_to_message_id", None)
        kwargs.pop("quote", None)
        kwargs.pop("do_quote", None)
        try:
            return await self._bot.send_message(chat_id=self._chat_id, text=text, **kwargs)
        except Exception as e:
            # Onay mesajları sessizce kaybolmasın: tam traceback + parse_mode'suz yeniden dene
            # (çoğu hata bozuk Markdown/HTML'den gelir; metin yine de kullanıcıya ulaşsın).
            logger.exception(f"shim reply_text failed (chat_id={self._chat_id}): {e}")
            if kwargs.get("parse_mode"):
                try:
                    kwargs.pop("parse_mode", None)
                    return await self._bot.send_message(chat_id=self._chat_id, text=text, **kwargs)
                except Exception as e2:
                    logger.exception(f"shim reply_text retry (no parse_mode) failed: {e2}")
            return None


class _ShimUpdate:
    def __init__(self, bot, uid, first_name, username, data):
        self.effective_user = _ShimUser(uid, first_name, username)
        self.effective_chat = _ShimChat(uid)
        self.message = _ShimMessage(bot, uid, data)
        self.effective_message = self.message


class _ShimContext:
    def __init__(self, bot, bot_data, uid=None):
        self.bot = bot            # GERÇEK Application.bot (grup send_message buradan gider)
        self.bot_data = bot_data  # GERÇEK bot_data (group_id / pending_report paylaşılır)
        # Bazı yollar context.user_data'ya dokunur; yoksa AttributeError tüm eylemi düşürür.
        # Kullanıcı başına kalıcı olsun diye bot_data içinde tutulur (istekler arası korunur).
        try:
            self.user_data = bot_data.setdefault("_shim_user_data", {}).setdefault(int(uid or 0), {})
        except Exception:
            self.user_data = {}
        self.chat_data = {}


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
    """Kök adres → GÜNCEL NERO (`nero/index.html`).

    Eskiden burası eski uygulamayı (kökteki index.html) sunuyordu ve Nero ayrı bir
    sürüm klasöründeydi. Eski uygulamaya dönülmeyeceği için kök artık Nero'yu
    sunar; eski uygulama `legacy.html` olarak duruyor (flags.json «kill» yolu ve
    olası acil dönüş için silinmedi)."""
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "nero", "index.html")
    if not os.path.isfile(path):
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "index.html")
    try:
        with open(path, "r", encoding="utf-8") as f:
            html = f.read()
        return _nocache(_cors(web.Response(text=html, content_type="text/html")))
    except Exception as e:
        return web.Response(text=f"index.html bulunamadı: {e}", status=500)


async def web_legacy(request):
    """Eski uygulama — SADECE acil dönüş yolu (flags.json «kill»/legacyUrl).
    Kök adres artık Nero'yu sunduğu için eski uygulamanın kendi adresi bu."""
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "index.html")
    if not os.path.isfile(path):
        return _cors(web.Response(text="404: legacy index.html yok", status=404))
    return _nocache(_cors(web.FileResponse(path)))


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


async def web_nero(request):
    """Nero sürümlerini /nero/<...> altından servis eder (flags.json dahil).
    Dizin dışına çıkma engelli — /nero/../bot.py ile kaynak indirilemez."""
    rel = request.match_info.get("path", "")
    base = os.path.join(os.path.dirname(os.path.abspath(__file__)), "nero")
    full = os.path.normpath(os.path.join(base, rel))
    if not full.startswith(base + os.sep):
        return _cors(web.Response(text="403: Forbidden", status=403))
    # Sondaki «/» ya da klasör adı verilmişse içindeki index.html'i ver
    # («/nero/2026.08.01-16/» de «/nero/2026.08.01-16» de çalışsın — 404 tuzağı bitsin).
    if os.path.isdir(full):
        full = os.path.join(full, "index.html")
    if not os.path.isfile(full):
        return _cors(web.Response(text="404: Not Found", status=404))
    return _nocache(_cors(web.FileResponse(full)))


async def web_app_current(request):
    """SABİT ADRES: /app → HER ZAMAN `nero/index.html` (güncel Nero).

    BotFather'a bir kez «/app» yazılır ve bir daha dokunulmaz. Sürüm klasörü ve
    her değişiklikte env güncelleme dönemi bitti: yeni sürüm = `nero/index.html`'i
    değiştir + push. Geri alma = git'te o dosyayı geri al."""
    full = os.path.join(os.path.dirname(os.path.abspath(__file__)), "nero", "index.html")
    if not os.path.isfile(full):
        return _cors(web.Response(text="404: nero/index.html yok", status=404))
    return _nocache(_cors(web.FileResponse(full)))


async def web_ver(request):
    """Güncel build sürümünü döndür — client cache'li eskiyse kendini yeniler."""
    return _nocache(_cors(web.Response(text=_app_build(), content_type="text/plain")))


async def web_options(request):
    return _cors(web.Response(text=""))


async def _read_json(request):
    """Gövdeyi içerik tipinden VE biçiminden bağımsız oku (text/plain olabilir →
    CORS preflight'ı atlamak için). İki biçim desteklenir:
      1) JSON              : {"initData":"...","data":"..."}
      2) form/düz metin    : token=<uid>.<exp>.<sig>&data=...   (Content-Type: text/plain)
    İkincisi olmadan jetonlu istemci «bad json» ile reddediliyordu."""
    try:
        raw = await request.text()
        if not raw:
            return {}
        s = raw.strip()
        try:
            v = json.loads(s)
            # JSON ama sözlük değilse (ör. düz string) → aşağıdaki key=value yolunu dene
            if isinstance(v, dict):
                return v
        except Exception:
            pass
        # key=value[&key=value...] gövdesi (token=..., data=..., initData=..., period=...)
        if "=" in s:
            pairs = dict(parse_qsl(s, keep_blank_values=True))
            if pairs:
                return pairs
        return None
    except Exception:
        return None


async def api_state(request):
    """POST {initData} → kullanıcının state'ini hash-payload formatında döner.
    İstemci bunu location.hash'e yazar; mevcut JS değişmeden okur."""
    body = await _read_json(request)
    if body is None:
        return _cors(web.json_response({"error": "bad json"}, status=400))
    db = get_db()
    # Kimlik: initData (Telegram imzası) → yoksa imzalı jeton (#t=, initData yedeği).
    user = web_auth_user(body, db)
    if not user:
        return _cors(web.json_response({"error": "unauthorized"}, status=403))
    # ERİŞİM KAPISI: imza geçerli olsa bile kayıtlı/onaylı değilse VERİ YOK.
    # 403 yerine minik bir «locked» payload dönüyoruz ki uygulama bozuk görünmesin,
    # kişi net bir «Доступ ограничен» ekranı görsün. İçinde şube/katalog/personel
    # gibi HİÇBİR işletme verisi yok.
    if not nero_access_ok(db, user["id"]):
        from urllib.parse import quote as _q
        logger.info(f"NERO erisim reddedildi uid={user['id']} ({user.get('first_name','')})")
        return _nocache(_cors(web.Response(
            # `arch`: owner bu kişiyi REDDETTİ (arşiv). Uygulama o zaman «istek
            # gönder» düğmesini hiç göstermez — istek zaten reddedilirdi.
            text=(f"uid={user['id']}&locked=1&name={_q(user.get('first_name','') or '')}"
                  f"&arch={1 if _nero_archived(db, user['id']) else 0}"),
            content_type="text/plain")))
    # CİHAZ KAPISI: kişi yetkili olsa bile onaylanmamış bir telefondan/tabletten
    # veri verilmez. Kişinin ilk cihazı otomatik güvenilir (kimse kilitlenmesin);
    # sonraki cihazlar owner onayı bekler. Cihaz kimliği göndermeyen istemci
    # eski davranışı görür.
    _dev = str(body.get("device") or "")[:64]
    _dst = device_gate(db, user["id"], _dev,
                       platform=str(body.get("dev_platform") or "")[:32],
                       label=str(body.get("dev_label") or "")[:64])
    if _dst in ("new", "pending", "revoked"):
        from urllib.parse import quote as _q
        if _dst == "new":
            # Owner'lara HABER VER — yoksa çalışan kapıda kalır, kimsenin haberi olmaz.
            try:
                _tg = request.app.get("tg_app")
                _nm = display_name_for(db, user["id"], fallback=user.get("first_name", "?"))
                _pl = str(body.get("dev_platform") or "?")
                for _o in db.execute("SELECT user_id FROM users WHERE role='owner'").fetchall():
                    await _tg.bot.send_message(
                        _o["user_id"],
                        f"📱 *Новое устройство* — {md_safe(_nm)}\n"
                        f"Платформа: {md_safe(_pl)} · код `{_dev[:8]}`\n"
                        "Вход заблокирован до подтверждения.\n"
                        "Управление → Устройства",
                        parse_mode="Markdown")
            except Exception as e:
                logger.warning(f"device notify failed: {e}")
        logger.info(f"NERO cihaz reddedildi uid={user['id']} dev={_dev[:8]} durum={_dst}")
        return _nocache(_cors(web.Response(
            text=(f"uid={user['id']}&locked=1&lock_reason=device&lock_dev={_q(_dev[:8])}"
                  f"&name={_q(user.get('first_name','') or '')}"),
            content_type="text/plain")))
    try:
        # Opsiyonel «period» (YYYY-MM): owner geçmiş ay maaşlarını görüntülerken gelir.
        payload = build_hash_payload(db, user["id"], user.get("first_name", "Бариста"),
                                     sel_period=(body.get("period") or None))
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
    """POST {initData|token, data} → sendData ile aynı işi yapar (sipariş/vardiya vb.).
    Kimlik: initData imzası VEYA imzalı jeton (#t=); sonra handle_webapp_data
    değiştirilmeden çağrılır. Jeton kabulü olmadan uygulamadaki butonlar çalışmaz."""
    body = await _read_json(request)
    if body is None:
        return _cors(web.json_response({"error": "bad json"}, status=400))
    user = web_auth_user(body)
    if not user:
        return _cors(web.json_response({"error": "unauthorized"}, status=403))
    data_str = body.get("data")
    if not data_str:
        return _cors(web.json_response({"error": "no data"}, status=400))
    tg_app = request.app["tg_app"]
    shim_update = _ShimUpdate(
        tg_app.bot, user["id"], user.get("first_name", "Бариста"),
        user.get("username"), data_str)
    shim_context = _ShimContext(tg_app.bot, tg_app.bot_data, uid=user["id"])
    try:
        await handle_webapp_data(shim_update, shim_context)
    except Exception as e:
        # TAM traceback — «eylem işlendi ama grup mesajı gitmedi» gibi sessiz hataların
        # gerçek istisnası görünsün. İstemci sessizce başarılı sanmasın diye 500.
        logger.exception(f"api_action handle_webapp_data FAILED uid={user.get('id')} data={str(data_str)[:200]}")
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
        web.get("/legacy", web_legacy),
        web.get("/index.html", web_index),
        web.get("/health", web_health),
        web.get("/app", web_app_current),
        web.get("/nero/{path:.+}", web_nero),
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
