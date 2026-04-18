"""
CAFFELITO TELEGRAM BOT ☕
Заказ, Задачи, Уборка и ОКК контроль
"""

import json, os, logging, sqlite3
from datetime import datetime, timezone, timedelta
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    WebAppInfo, BotCommand, KeyboardButton, ReplyKeyboardMarkup
)
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    ContextTypes, MessageHandler, filters
)

BOT_TOKEN = os.getenv("BOT_TOKEN", "BURAYA_BOT_TOKEN_YAZ")
WEBAPP_URL = os.getenv("WEBAPP_URL", "")
GROUP_CHAT_ID = os.getenv("GROUP_CHAT_ID", "")  # Grup ID — /setgroup komutuyla alınır
TZ = timezone(timedelta(hours=5))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─── DATABASE ───
def get_db():
    db = sqlite3.connect("caffelito.db")
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
        created_at TEXT)""")
    db.execute("""CREATE TABLE IF NOT EXISTS shifts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER, hours REAL, drinks TEXT,
        bonus INTEGER, hourly_pay INTEGER, total INTEGER,
        date TEXT, period TEXT, created_at TEXT)""")
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


def has_owner(db):
    row = db.execute("SELECT COUNT(*) as c FROM users WHERE role='owner'").fetchone()
    return (row["c"] or 0) > 0


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


def calc_bonus(drinks):
    total = 0
    for k, v in (drinks or {}).items():
        total += BONUS_RATES.get(k, 0) * int(v or 0)
    return total


def calc_summary(db, user_id, period=None):
    period = period or current_period()
    shifts = db.execute(
        "SELECT * FROM shifts WHERE user_id=? AND period=? ORDER BY created_at",
        (user_id, period)).fetchall()
    fines = db.execute(
        "SELECT * FROM fines WHERE user_id=? AND period=? ORDER BY created_at",
        (user_id, period)).fetchall()
    paid_row = db.execute(
        "SELECT COALESCE(SUM(amount),0) as s FROM payments WHERE user_id=? AND period=?",
        (user_id, period)).fetchone()

    hours = sum(s["hours"] for s in shifts)
    bonus = sum(s["bonus"] for s in shifts)
    hourly = sum(s["hourly_pay"] for s in shifts)
    fine_total = sum(f["amount"] for f in fines)
    paid_total = paid_row["s"] or 0
    gross = hourly + bonus
    net = gross - fine_total - paid_total

    return {
        "period": period,
        "hours": hours,
        "bonus": bonus,
        "hourly": hourly,
        "fines": fine_total,
        "paid": paid_total,
        "gross": gross,
        "net": net,
        "shifts_count": len(shifts),
        "fines_count": len(fines),
        "shifts": [dict(s) for s in shifts],
        "fines_list": [dict(f) for f in fines],
    }


def build_webapp_url(base_url, user_id, name, db):
    """Build WebApp URL with user, role, summary and (for owner) baristas embedded in hash."""
    from urllib.parse import quote
    upsert_user(db, user_id, name, None, None)
    role = get_role(db, user_id)
    s = calc_summary(db, user_id)
    summary = {
        "hours": s["hours"], "bonus": s["bonus"], "hourly": s["hourly"],
        "fines": s["fines"], "paid": s["paid"], "net": s["net"],
        "period": s["period"], "shifts_count": s["shifts_count"],
        "fines_count": s["fines_count"],
        "shifts": s["shifts"][-30:],
        "fines_list": s["fines_list"][-30:],
    }
    parts = [
        f"uid={user_id}",
        f"role={role}",
        f"name={quote(name or '')}",
        f"summary={quote(json.dumps(summary, ensure_ascii=False))}",
    ]
    if role == "owner":
        rows = db.execute(
            "SELECT user_id, name, username, role FROM users ORDER BY name").fetchall()
        baristas = []
        for b in rows:
            bs = calc_summary(db, b["user_id"])
            baristas.append({
                "id": b["user_id"], "n": b["name"], "u": b["username"] or "",
                "r": b["role"], "h": bs["hours"], "b": bs["bonus"],
                "hp": bs["hourly"], "f": bs["fines"],
                "paid": bs["paid"], "net": bs["net"],
                "sc": bs["shifts_count"], "fc": bs["fines_count"],
            })
        parts.append(f"baristas={quote(json.dumps(baristas, ensure_ascii=False))}")
    return base_url + "#" + "&".join(parts)

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

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = get_db()
    db.execute("INSERT OR IGNORE INTO shops (chat_id) VALUES (?)",
               (update.effective_chat.id,))
    db.commit()
    user = update.effective_user
    upsert_user(db, user.id, user.first_name, user.username, update.effective_chat.id)

    # Kalıcı buton altta — her zaman görünür
    if WEBAPP_URL:
        url = build_webapp_url(WEBAPP_URL, user.id, user.first_name, db)
        reply_kb = ReplyKeyboardMarkup(
            [[KeyboardButton("☕ Открыть Caffelito", web_app=WebAppInfo(url=url))]],
            resize_keyboard=True
        )
    else:
        reply_kb = None

    role_note = ""
    if not has_owner(db):
        role_note = "\n\n👑 Владелец ещё не назначен. Используйте /setowner чтобы стать владельцем."
    elif get_role(db, user.id) == "owner":
        role_note = "\n\n👑 Вы — владелец."

    await update.message.reply_text(
        "☕ *CAFFELITO BOT*\n\n"
        "Нажмите кнопку ниже чтобы открыть приложение 👇" + role_note,
        reply_markup=reply_kb,
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

        elif action == "shift":
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
                "INSERT INTO shifts (user_id, hours, drinks, bonus, hourly_pay, total, date, period, created_at) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (user.id, hours, json.dumps(drinks, ensure_ascii=False),
                 bonus, hourly_pay, total,
                 now.strftime("%Y-%m-%d"), period, now.isoformat()))
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
            db = get_db()
            if get_role(db, user.id) != "owner":
                await update.message.reply_text("❌ Только владелец может выписывать штрафы.")
                return
            target_id = int(data.get("target", 0) or 0)
            amount = int(data.get("amount", 0) or 0)
            reason = (data.get("reason") or "").strip() or "Без причины"
            ftype = data.get("type", "manual")
            if not target_id or amount <= 0:
                await update.message.reply_text("❌ Неверные данные штрафа.")
                return
            target_row = db.execute("SELECT * FROM users WHERE user_id=?", (target_id,)).fetchone()
            if not target_row:
                await update.message.reply_text(f"❌ Бариста не найден: {target_id}")
                return
            period = current_period()
            db.execute(
                "INSERT INTO fines (user_id, amount, reason, type, period, added_by, added_by_name, created_at) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (target_id, amount, reason, ftype, period,
                 user.id, user.first_name, now.isoformat()))
            db.commit()
            await update.message.reply_text(
                f"⚠️ Штраф добавлен\n\n"
                f"Кому: {target_row['name']}\n"
                f"Сумма: -{fmt_sum(amount)} сум\n"
                f"Причина: {reason}")
            try:
                await context.bot.send_message(
                    target_id,
                    f"⚠️ *Вам начислен штраф*\n\n"
                    f"Сумма: *-{fmt_sum(amount)}* сум\n"
                    f"Причина: {reason}\n"
                    f"От: {user.first_name}\n\n"
                    f"Баланс: /zarplata",
                    parse_mode="Markdown")
            except Exception as e:
                logger.warning(f"Notify fine failed: {e}")

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
            await update.message.reply_text(
                f"✅ Выплата записана\n\n"
                f"Кому: {target_row['name']}\n"
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
            await update.message.reply_text(f"✅ {target_row['name']}: роль → {new_role}")
            try:
                if new_role == "owner":
                    msg = f"👑 Вам выдали роль *владельца*!\nОт: {user.first_name}"
                else:
                    msg = f"ℹ️ Ваша роль изменена на *бариста*.\nОт: {user.first_name}"
                await context.bot.send_message(target_id, msg, parse_mode="Markdown")
            except Exception as e:
                logger.warning(f"Notify role failed: {e}")

    except Exception as e:
        logger.error(f"WEBAPP DATA ERROR: {e}")
        try:
            await update.message.reply_text(f"❌ Ошибка: {e}")
        except:
            pass


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
    url = build_webapp_url(webapp_url, user.id, user.first_name, db)
    role = get_role(db, user.id)

    # Kalıcı buton — klavyenin üstünde her zaman görünür
    reply_kb = ReplyKeyboardMarkup(
        [[KeyboardButton("☕ Открыть Caffelito", web_app=WebAppInfo(url=url))]],
        resize_keyboard=True
    )
    role_line = "👑 *Владелец*" if role == "owner" else "👤 *Бариста*"
    await update.message.reply_text(
        f"☕ *CAFFELITO*\n\n"
        f"{role_line}\n\n"
        f"Кнопка приложения обновлена 👇\n"
        f"Нажмите «☕ Открыть Caffelito» внизу экрана",
        reply_markup=reply_kb,
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

def main():
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("zakaz", cmd_order))
    app.add_handler(CommandHandler("zadachi", cmd_gorev))
    app.add_handler(CommandHandler("uborka", cmd_temizlik))
    app.add_handler(CommandHandler("okk", cmd_okk))
    app.add_handler(CommandHandler("otchet", cmd_report))
    app.add_handler(CommandHandler("setgroup", cmd_setgroup))
    app.add_handler(CommandHandler("menu", cmd_menu))
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
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.StatusUpdate.WEB_APP_DATA, handle_webapp_data))
    print("☕ Caffelito Bot запущен!")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
