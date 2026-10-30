# Kasa raporu düzeltilince gruptaki «СМЕННЫЙ ОТЧЁТ» mesajı da düzelir (owner 2026-10-30:
# «Капучинатор'u sonradan ekledim, gruptaki mesaj değişmedi»).
# Çalıştırma: rm -f /tmp/cg.db; PYTHONPATH=. DB_PATH=/tmp/cg.db BOT_TOKEN=123:abc python tests/cash_report_gsync_test.py
import json, asyncio, bot
from datetime import datetime
from types import SimpleNamespace as NS
FAIL = []
def check(c, m):
    print(('  ok  ' if c else '  FAIL ') + m)
    if not c: FAIL.append(m)
EDITS = []
class FB:
    async def send_message(self, chat_id=None, text=None, *a, **k): return None
    async def edit_message_text(self, chat_id=None, message_id=None, text=None, **k):
        EDITS.append((chat_id, message_id, text)); return None
    def __getattr__(self, n):
        async def f(*a, **k): return None
        return f
def act(uid, p):
    ctx = bot._ShimContext(FB(), {}, uid=uid)
    asyncio.run(bot.handle_webapp_data(bot._ShimUpdate(FB(), uid, f'U{uid}', None, json.dumps(p)), ctx))
db = bot.get_db()
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized) VALUES (1,'Owner','owner',1,1)")
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (10,'Мираглам','barista',1,1,1)")
db.execute("INSERT OR REPLACE INTO branches (id, name, active) VALUES (1,'C5',1)")
at = datetime.now(bot.TZ).replace(second=0, microsecond=0)
exps = [{"n": "Лимон+сок+обед", "a": 165000}, {"n": "Свои · Мираглам", "a": 26000}]
rid = db.execute(
    "INSERT INTO cashreports (user_id,user_name,date,period,created_at,bylo,restock,ostalos,sold,cups_total,"
    "vyshlo,na_sdachi,kassa,expenses,expenses_total,note,daily_pay,branch_id) VALUES (10,'Мираглам',?,?,?,?,?,?,?,?,0,0,-73400,?,?,'',73400,1)",
    (at.strftime('%Y-%m-%d'), at.strftime('%Y-%m'), at.isoformat(),
     json.dumps({"Купол 400": 47, "Купол 300": 47}), json.dumps({}), json.dumps({"Купол 400": 39, "Купол 300": 46}),
     json.dumps({"Купол 400": 8, "Купол 300": 1}), 9, json.dumps(exps, ensure_ascii=False), 191000)).lastrowid
db.commit()
t0 = bot.cash_report_group_text(db, db.execute("SELECT * FROM cashreports WHERE id=?", (rid,)).fetchone())
check('Лимон+сок+обед: 165.000' in t0 and 'Купол 400:    47 → 39 = 8' in t0 and 'Дневной бонус: 73.400' in t0,
      'rapor metni kayıttan kuruluyor')
# eski (bağlanmamış) grup mesajı: başlıktaki saatle bulunur
db.execute("INSERT INTO sent_msgs (chat_id,message_id,text,ents,kb,created_at,orig_text) VALUES (-100,555,?,?,?,?,?)",
           (bot.re.sub(r'<[^>]+>', '', t0), '[]', '', at.isoformat(), ''))
db.commit()
act(1, {'action': 'cash_report_edit', 'id': rid,
        'expenses': exps + [{"n": "Капучинатор", "a": 92000}]})
check(len(EDITS) == 1 and EDITS[0][:2] == (-100, 555), 'gruptaki AYNI mesaj düzenlendi (yeni mesaj yok)')
check(EDITS and 'Капучинатор: 92.000' in EDITS[0][2] and 'Итого расходы: 283.000' in EDITS[0][2], 'yeni masraf ve toplam mesajda')
check(EDITS and 'исправлено владельцем' in EDITS[0][2], '«исправлено владельцем» notu')
r = db.execute("SELECT kind, shift_id, text FROM sent_msgs WHERE message_id=555").fetchone()
check(r["kind"] == 'cash_report' and r["shift_id"] == rid and 'Капучинатор' in r["text"], 'mesaj rapora bağlandı, metin güncel')
# ikinci düzeltme: bağlı mesaj üzerinden
act(1, {'action': 'cash_report_edit', 'id': rid, 'note': 'проверено'})
check(len(EDITS) == 2 and '📝 проверено' in EDITS[1][2], 'ikinci düzeltme de mesaja yansıdı')
print('ALL OK' if not FAIL else 'FAILED: %s' % FAIL)
raise SystemExit(1 if FAIL else 0)
