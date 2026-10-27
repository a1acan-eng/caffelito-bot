# Maaş günü otomatik PDF (owner 2026-10-27): 10'u, 20'si, 30'u — owner'lara bir kez.
# Çalıştırma: rm -f /tmp/pd.db; PYTHONPATH=. DB_PATH=/tmp/pd.db BOT_TOKEN=123:abc python tests/payday_test.py
import json, asyncio, bot
from datetime import datetime
FAIL = []
def check(c, m):
    print(('  ok  ' if c else '  FAIL ') + m)
    if not c: FAIL.append(m)
DOCS = []
class FB:
    async def send_message(self, *a, **k): return None
    async def send_document(self, chat_id=None, document=None, filename=None, caption=None, **k):
        DOCS.append((chat_id, filename)); return None
    def __getattr__(self, n):
        async def f(*a, **k): return None
        return f
def act(uid, p):
    ctx = bot._ShimContext(FB(), {}, uid=uid)
    asyncio.run(bot.handle_webapp_data(bot._ShimUpdate(FB(), uid, f'U{uid}', None, json.dumps(p)), ctx))
    return ctx.result or {}
db = bot.get_db()
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized,branch_id) VALUES (1,'Owner','owner',1,1,1)")
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (10,'Бек','barista',1,1,1)")
db.commit()
cfg = bot.payday_cfg(db)
T = lambda *a: datetime(*a, tzinfo=bot.TZ)
check(cfg == {"on": 1, "at": "12:00"}, 'varsayılan: açık, 12:00')
check(bot.payday_due(T(2026, 10, 10, 12, 5), cfg) == ('2026-10', 1), '10\'u 12:05 → 1–10')
check(bot.payday_due(T(2026, 10, 10, 11, 59), cfg) is None, '10\'u 11:59 → henüz değil')
check(bot.payday_due(T(2026, 10, 20, 18, 0), cfg) == ('2026-10', 2), '20\'si → 11–20')
check(bot.payday_due(T(2026, 10, 30, 12, 0), cfg) == ('2026-10', 3), '30 Ekim → 21–31')
check(bot.payday_due(T(2026, 10, 31, 12, 0), cfg) is None, '31 Ekim → tekrar yok')
check(bot.payday_due(T(2027, 2, 28, 12, 0), cfg) == ('2027-02', 3), '28 Şubat (son gün) → 21–28')
check(bot.payday_due(T(2026, 10, 15, 12, 0), cfg) is None, '15\'i → maaş günü değil')
tag = asyncio.run(bot.payday_run(FB(), db, T(2026, 10, 20, 12, 30)))
check(tag == '2026-10-2' and DOCS == [(1, 'Nero_zarplata_vse_2026-10_11-20.pdf')], 'owner\'a 11–20 PDF\'i gitti (barista\'ya değil)')
asyncio.run(bot.payday_run(FB(), db, T(2026, 10, 20, 15, 0)))
check(len(DOCS) == 1, 'aynı dönem ikinci kez gönderilmez')
check(act(10, {'action': 'payday_auto_set', 'on': 0}).get('ok') is False, 'barista ayarı değiştiremez')
check(act(1, {'action': 'payday_auto_set', 'at': '25:00'}).get('ok') is False, 'geçersiz saat reddedilir')
r = act(1, {'action': 'payday_auto_set', 'on': 0, 'at': '09:30'})
check(r.get('ok') and bot.payday_cfg(db) == {"on": 0, "at": "09:30"}, 'owner kapattı, saat 09:30')
check(bot.payday_due(T(2026, 10, 30, 12, 0), bot.payday_cfg(db)) is None, 'kapalıyken gönderilmez')
print('ALL OK' if not FAIL else 'FAILED: %s' % FAIL)
raise SystemExit(1 if FAIL else 0)
