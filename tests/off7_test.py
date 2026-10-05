# «7 gün üst üste izinsiz» uyarısı (owner 2026-10-04).
# Çalıştırma: rm -f /tmp/off7.db; PYTHONPATH=. DB_PATH=/tmp/off7.db BOT_TOKEN=123:abc python tests/off7_test.py
import asyncio, bot
from datetime import date, datetime, timedelta
FAIL = []
def check(c, m):
    print(('  ok  ' if c else '  FAIL ') + m)
    if not c: FAIL.append(m)

db = bot.get_db()
db.execute("UPDATE branches SET name='C5', group_chat_id='-1001' WHERE id=1")
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized) VALUES (1,'Owner','owner',1,1)")
for uid, nm in ((10, 'Ахмет'), (11, 'Бек'), (12, 'Вали'), (13, 'Гуля'), (14, 'Дилшод')):
    db.execute("INSERT OR REPLACE INTO users (user_id,name,display_name,role,branch_id,approved,authorized) "
               "VALUES (?,?,?,'barista',1,1,1)", (uid, nm, nm))
db.execute("UPDATE users SET archived=1 WHERE user_id=14")
sat = date(2026, 10, 3)                       # Cumartesi
mon = sat - timedelta(days=5); prev = mon - timedelta(days=7)
def put(uid, wk, codes):
    for d, c in enumerate(codes):
        if c: db.execute("INSERT OR REPLACE INTO shift_grid (week_key,day,user_id,code) VALUES (?,?,?,?)", (wk.isoformat(), d, uid, c))
W = 'c5d'
put(10, mon, [W] * 7)                         # 7/7 → uyarı
put(11, mon, [W, W, W, 'off', W, W, W])       # izinli → yok
put(12, prev, [None, None, None, W, W, W, W]); put(12, mon, [W, W, W, None, W, W, W])  # 7 üst üste (geçen hafta+bu) ama bu hafta boş gün → 7 seri var
put(13, mon, [W, W, W, None, W, W, W])        # 3+3, izin yok ama seri <7 → yok
put(14, mon, [W] * 7)                         # arşivli → yok
put(1, mon, [W] * 7)                          # owner → yok
db.commit()
r = {u: k for u, _n, _b, k in bot.off7_scan(db, sat)}
check(10 in r and r[10] == 7, '7/7 çalışan uyarıda (7 gün)')
check(11 not in r, 'izinli olan uyarıda değil')
check(12 in r and r[12] == 7, 'geçen haftadan gelen 7 günlük seri yakalanıyor')
check(13 not in r, 'serisi 7 olmayan uyarıda değil')
check(14 not in r and 1 not in r, 'arşivli ve owner hariç')
put(10, mon, [W, W, W, W, W, W, 'sick']); db.commit()
check(10 not in {u for u, *_ in bot.off7_scan(db, sat)}, 'hasta günü olan uyarıda değil')

# Döngü: cumartesi 19:00 sonrası bir kez gönderir.
sent = []
class FB:
    async def send_message(self, chat_id=None, text=None, *a, **k): sent.append((chat_id, text))
class APP: bot = FB()
class FakeDT(datetime):
    @classmethod
    def now(cls, tz=None): return datetime(2026, 10, 3, 19, 30, tzinfo=tz)
bot.datetime = FakeDT
_real_sleep = asyncio.sleep
async def fast_sleep(s, *a, **k):
    if s >= 90: return await _real_sleep(0)
    return await _real_sleep(s)
async def drive():
    bot.asyncio.sleep = fast_sleep
    t = asyncio.create_task(bot.off7_loop(APP()))
    for _ in range(20): await _real_sleep(0)
    t.cancel()
asyncio.run(drive())
check(len(sent) == 1 and sent[0][0] == 1 and 'Вали' in sent[0][1] and 'Ахмет' not in sent[0][1], 'owner\'a tek mesaj, doğru isimler')
check(db.execute("SELECT val FROM meta WHERE k='off7_sent_2026-09-28'").fetchone() is not None, 'hafta işaretlendi')
sent.clear(); asyncio.run(drive())
check(not sent, 'aynı hafta ikinci kez gönderilmiyor')
bot.asyncio.sleep = _real_sleep
print('FAIL' if FAIL else 'ALL OK', FAIL)
