# Утренняя сводка + напоминание о смене (owner 2026-11-12).
# Çalıştırma: rm -f /tmp/as.db; PYTHONPATH=. DB_PATH=/tmp/as.db BOT_TOKEN=123:abc python tests/assist_test.py
import json, asyncio, bot
from datetime import datetime, timedelta
FAIL = []
def check(c, m):
    print(('  ok  ' if c else '  FAIL ') + m)
    if not c: FAIL.append(m)
sent = []
class FB:
    async def send_message(self, chat_id=None, text=None, *a, **k): sent.append((chat_id, text))
    def __getattr__(self, n):
        async def f(*a, **k): return None
        return f
def act(uid, p):
    ctx = bot._ShimContext(FB(), {}, uid=uid)
    asyncio.run(bot.handle_webapp_data(bot._ShimUpdate(FB(), uid, f'U{uid}', None, json.dumps(p)), ctx))
    return getattr(ctx, 'result', None)
bot.get_db()
db = bot.get_db()
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized,branch_id) VALUES (1,'Owner','owner',1,1,1)")
for uid, nm in ((10, 'Damir'), (11, 'Ubay'), (12, 'Karina')):
    db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (?,?,'barista',1,1,1)", (uid, nm))
db.execute("INSERT OR REPLACE INTO branches (id,name,active,group_chat_id) VALUES (1,'C5',1,'-1')")
db.execute("INSERT OR REPLACE INTO branches (id,name,active,group_chat_id) VALUES (2,'Magic',1,'-2')")
for c, b, s, e in (('c5d', 1, '07:00', '17:30'), ('c5n', 1, '16:30', '03:00')):
    db.execute("INSERT OR REPLACE INTO shift_templates (code, branch_id, start_t, end_t, active) VALUES (?,?,?,?,1)", (c, b, s, e))
NOW = datetime(2026, 11, 12, 9, 5)
wk = (NOW.date() - timedelta(days=NOW.weekday())).isoformat()
for uid, code in ((10, 'c5d'), (11, 'c5n'), (12, 'off')):
    db.execute("INSERT OR REPLACE INTO shift_grid (week_key, day, user_id, code) VALUES (?,?,?,?)", (wk, NOW.weekday(), uid, code))
# dün: Ubay'ın gece vardiyası (11'i 16:30 → 12'si 03:00) + kasa raporu; Damir'in gündüzü raporsuz
db.execute("INSERT INTO shifts (user_id,hours,total,date,period,created_at,start_time,end_time,branch_id) VALUES (11,10.5,0,'2026-11-11','2026-11','x','2026-11-11T16:30:00','2026-11-12T03:00:00',1)")
db.execute("INSERT INTO shifts (user_id,hours,total,date,period,created_at,start_time,end_time,branch_id) VALUES (10,10,0,'2026-11-11','2026-11','x','2026-11-11T07:10:00','2026-11-11T17:30:00',1)")
db.execute("INSERT INTO cashreports (user_id,user_name,date,period,created_at,cups_total,expenses_total,branch_id) VALUES (11,'Ubay','2026-11-12','2026-11','2026-11-12T03:05:00',140,50000,1)")
db.execute("INSERT INTO opening_delays (user_id,user_name,branch_id,date,delay_min,status,created_at) VALUES (10,'Damir',1,'2026-11-11',10,'pending','x')")
db.execute("INSERT INTO fines (user_id,amount,reason,period,created_at) VALUES (10,20000,'x','2026-11','2026-11-11T18:00:00')")
db.commit()

t = bot.digest_text(db, NOW)
print(t)
check('Вчера · 11.11' in t and 'смен 2 · 20.5 ч' in t and 'продано 140' in t, 'dün: vardiya, saat, bardak (gece vardiyası düne sayılır)')
check('кассовых отчётов 1 из 2' in t, 'eksik kasa raporu uyarısı')
check('Damir +10 мин' in t and 'Штрафы: 1 · 20' in t, 'geç kalma ve ceza')
check('Damir 07:00–17:30' in t and 'Ubay 16:30–03:00' in t, 'bugünkü plan')
check('Magic</b>: ⚠️ в графике никого' in t, 'planı boş şube uyarısı')
check('Выходной: Karina' in t, 'izinliler')

# günde bir kez, saatten önce gitmez
check(asyncio.run(bot.digest_run(FB(), db, datetime(2026, 11, 12, 8, 59))) is None and not sent, 'saat gelmeden gitmez')
check(asyncio.run(bot.digest_run(FB(), db, NOW)) == '2026-11-12' and [c for c, _ in sent] == [1], 'owner\'a gitti')
check(asyncio.run(bot.digest_run(FB(), db, NOW + timedelta(hours=3))) is None and len(sent) == 1, 'aynı gün ikinci kez gitmez')
check(asyncio.run(bot.digest_run(FB(), db, NOW + timedelta(days=1))) == '2026-11-13', 'ertesi gün yine gider')

# hatırlatma: 16:30 vardiyası, 60 dk önce
sent.clear()
check(asyncio.run(bot.shift_remind_run(FB(), db, datetime(2026, 11, 12, 15, 20))) == [], '70 dk kala erken')
r = asyncio.run(bot.shift_remind_run(FB(), db, datetime(2026, 11, 12, 15, 31)))
check(r == [(11, 'c5n')] and sent and sent[0][0] == 11 and 'C5 · 16:30–03:00' in sent[0][1], f'59 dk kala çalışana gitti ({r})')
check(asyncio.run(bot.shift_remind_run(FB(), db, datetime(2026, 11, 12, 15, 40))) == [], 'aynı vardiya için ikinci kez gitmez')
check(asyncio.run(bot.shift_remind_run(FB(), db, datetime(2026, 11, 12, 16, 45))) == [], 'vardiya başladıktan sonra gitmez')
# engelli ve vardiyası açık olana gitmez
db.execute("DELETE FROM meta WHERE k='shrem_sent'"); db.commit()
bot.user_suspend_set(db, 11, 1, 1, 'Owner')
db.execute("INSERT OR REPLACE INTO shift_grid (week_key, day, user_id, code) VALUES (?,?,11,'c5n')", (wk, NOW.weekday())); db.commit()
check(asyncio.run(bot.shift_remind_run(FB(), db, datetime(2026, 11, 12, 15, 31))) == [], 'engelliye gitmez')
# ayarlar
r = act(10, {'action': 'assist_set', 'rm_on': 0})
check(r and r['ok'] is False, 'barista ayarı değiştiremez')
r = act(1, {'action': 'assist_set', 'rm_on': 0, 'dg_at': '8:30', 'rm_min': 30})
check(r and r['ok'] and r['cfg'] == {'dg_on': 1, 'dg_at': '08:30', 'rm_on': 0, 'rm_min': 30}, f'owner ayarı ({r})')
check(act(1, {'action': 'assist_set', 'dg_at': '25:00'})['ok'] is False, 'geçersiz saat')
db.execute("DELETE FROM meta WHERE k='shrem_sent'"); db.commit()
check(asyncio.run(bot.shift_remind_run(FB(), db, datetime(2026, 11, 12, 6, 31))) == [], 'kapalıyken hatırlatma gitmez')
sent.clear()
r = act(1, {'action': 'digest_now'})
check(r and r['ok'], '«şimdi gönder»')
print('ALL OK' if not FAIL else 'FAILED: %s' % FAIL)
raise SystemExit(1 if FAIL else 0)
