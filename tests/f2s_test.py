# Штрафы → в зарплату (owner 2026-10-29): owner ceza defterindeki parayı istediği
# zaman, istediği tutarda bu ayki maaşına geçirir; geri alabilir.
# Çalıştırma: rm -f /tmp/fs.db; PYTHONPATH=. DB_PATH=/tmp/fs.db BOT_TOKEN=123:abc python tests/f2s_test.py
import json, asyncio, bot
from datetime import datetime
FAIL = []
def check(c, m):
    print(('  ok  ' if c else '  FAIL ') + m)
    if not c: FAIL.append(m)
class FB:
    async def send_message(self, chat_id=None, text=None, *a, **k): return None
    def __getattr__(self, n):
        async def f(*a, **k): return None
        return f
def act(uid, p):
    ctx = bot._ShimContext(FB(), {}, uid=uid)
    asyncio.run(bot.handle_webapp_data(bot._ShimUpdate(FB(), uid, f'U{uid}', None, json.dumps(p)), ctx))
    return getattr(ctx, 'result', None)
db = bot.get_db()
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized,branch_id) VALUES (1,'Owner','owner',1,1,1)")
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (10,'Дамир','barista',1,1,1)")
db.execute("INSERT OR REPLACE INTO branches (id, name, active) VALUES (1,'C5',1)")
per = bot.current_period(); now = datetime.now(bot.TZ).isoformat()
def fine(uid, amt, typ, by):
    db.execute("INSERT INTO fines (user_id,amount,reason,type,period,added_by,created_at) VALUES (?,?,?,?,?,?,?)",
               (uid, amt, 'т', typ, per, by, now))
fine(10, 50000, 'manual', 1)            # owner kesti
fine(10, 30000, 'opening_delay', 0)     # sistem kesti → birincil owner
fine(10, 1000000, 'insp_70', 1)         # marka cezası → sayılmaz
fine(1, 20000, 'manual', 1)             # owner kendi üstüne → sayılmaz
db.commit()
net0 = bot.calc_summary(db, 1, per)["net"]
emp0 = bot.calc_summary(db, 10, per)["net"]
check(bot.f2s_pool(db, 1) == (80000, 0, 80000), f'havuz 80.000 (marka ve kendi cezası hariç) {bot.f2s_pool(db, 1)}')
r = act(10, {'action': 'f2s_move', 'amount': 1000})
check(r and not r.get('ok'), 'barista geçiremez')
r = act(1, {'action': 'f2s_move', 'amount': 90000})
check(r and not r.get('ok') and '80' in r.get('error', ''), 'havuzdan fazlası reddedildi')
r = act(1, {'action': 'f2s_move', 'amount': 30000})
check(r and r.get('ok') and r['f2s']['left'] == 50000 and r['f2s']['moved'] == 30000, 'kısmi geçiş 30.000, kalan 50.000')
check(bot.calc_summary(db, 1, per)["net"] == net0 + 30000, 'owner maaşı +30.000')
check(bot.calc_summary(db, 10, per)["net"] == emp0, 'çalışan etkilenmez')
r = act(1, {'action': 'f2s_move', 'amount': 50000})
check(r and r.get('ok') and r['f2s']['left'] == 0, 'kalanın hepsi geçti')
aid = r['f2s']['rows'][0]['id']
r = act(1, {'action': 'f2s_undo', 'id': aid})
check(r and r.get('ok') and r['f2s']['left'] == 50000, 'geri alındı → kalan 50.000')
check(bot.calc_summary(db, 1, per)["net"] == net0 + 30000, 'geri alınca maaştan düştü')
check(db.execute("SELECT COUNT(*) AS n FROM logs WHERE action IN ('f2s_move','f2s_undo')").fetchone()["n"] == 3, 'günlükte 3 kayıt')
check(db.execute("SELECT COUNT(*) AS n FROM fines WHERE COALESCE(settled,0)=1").fetchone()["n"] == 0, 'ceza işaretleri değişmedi')
print('ALL OK' if not FAIL else 'FAILED: %s' % FAIL)
raise SystemExit(1 if FAIL else 0)
