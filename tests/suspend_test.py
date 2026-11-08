# Çalışanı askıya al: tüm günler bloke (owner 2026-11-08).
# Çalıştırma: rm -f /tmp/su.db; PYTHONPATH=. DB_PATH=/tmp/su.db BOT_TOKEN=123:abc python tests/suspend_test.py
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
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (10,'Damir','barista',1,1,1)")
db.execute("INSERT OR REPLACE INTO branches (id,name,active,max_staff) VALUES (1,'C5',1,4)")
db.execute("INSERT OR REPLACE INTO shift_templates (code, branch_id, start_t, end_t, active) VALUES ('c5d',1,'07:00','17:30',1)")
db.commit()
today = datetime.now(bot.TZ).replace(tzinfo=None).date()
wk1 = bot.grid_week_key(1)
for d in range(7):
    db.execute("INSERT OR REPLACE INTO shift_grid (week_key, day, user_id, code) VALUES (?,?,10,?)", (wk1, d, 'off' if d == 0 else 'c5d'))
db.commit()
r = act(10, {'action': 'user_suspend', 'target': 10, 'on': 1})
check(r and not r.get('ok') and not bot.user_suspended(db, 10), 'çalışan kendini/başkasını bloke edemez')
r = act(1, {'action': 'user_suspend', 'target': 1, 'on': 1})
check(r and not r.get('ok'), 'owner bloke edilemez')
r = act(1, {'action': 'user_suspend', 'target': 10, 'on': 1})
check(r and r.get('ok') and bot.user_suspended(db, 10) and r['cells'] == 6, f'bloke · gelecek 6 vardiya kaldırıldı ({r})')
check(db.execute("SELECT code FROM shift_grid WHERE week_key=? AND user_id=10", (wk1,)).fetchall()[0]['code'] == 'off', 'OFF günü korunur')
check(any(c == 10 and 'отстранил' in t for c, t in sent), 'çalışana bildirim')
check(not bot.grid_check(db, wk1, 3, 10, 'c5d')[0], 'Grafik\'e vardiya atanamaz')
act(1, {'action': 'shift_grid_set', 'target_uid': 10, 'week': 1, 'day': 3, 'code': 'c5d'})
check(not db.execute("SELECT 1 FROM shift_grid WHERE week_key=? AND day=3 AND user_id=10", (wk1,)).fetchone(), 'owner da atayamaz')
act(10, {'action': 'shift_start', 'branch_id': 1})
check(bot.get_active_shift(db, 10) is None, 'vardiya açamaz')
act(1, {'action': 'force_start_shift', 'target': 10, 'branch_id': 1})
check(bot.get_active_shift(db, 10) is None, 'owner da başlatamaz')
check(bot.cov_user(db, 10)['arch'] == 1, 'yedek aramaya girmez')
pl = bot.build_hash_payload(db, 1, 'Owner')
from urllib.parse import unquote
ro = json.loads(unquote(pl.split('roster=')[1].split('&')[0]))
check(any(x['id'] == 10 and x['sp'] == 1 for x in ro), 'payload: roster sp=1')
r = act(1, {'action': 'user_suspend', 'target': 10, 'on': 0})
check(r and r.get('ok') and not bot.user_suspended(db, 10) and r['cells'] == 6, f'blok kalktı · 6 vardiya geri geldi ({r})')
check(len(db.execute("SELECT 1 FROM shift_grid WHERE week_key=? AND user_id=10 AND code='c5d'", (wk1,)).fetchall()) == 6, 'hücreler geri')
act(10, {'action': 'shift_start', 'branch_id': 1})
check(bot.get_active_shift(db, 10) is not None, 'blok kalkınca vardiya açar')
print('ALL OK' if not FAIL else 'FAILED: %s' % FAIL)
raise SystemExit(1 if FAIL else 0)
