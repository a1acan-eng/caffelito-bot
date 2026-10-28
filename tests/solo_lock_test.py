# «Один за двоих» KİLİDİ (owner 2026-10-28): o gün şubede ikinci barista vardiya
# açtıktan sonra mod dokunulmaz — barista da owner da açamaz/kapatamaz; ertesi gün serbest.
# Çalıştırma: rm -f /tmp/sl.db; PYTHONPATH=. DB_PATH=/tmp/sl.db BOT_TOKEN=123:abc python tests/solo_lock_test.py
import json, asyncio, bot
from datetime import datetime, timedelta
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
db = bot.get_db()
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized) VALUES (1,'Owner','owner',1,1)")
for uid, nm in ((10, 'Дамир'), (11, 'Хасан')):
    db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (?,?,'barista',1,1,1)", (uid, nm))
db.execute("INSERT OR REPLACE INTO branches (id, name, active) VALUES (1,'C5',1)")
db.commit()
now = datetime.now(bot.TZ).replace(tzinfo=None)
hm = lambda d: d.strftime('%H:%M')
bot.op_cfg_save(db, {"handover_flow": 1, "windows": {"1": [
    {"s": hm(now - timedelta(minutes=30)), "e": hm(now + timedelta(hours=9))},
    {"s": hm(now + timedelta(hours=8)), "e": hm(now + timedelta(hours=17))}]}})
db.commit()
act(10, {'action': 'shift_start', 'branch_id': 1})
check(not bot.ho_solo_locked(db, 1), 'tek barista → kilit yok')
act(10, {'action': 'ho_solo', 'branch_id': 1, 'on': 1})
check(bot.ho_solo(db, 1), 'tek barista modu açabildi')
act(10, {'action': 'ho_solo', 'branch_id': 1, 'on': 0})
check(not bot.ho_solo(db, 1), 'tek barista modu kapatabildi')
act(11, {'action': 'shift_start', 'branch_id': 1})
check(bot.ho_solo_locked(db, 1), 'ikinci barista açınca kilitli')
act(1, {'action': 'ho_solo', 'branch_id': 1, 'on': 1})
check(not bot.ho_solo(db, 1), 'owner da açamıyor')
act(11, {'action': 'shift_end', 'drinks': {}, 'light': 1})
act(10, {'action': 'ho_solo', 'branch_id': 1, 'on': 1})
check(not bot.ho_solo(db, 1), 'ikinci kapatıp gitse de bugün kilitli kalır')
check(not bot.ho_solo_can(db, 10, 1), 'solo_can kapalı')
# Kilit açıkken mod açık kalmışsa (eski veri) owner kapatamaz da
bot.ho_solo_set(db, 1, True)
act(1, {'action': 'ho_solo', 'branch_id': 1, 'on': 0})
check(bot.ho_solo(db, 1), 'kilitliyken kapatma da yok (dokunulmaz)')
bot.ho_solo_set(db, 1, False)
check(not bot.ho_solo_locked(db, 1, now + timedelta(days=1)), 'ertesi gün kilit yok')
print('ALL OK' if not FAIL else 'FAILED: %s' % FAIL)
raise SystemExit(1 if FAIL else 0)
