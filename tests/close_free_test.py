# «Закрывает смену сам» (owner 2026-10-21): izinli kişi plan bitişinden önce kapatır, kilit/ihlal yok.
# Çalıştırma: rm -f /tmp/cf.db; PYTHONPATH=. DB_PATH=/tmp/cf.db BOT_TOKEN=123:abc python tests/close_free_test.py
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
    return ctx.result
db = bot.get_db()
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized) VALUES (1,'Owner','owner',1,1)")
for uid, nm in ((10, 'Убай'), (11, 'Бек')):
    db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (?,?,'barista',1,1,1)", (uid, nm))
db.execute("INSERT OR REPLACE INTO branches (id, name, active) VALUES (1,'C5',1)")
bot.op_cfg_save(db, {"handover_flow": 1})
db.commit()
now = datetime.now(bot.TZ).replace(tzinfo=None)
def open_shift(uid, nm):
    st = now - timedelta(hours=3)
    sid = db.execute("INSERT INTO shifts (user_id,hours,total,date,period,created_at,start_time,branch_id) VALUES (?,0,0,?,?,?,?,1)",
                     (uid, st.strftime('%Y-%m-%d'), st.strftime('%Y-%m'), st.isoformat(), st.isoformat())).lastrowid
    db.execute("INSERT INTO handover (shift_id,user_id,user_name,branch_id,date,sched_end,created_at,draft) VALUES (?,?,?,1,?,?,?,'{}')",
               (sid, uid, nm, now.strftime('%Y-%m-%d'), (now + timedelta(hours=4)).isoformat(), now.isoformat()))
    db.commit()
    return sid
check(bot.ho_enabled(None, db), 'devir akışı açık')
# Yetkisiz: owner olmayan açamaz
act(11, {'action': 'close_free_set', 'target': 10, 'on': 1})
check(not bot.close_free_on(db, 10), 'barista kendine izin veremez')
# İzin yok → plan bitişinden önce kapanmaz
s11 = open_shift(11, 'Бек')
act(11, {'action': 'shift_end', 'drinks': {}})
check(bot.get_active_shift(db, 11) is not None, 'izinsiz kişi erken kapatamıyor (kilit)')
# Owner izin verir → kapanır, ihlal yok
act(1, {'action': 'close_free_set', 'target': 10, 'on': 1})
check(bot.close_free_on(db, 10), 'owner izni açtı')
s10 = open_shift(10, 'Убай')
act(10, {'action': 'shift_end', 'drinks': {}})
check(bot.get_active_shift(db, 10) is None, 'izinli kişi erken kapattı')
h = bot.ho_row_for_shift(db, s10)
check(h["early_status"] == "approved" and not h["unauth"], 'erken çıkış onaylı, ihlal kaydı yok')
# Kapatınca eski hal
act(1, {'action': 'close_free_set', 'target': 10, 'on': 0})
check(not bot.close_free_on(db, 10), 'owner izni kapattı')
print('FAIL' if FAIL else 'ALL OK', FAIL)
raise SystemExit(1 if FAIL else 0)
