# Owner «Закрыть как сотрудник» (owner 2026-10-24): owner başkasının vardiyasını o kişinin
# kendi kapanışıyla kapatır (bardak + kasa raporu), owner'ın kendi vardiyasına dokunulmaz.
# Çalıştırma: rm -f /tmp/ca.db; PYTHONPATH=. DB_PATH=/tmp/ca.db BOT_TOKEN=123:abc python tests/close_as_test.py
import json, asyncio, bot
from datetime import datetime, timedelta
FAIL = []
def check(c, m):
    print(('  ok  ' if c else '  FAIL ') + m)
    if not c: FAIL.append(m)
SENT = []
class FB:
    async def send_message(self, chat_id=None, text=None, *a, **k):
        SENT.append((chat_id, text or '')); return None
    def __getattr__(self, n):
        async def f(*a, **k): return None
        return f
def act(uid, p):
    ctx = bot._ShimContext(FB(), {}, uid=uid)
    asyncio.run(bot.handle_webapp_data(bot._ShimUpdate(FB(), uid, f'U{uid}', None, json.dumps(p)), ctx))
    return ctx.result
db = bot.get_db()
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (1,'Owner','owner',1,1,1)")
for uid, nm in ((10, 'Бек'), (11, 'Убай')):
    db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (?,?,'barista',1,1,1)", (uid, nm))
db.execute("INSERT OR REPLACE INTO branches (id, name, active) VALUES (1,'C5',1)")
db.execute("INSERT OR REPLACE INTO branches (id, name, active) VALUES (2,'Magic',1)")
db.commit()
now = datetime.now(bot.TZ).replace(tzinfo=None)
def open_shift(uid, h, bid=1):
    st = now - timedelta(hours=h)
    sid = db.execute("INSERT INTO shifts (user_id,hours,total,date,period,created_at,start_time,branch_id,shift_role) "
                     "VALUES (?,0,0,?,?,?,?,?,'barista')",
                     (uid, st.strftime('%Y-%m-%d'), st.strftime('%Y-%m'), st.isoformat(), st.isoformat(), bid)).lastrowid
    db.commit(); return sid
def row(sid): return db.execute("SELECT * FROM shifts WHERE id=?", (sid,)).fetchone()
s1, s10 = open_shift(1, 2, bid=2), open_shift(10, 6)
db.execute("INSERT INTO cashreports (user_id,user_name,date,period,created_at,ostalos,branch_id) VALUES (11,'Убай',?,?,?,?,1)",
           (now.strftime('%Y-%m-%d'), now.strftime('%Y-%m'), (now - timedelta(days=1)).isoformat(), json.dumps({"Стакан 300": 77})))
db.commit()
# info
r = act(1, {'action': 'close_as_info', 'target': 10})
check(r and r.get('ok') and r['uid'] == 10 and r['bid'] == 1 and r['asst'] == 0, 'close_as_info: kişi/şube/rol')
check(r and r.get('kasa_last', {}).get('Стакан 300') == 77, 'close_as_info: şubenin son «Осталось»u')
check((act(11, {'action': 'close_as_info', 'target': 10}) or {}).get('ok') is False, 'barista close_as_info alamaz')
# Barista başkası adına kapatamaz
act(11, {'action': 'shift_end', 'drinks': {"ml300": 5}, 'as_uid': 10})
check(row(s10)["end_time"] is None, 'barista başkasının vardiyasını kapatamaz')
# Owner, Бек adına bardaklı kapanış
drinks = {"ml300": 20}
full = bot.calc_bonus(drinks, bot.get_prices(db))
SENT.clear()
act(1, {'action': 'shift_end', 'drinks': drinks, 'as_uid': 10})
r10 = row(s10)
check(r10["end_time"] is not None and (r10["bonus"] or 0) == full, f'Бек vardiyası bardaklarla kapandı (bonus {full})')
check(row(s1)["end_time"] is None, 'owner\'ın kendi vardiyası açık kaldı')
check(any(str(c) == '10' and 'закрыл владелец' in t for c, t in SENT), 'Бек\'e «закрыл владелец» bildirimi')
act(1, {'action': 'cash_report', 'cups': [], 'itogo': 0, 'vyshlo': 100000, 'daily_pay': full, 'branch_id': 1, 'as_uid': 10,
        'start_time': r10["start_time"], 'end_time': r10["end_time"]})
cr = db.execute("SELECT user_id, branch_id, daily_pay FROM cashreports ORDER BY id DESC LIMIT 1").fetchone()
check(cr["user_id"] == 10 and cr["branch_id"] == 1 and cr["daily_pay"] == full, 'kasa raporu Бек adına, C5 şubesine')
check(db.execute("SELECT COUNT(*) FROM cashreports WHERE user_id=1").fetchone()[0] == 0, 'owner adına rapor yazılmadı')
u10 = db.execute("SELECT name FROM users WHERE user_id=10").fetchone()
check(u10["name"] == 'Бек', 'Бек\'in kaydı owner bilgisiyle ezilmedi')
print('ALL OK' if not FAIL else 'FAILED: %s' % FAIL)
raise SystemExit(1 if FAIL else 0)
