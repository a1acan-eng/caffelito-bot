# «Отклонить» заявка на выходной: sunucuda 'no' olur, payload'dan düşer,
# motor açığı kapanır; kayıttaki hafta/gün esastır (owner 2026-10-09).
# Çalıştırma: rm -f /tmp/dr.db; PYTHONPATH=. DB_PATH=/tmp/dr.db BOT_TOKEN=123:abc python tests/dayoff_reject_test.py
import json, asyncio, urllib.parse as _up, bot
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
    asyncio.run(bot.handle_webapp_data(bot._ShimUpdate(FB(), uid, f'U{uid}', None, json.dumps(p)), bot._ShimContext(FB(), {}, uid=uid)))
db = bot.get_db()
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized) VALUES (1,'Owner','owner',1,1)")
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (10,'Сухроб','barista',1,1,1)")
now = datetime.now(bot.TZ).isoformat()
wk0, wk1 = bot.grid_week_key(0), bot.grid_week_key(1)
def req(wk, day):
    c = db.execute("INSERT INTO dayoff_requests (user_id, week_key, day, note, status, created_at) VALUES (10,?,?,'',?,?)",
                   (wk, day, 'pending', now))
    db.commit(); return c.lastrowid
def reqs(uid):
    s = bot.build_hash_payload(db, uid, 'x')
    d = dict(p.split('=', 1) for p in s.lstrip('#').split('&') if '=' in p)
    return json.loads(_up.unquote(d['dayoff_reqs']))
r1 = req(wk0, 6)
db.execute("INSERT INTO open_shifts (week_key, day, code, from_uid, from_name, status, reason, created_at, kind, cov, dayoff_req_id) "
           "VALUES (?,6,'x',10,'Сухроб','open','',?,'dayoff','searching',?)", (wk0, now, r1)); db.commit()
check(len(reqs(1)) == 1, 'owner заявкаyı görüyor')
act(1, {'action': 'dayoff_decide', 'request_id': r1, 'decision': 'no', 'target_uid': 10, 'week': 0, 'day': 6})
check(db.execute("SELECT status FROM dayoff_requests WHERE id=?", (r1,)).fetchone()['status'] == 'no', "durum 'no'")
check(reqs(1) == [] and reqs(10) == [], 'yenileyince заявка geri gelmiyor')
check(db.execute("SELECT status FROM open_shifts WHERE dayoff_req_id=?", (r1,)).fetchone()['status'] == 'done', 'motor açığı kapandı')
# Gelecek haftanın заявкаsı, owner ekranda bu haftaya bakarken onaylanırsa doğru haftaya yazılır.
r2 = req(wk1, 2)
act(1, {'action': 'dayoff_decide', 'request_id': r2, 'decision': 'ok', 'target_uid': 10, 'week': 0, 'day': 2})
g = db.execute("SELECT week_key FROM shift_grid WHERE user_id=10 AND day=2 AND code='off'").fetchall()
check([x['week_key'] for x in g] == [wk1], 'onay kayıttaki haftaya yazıldı')
print('FAIL' if FAIL else 'ALL OK', FAIL)
