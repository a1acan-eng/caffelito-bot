# Долги гостей: her şubeye ayrı PIN (owner 2026-10-11) — şube değiştirip başka şubenin
# listesini aynı PIN'le açmak mümkün olmamalı.
# Çalıştırma: rm -f /tmp/gp.db; PYTHONPATH=. DB_PATH=/tmp/gp.db BOT_TOKEN=123:abc python tests/gdebt_pin_branch_test.py
import json, asyncio, urllib.parse as up, bot
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
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (10,'Бек','barista',1,1,1)")
for bid, nm in ((1, 'C5'), (2, 'Magic')):
    db.execute("INSERT OR REPLACE INTO branches (id, name, active) VALUES (?,?,1)", (bid, nm))
db.execute("INSERT INTO guests (name, branch_id, created_at) VALUES ('Гость C5',1,'x')")
db.execute("INSERT INTO guests (name, branch_id, created_at) VALUES ('Гость Magic',2,'x')")
db.commit()
def gd(uid):
    s = bot.build_hash_payload(db, uid, 'x')
    d = dict(p.split('=', 1) for p in s.lstrip('#').split('&') if '=' in p)
    return json.loads(up.unquote(d['gdebt']))
def to_branch(b):
    db.execute("INSERT OR REPLACE INTO meta (k,val) VALUES ('cur_branch_10', ?)", (str(b),)); db.commit()
act(1, {'action': 'gdebt_pin_set', 'pin': '1111', 'branch_id': 1})
act(1, {'action': 'gdebt_pin_set', 'pin': '1111', 'branch_id': 2})
check(not bot.gdebt_pin_set(db, 2), 'aynı PIN ikinci şubeye konamaz')
act(1, {'action': 'gdebt_pin_set', 'pin': '2222', 'branch_id': 2})
check(bot.gdebt_pin_set(db, 1) and bot.gdebt_pin_set(db, 2), 'iki şubenin ayrı PIN\'i var')
to_branch(1)
check(gd(10)['locked'] == 1, 'C5: PIN girilmeden kapalı')
act(10, {'action': 'gdebt_unlock', 'pin': '1111'})
g = gd(10)
check(g['locked'] == 0 and [x['n'] for x in g['guests']] == ['Гость C5'], 'C5 PIN\'i ile yalnız C5 misafirleri açıldı')
to_branch(2)
check(gd(10)['locked'] == 1, 'Magic\'e geçince liste YİNE kapalı')
act(10, {'action': 'gdebt_unlock', 'pin': '1111'})
check(gd(10)['locked'] == 1, 'C5 PIN\'i Magic\'i açmaz')
act(10, {'action': 'gdebt_unlock', 'pin': '2222'})
g = gd(10)
check(g['locked'] == 0 and [x['n'] for x in g['guests']] == ['Гость Magic'], 'Magic PIN\'i ile yalnız Magic açıldı')
o = gd(1)
check(o['pins'] == {'1': 1, '2': 1}, 'owner hangi şubede PIN olduğunu görür')
act(1, {'action': 'gdebt_pin_set', 'pin': '', 'branch_id': 2})
check(not bot.gdebt_pin_set(db, 2), 'owner bir şubenin PIN\'ini kaldırabilir')
print('FAIL' if FAIL else 'ALL OK', FAIL)
