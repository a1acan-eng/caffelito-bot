# Объединить названия: несколько вариантов одного товара → одно (owner 2026-11-17).
# Çalıştırma: rm -f /tmp/bm.db; PYTHONPATH=. DB_PATH=/tmp/bm.db BOT_TOKEN=123:abc python tests/brx_merge_test.py
import json, asyncio, bot
FAIL = []
def check(c, m):
    print(('  ok  ' if c else '  FAIL ') + m)
    if not c: FAIL.append(m)
class FB:
    async def send_message(self, *a, **k): return None
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
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized,branch_id) VALUES (5,'Akmal','barista',1,1,1)")
db.execute("INSERT OR REPLACE INTO branches (id,name,active) VALUES (1,'C5',1)")
db.execute("INSERT OR REPLACE INTO branches (id,name,active) VALUES (2,'Magic',1)")
for it, q in (('крышка на стакан 300/400/500', 4), ('Крышки большие', 5), ('крышки  большие', 1), ('Чай', 2)):
    db.execute("INSERT INTO brx_tx (from_bid,to_bid,item,unit,qty,by_id,by_name,at) VALUES (1,2,?,'пачка',?,5,'A','2026-11-16T10:00:00')", (it, q))
db.execute("INSERT INTO brx_items (name,unit,by_name,at,pack) VALUES ('Крышки большие','пачка','x','2026-11-01',50)")
db.commit()
r = act(5, {'action': 'brx_merge_many', 'srcs': ['Крышки большие', 'крышка на стакан 300/400/500'], 'dst': 'Крышка'})
check(r and not r.get('ok'), 'barista birleştiremez')
r = act(1, {'action': 'brx_merge_many', 'srcs': ['Крышки большие', 'крышка на стакан 300/400/500', 'Крышка 300/400/500'], 'dst': 'Крышка 300/400/500'})
names = [x[0] for x in db.execute("SELECT DISTINCT item FROM brx_tx ORDER BY item").fetchall()]
check(r.get('ok') and r.get('done') == 3 and names == ['Крышка 300/400/500', 'Чай'], f'birleşti ({r}, {names})')
bal = bot.brx_balance(db)
check(abs(sum(v for d in bal.values() for k, v in d.items() if k[0].startswith('крышка'))) == 500 and len(list(bal.values())[0]) == 2, f'bakiye tek satır (10 пач. × 50 = 500 шт) ({bal})')
it = db.execute("SELECT name, pack, COALESCE(archived,0) a FROM brx_items ORDER BY id").fetchall()
check([tuple(x) for x in it] == [('Крышки большие', 50, 1), ('Крышка 300/400/500', 50, 0)], f'liste: eski arşiv, yeni ad paketiyle ({[tuple(x) for x in it]})')
print('ALL OK' if not FAIL else 'FAILED: %s' % FAIL)
raise SystemExit(1 if FAIL else 0)
