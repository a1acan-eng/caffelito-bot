# «Скопировать прошлую неделю» (owner 2026-10-06).
# Çalıştırma: rm -f /tmp/gc.db; PYTHONPATH=. DB_PATH=/tmp/gc.db BOT_TOKEN=123:abc python tests/grid_copy_test.py
import asyncio, json, bot
FAIL = []
def check(c, m):
    print(('  ok  ' if c else '  FAIL ') + m)
    if not c: FAIL.append(m)
db = bot.get_db()
class FB:
    async def send_message(self, *a, **k): return None
    def __getattr__(self, n):
        async def f(*a, **k): return None
        return f
def act(uid, p):
    asyncio.run(bot.handle_webapp_data(bot._ShimUpdate(FB(), uid, f'U{uid}', None, json.dumps(p)), bot._ShimContext(FB(), {}, uid=uid)))
db.execute("UPDATE branches SET name='C5', group_chat_id='-1001' WHERE id=1")
db.execute("DELETE FROM shift_templates")
db.execute("INSERT INTO shift_templates (code,branch_id,start_t,end_t,active) VALUES ('c5d',1,'07:00','17:00',1)")
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized) VALUES (1,'Owner','owner',1,1)")
for u in (10, 11):
    db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (?,?,'barista',1,1,1)", (u, f'U{u}'))
src, dst = bot.grid_week_key(0), bot.grid_week_key(1)
for d in range(7):
    db.execute("INSERT INTO shift_grid (week_key,day,user_id,code) VALUES (?,?,10,?)", (src, d, 'off' if d == 2 else 'c5d'))
db.execute("INSERT INTO shift_grid (week_key,day,user_id,code) VALUES (?,0,11,'c5d')", (src,))
db.execute("INSERT INTO shift_grid (week_key,day,user_id,code) VALUES (?,1,10,'off')", (dst,))   # dolu hücreye dokunulmaz
db.commit()
act(10, {'action': 'shift_grid_copy', 'week': 1, 'uids': [10, 11]})
check(db.execute("SELECT COUNT(*) c FROM shift_grid WHERE week_key=?", (dst,)).fetchone()['c'] == 1, 'çalışan kopyalayamaz')
act(1, {'action': 'shift_grid_copy', 'week': 1, 'uids': [10, 11]})
g = {(r['user_id'], r['day']): r['code'] for r in db.execute("SELECT * FROM shift_grid WHERE week_key=?", (dst,))}
check(g.get((10, 0)) == 'c5d' and g.get((10, 2)) == 'off' and g.get((11, 0)) == 'c5d', 'vardiya ve izinler kopyalandı')
check(g.get((10, 1)) == 'off', 'dolu hücre korunur')
check(len(g) == 8, f'toplam 8 hücre ({len(g)})')
print('FAIL' if FAIL else 'ALL OK', FAIL)
