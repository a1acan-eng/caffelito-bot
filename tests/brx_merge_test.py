# Aynı ürün iki adla (owner 2026-10-31: «Cacao / Какао», «Сливки 11% / Сливки 11% 0,2 л») →
# owner birleştirir: tüm kayıtlar tek ada, borç tek satırda toplanır; katalogdaki eski ad arşive.
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
db = bot.get_db()
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized) VALUES (1,'Owner','owner',1,1)")
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (10,'Дамир','barista',1,1,1)")
for i, n in ((1, 'C5'), (2, 'Magic')):
    db.execute("INSERT OR REPLACE INTO branches (id, name, active) VALUES (?,?,1)", (i, n))
for it, q in (('Cacao', 1), ('Какао', 2), ('Сливки 11%', 5), ('Сливки 11% 0,2 л', 6)):
    db.execute("INSERT INTO brx_tx (from_bid,to_bid,item,unit,qty,note,by_id,by_name,at) VALUES (2,1,?,'шт',?,'',1,'O','2026-10-30T10:00')", (it, q))
db.execute("INSERT INTO brx_items (name,unit,by_name,at) VALUES ('Cacao','шт','O','x')")
db.commit()
r = act(10, {'action': 'brx_merge', 'src': 'Cacao', 'dst': 'Какао'})
check(r and not r.get('ok'), 'barista birleştiremez')
r = act(1, {'action': 'brx_merge', 'src': 'Cacao', 'dst': 'Какао'})
check(r and r.get('ok') and r['done'] == 1, 'Cacao → Какао (1 kayıt)')
r = act(1, {'action': 'brx_merge', 'src': 'Сливки 11% 0,2 л', 'dst': 'Сливки 11%'})
check(r and r.get('ok'), 'Сливки birleşti')
bal = bot.brx_balance(db)[(1, 2)]
check(bal.get(('какао', 'шт')) == -3 and ('cacao', 'шт') not in bal, f'Какао tek satır: 3 {bal}')
check(bal.get(('сливки 11%', 'шт')) == -11, 'Сливки tek satır: 11')
check(db.execute("SELECT archived FROM brx_items WHERE name='Cacao'").fetchone()[0] == 1, 'katalogda eski ad arşivde')
check(db.execute("SELECT 1 FROM logs WHERE action='brx_merge'").fetchone() is not None, 'günlükte')
r = act(1, {'action': 'brx_merge', 'src': 'Какао', 'dst': 'какао'})
check(r and not r.get('ok'), 'aynı ada birleştirme reddedilir')
print('ALL OK' if not FAIL else 'FAILED: %s' % FAIL)
raise SystemExit(1 if FAIL else 0)
