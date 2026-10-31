# Süt ve Сливки 11% koli/adet (owner 2026-10-31: «kolide 12 tane, bir tane olarak hesaplıyor»).
# Çalıştırma: rm -f /tmp/bp.db; PYTHONPATH=. DB_PATH=/tmp/bp.db BOT_TOKEN=123:abc python tests/brx_pack_seed_test.py
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
bot.get_db()
db = bot.get_db()
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized) VALUES (1,'Owner','owner',1,1)")
for i, n in ((1, 'C5'), (2, 'Magic')):
    db.execute("INSERT OR REPLACE INTO branches (id, name, active) VALUES (?,?,1)", (i, n))
db.execute("INSERT INTO brx_items (name,unit,by_name,at,pack) VALUES ('Сливки 11%','шт','O','x',30)")
db.commit()
bot.brx_pack_seed(db)
pk = bot.brx_packs(db)
check(pk.get('молоко 3,2%') == 12, f'süt kolisi 12 ({pk})')
check(pk.get('сливки 11%') == 30, 'owner\'ın ayarı ezilmez')
db.execute("UPDATE brx_items SET pack=NULL WHERE name='Молоко 3,2%'"); db.commit()
bot.brx_pack_seed(db)
check(not bot.brx_packs(db).get('молоко 3,2%'), 'tohum bir kez çalışır (owner kaldırdıysa geri gelmez)')
db.execute("UPDATE brx_items SET pack=12 WHERE name='Молоко 3,2%'"); db.commit()
act(1, {'action': 'brx_add', 'from': 1, 'to': 2, 'items': [{'item': 'Молоко 3,2%', 'unit': 'пачка', 'qty': 1, 'pk': 12},
                                                          {'item': 'Молоко 3,2%', 'unit': 'шт', 'qty': 3}]})
bal = bot.brx_balance(db)
v = [d.get(('молоко 3,2%', 'шт')) for d in bal.values()]
check(15 in [abs(x or 0) for x in v], f'1 koli + 3 tane = 15 шт ({bal})')
print('ALL OK' if not FAIL else 'FAILED: %s' % FAIL)
raise SystemExit(1 if FAIL else 0)
