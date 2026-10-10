# Между филиалами v2 (owner 2026-10-10): paket boyutu, kayıt düzeltme, «наша/чужая» nokta.
# Çalıştırma: rm -f /tmp/b2.db; PYTHONPATH=. DB_PATH=/tmp/b2.db BOT_TOKEN=123:abc python tests/brx_v2_test.py
import json, asyncio, bot
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
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (11,'Али','barista',1,1,1)")
for bid, nm in ((1, 'C5'), (2, 'Magic')):
    db.execute("INSERT OR REPLACE INTO branches (id, name, active) VALUES (?,?,1)", (bid, nm))
db.commit()
bal = lambda: {(a, b, k): v for (a, b), d in bot.brx_balance(db).items() for k, v in d.items()}
# paket boyutu
act(1, {'action': 'brx_item_pack', 'name': 'Крышки большие', 'pack': 100})
check(bot.brx_packs(db).get('крышки большие') == 100, 'owner paket boyutu ayarladı (100 шт)')
act(10, {'action': 'brx_item_pack', 'name': 'Крышки большие', 'pack': 5})
check(bot.brx_packs(db).get('крышки большие') == 100, 'çalışan paket boyutunu değiştiremez')
act(1, {'action': 'brx_add', 'from': 1, 'to': 2, 'items': [{'item': 'Крышки большие', 'unit': 'пачка', 'qty': 2}, {'item': 'Крышки большие', 'unit': 'шт', 'qty': 30}]})
check(bal().get((1, 2, ('крышки большие', 'шт'))) == 230, 'пачка + шт adette toplanır (2×100 + 30 = 230)')
# düzeltme
tx = db.execute("SELECT id FROM brx_tx WHERE unit='пачка'").fetchone()['id']
act(11, {'action': 'brx_edit', 'id': tx, 'qty': 1, 'unit': 'пачка'})
check(db.execute("SELECT qty FROM brx_tx WHERE id=?", (tx,)).fetchone()['qty'] == 2, 'başkasının kaydını çalışan düzeltemez')
act(1, {'action': 'brx_edit', 'id': tx, 'qty': 1, 'unit': 'пачка'})
r = db.execute("SELECT qty, edited_by FROM brx_tx WHERE id=?", (tx,)).fetchone()
check(r['qty'] == 1 and r['edited_by'], 'owner düzeltti (2 → 1 пачка), kimin düzelttiği yazılı')
check(bal().get((1, 2, ('крышки большие', 'шт'))) == 130, 'bakiye yeniden hesaplandı (130 шт)')
act(10, {'action': 'brx_add', 'from': 1, 'to': 2, 'items': [{'item': 'Сахар', 'unit': 'кг', 'qty': 5}]})
t2 = db.execute("SELECT id FROM brx_tx WHERE item='Сахар'").fetchone()['id']
act(10, {'action': 'brx_edit', 'id': t2, 'qty': 3, 'unit': 'кг'})
check(db.execute("SELECT qty FROM brx_tx WHERE id=?", (t2,)).fetchone()['qty'] == 3, 'yazan kendi kaydını 24 saat içinde düzeltir')
# yeni nokta: başka sahibin → «Взяли»
act(10, {'action': 'brx_add', 'from': {'n': 'Coffee Lab', 'own': 0}, 'to': 1, 'items': [{'item': 'Молоко', 'unit': 'л', 'qty': 12}]})
e = db.execute("SELECT id, own FROM brx_ext WHERE name='Coffee Lab'").fetchone()
check(e is not None and e['own'] == 0, 'yeni dış nokta «чужая» olarak eklendi')
check(bal().get((-e['id'], 1, ('молоко', 'л'))) == 12, 'çalışan dış noktadan «Взяли» yazabildi (biz borçluyuz)')
act(1, {'action': 'brx_add', 'from': 1, 'to': {'n': 'Склад', 'own': 1}, 'items': [{'item': 'Сахар', 'unit': 'кг', 'qty': 1}]})
check(db.execute("SELECT own FROM brx_ext WHERE name='Склад'").fetchone()['own'] == 1, '«наша точка» işaretli kaydedildi')
s = bot.build_hash_payload(db, 1, 'x')
d = dict(p.split('=', 1) for p in s.lstrip('#').split('&') if '=' in p)
import urllib.parse as up
bx = json.loads(up.unquote(d['brx']))
check(any(i.get('pk') == 100 for i in bx['items']) and any(x.get('own') == 1 for x in bx['ext']), 'payload: pk ve own gidiyor')
print('FAIL' if FAIL else 'ALL OK', FAIL)
