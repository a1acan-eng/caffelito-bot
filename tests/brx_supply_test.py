# «Склад» (owner 2026-10-20): depodan gelen mal borç DEĞİL; bardaklar kapanış sayımına yine girer.
# Çalıştırma: rm -f /tmp/bs.db; PYTHONPATH=. DB_PATH=/tmp/bs.db BOT_TOKEN=123:abc python tests/brx_supply_test.py
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
db.commit()
# Склад → C5: 100 Купол 400 + 300 Стакан 400 (ekran görüntüsündeki durum)
act(10, {'action': 'brx_add', 'from': 'Склад', 'to': 1, 'items': [{'item': 'Купол 400', 'unit': 'шт', 'qty': 100}, {'item': 'Стакан 400', 'unit': 'шт', 'qty': 300}]})
sid = -db.execute("SELECT id FROM brx_ext WHERE name='Склад'").fetchone()['id']
check(sid in bot.brx_supply_ids(db), '«Склад» otomatik depo olarak işaretlendi')
check(not any(sid in k for k in bot.brx_balance(db)), 'depodan gelen borç tablosunda YOK')
pend = bot.brx_cup_pending(db, 1)
check(sorted((x['cup'], x['q']) for x in pend) == [('Купол 400', 100), ('Стакан 400', 300)], 'bardaklar kapanışta «Было»ya +100 / +300 olarak girer')
# Normal şubeler arası borç bozulmadı
act(10, {'action': 'brx_add', 'from': 1, 'to': 2, 'items': [{'item': 'Сахар', 'unit': 'кг', 'qty': 2}]})
check(bot.brx_balance(db).get((1, 2)) == {('сахар', 'кг'): 2.0}, 'şubeler arası borç eskisi gibi')
# Payload'da «s» işareti
s = bot.build_hash_payload(db, 1, 'x')
d = dict(p.split('=', 1) for p in s.lstrip('#').split('&') if '=' in p)
bx = json.loads(up.unquote(d['brx']))
check(any(e['id'] == sid and e.get('s') == 1 for e in bx['ext']), 'payload: depo noktası s=1')
check(not any(sid in (r['a'], r['b']) for r in bx['bal']), 'payload: depo için borç satırı yok')
# Katalogdan «уп» + paket boyutu (owner 2026-10-20): 2 уп × 27 = 54 шт olarak borç
act(10, {'action': 'brx_add', 'from': 1, 'to': 2, 'items': [{'item': 'Сливки 11% 0,2 л', 'unit': 'уп', 'qty': 2, 'pk': 27}]})
check(bot.brx_balance(db)[(1, 2)].get(('сливки 11% 0,2 л', 'шт')) == 54.0, 'уп + pk=27 → 54 шт (paket boyutu kaydedildi)')
act(10, {'action': 'brx_add', 'from': 1, 'to': 2, 'items': [{'item': 'Сливки 11% 0,2 л', 'unit': 'шт', 'qty': 3}]})
check(bot.brx_balance(db)[(1, 2)].get(('сливки 11% 0,2 л', 'шт')) == 57.0, 'шт ile verilen aynı satıra eklenir (57 шт)')
# Bardak paketle (owner 2026-10-20): Стакан 500 пачка=15 → 2 пачки = 30 шт
db.execute("INSERT INTO brx_items (name, unit, at, pack) VALUES ('Стакан 500','шт','2026-10-01',15)"); db.commit()
act(10, {'action': 'brx_add', 'from': 1, 'to': 2, 'items': [{'item': 'Стакан 500', 'unit': 'пачка', 'qty': 2}]})
r = db.execute("SELECT unit, qty FROM brx_tx WHERE item='Стакан 500' ORDER BY id DESC LIMIT 1").fetchone()
check(r and r['unit'] == 'шт' and r['qty'] == 30, 'Стакан 500: 2 пачки → 30 шт kaydedildi (kapanış adetle)')
print('ALL OK' if not FAIL else 'FAILED: %s' % FAIL)
