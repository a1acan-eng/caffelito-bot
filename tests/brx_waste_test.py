# «Брак / списание» (owner 2026-10-11): borç tablosuna girmez, bardak sayımından düşer.
# Çalıştırma: rm -f /tmp/bw.db; PYTHONPATH=. DB_PATH=/tmp/bw.db BOT_TOKEN=123:abc python tests/brx_waste_test.py
import json, asyncio, urllib.parse as up, bot
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
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (10,'Бек','barista',1,1,1)")
for bid, nm in ((1, 'C5'), (2, 'Magic')):
    db.execute("INSERT OR REPLACE INTO branches (id, name, active) VALUES (?,?,1)", (bid, nm))
# Eskiden «чужая точка» olarak açılmış «Брак»
old = db.execute("INSERT INTO brx_ext (name, at, own) VALUES ('Брак', ?, 0)", (datetime.now(bot.TZ).isoformat(),)).lastrowid
db.execute("INSERT INTO brx_tx (from_bid,to_bid,item,unit,qty,by_id,by_name,at) VALUES (1,?,'Стакан 500','шт',4,10,'Бек',?)", (-old, datetime.now(bot.TZ).isoformat()))
db.commit()
check(-old in bot.brx_waste_ids(db), 'eski «Брак» noktası брак olarak tanındı')
check(bot.brx_balance(db) == {}, 'eski брак kaydı borç tablosundan çıktı')
act(10, {'action': 'brx_add', 'from': 1, 'to': 'waste', 'note': 'порвался', 'items': [{'item': 'Стакан 400', 'unit': 'шт', 'qty': 3}]})
r = db.execute("SELECT to_bid, note FROM brx_tx WHERE item='Стакан 400'").fetchone()
check(r and r['to_bid'] == -old and r['note'] == 'порвался', 'çalışan брак yazdı (aynı «Брак» noktasına, sebep ile)')
check(db.execute("SELECT COUNT(*) c FROM brx_ext").fetchone()['c'] == 1, 'yeni «Брак» noktası açılmadı')
check(bot.brx_balance(db) == {}, 'брак borç değil')
pend = bot.brx_cup_pending(db, 1)
check(sorted((x['cup'], x['q']) for x in pend) == [('Стакан 400', -3), ('Стакан 500', -4)], 'брак bardaklar kapanış sayımından düşer')
act(10, {'action': 'brx_add', 'from': -old, 'to': 1, 'items': [{'item': 'Стакан 400', 'unit': 'шт', 'qty': 1}]})
check(db.execute("SELECT COUNT(*) c FROM brx_tx WHERE from_bid=?", (-old,)).fetchone()['c'] == 0, 'braktan geri alma yok')
act(1, {'action': 'brx_add', 'from': 1, 'to': 2, 'items': [{'item': 'Сахар', 'unit': 'кг', 'qty': 2}]})
check(bot.brx_balance(db) == {(1, 2): {('сахар', 'кг'): 2.0}}, 'normal şube borcu yine çalışıyor')
s = bot.build_hash_payload(db, 1, 'x')
d = dict(p.split('=', 1) for p in s.lstrip('#').split('&') if '=' in p)
bx = json.loads(up.unquote(d['brx']))
check([e['w'] for e in bx['ext'] if e['id'] == -old] == [1], 'payload: брак noktası w=1')
print('FAIL' if FAIL else 'ALL OK', FAIL)
