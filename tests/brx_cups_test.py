# Между филиалами ↔ kapanış «Завоз» (owner 2026-10-06).
# Çalıştırma: rm -f /tmp/bc.db; PYTHONPATH=. DB_PATH=/tmp/bc.db BOT_TOKEN=123:abc python tests/brx_cups_test.py
import asyncio, json, bot
FAIL = []
def check(c, m):
    print(('  ok  ' if c else '  FAIL ') + m)
    if not c: FAIL.append(m)
db = bot.get_db()
sent = []
class FB:
    async def send_message(self, chat_id=None, text=None, *a, **k): sent.append((chat_id, text))
    def __getattr__(self, n):
        async def f(*a, **k): return None
        return f
def act(uid, p):
    sent.clear()
    asyncio.run(bot.handle_webapp_data(bot._ShimUpdate(FB(), uid, f'U{uid}', None, json.dumps(p)), bot._ShimContext(FB(), {}, uid=uid)))
db.execute("UPDATE branches SET name='C5', group_chat_id='-1001' WHERE id=1")
db.execute("INSERT OR REPLACE INTO branches (id,name,group_chat_id,sort_order,active) VALUES (2,'Magic','-1002',2,1)")
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized) VALUES (1,'Owner','owner',1,1)")
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (10,'Бек','barista',1,1,1)")
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (11,'Вали','barista',2,1,1)")
db.commit()
act(10, {'action': 'brx_add', 'from': 1, 'to': 2, 'item': 'стакан 300', 'unit': 'шт', 'qty': 50})
act(10, {'action': 'brx_add', 'from': 1, 'to': 2, 'item': 'Молоко', 'unit': 'л', 'qty': 6})
p1, p2 = bot.brx_cup_pending(db, 1), bot.brx_cup_pending(db, 2)
check(len(p1) == 1 and p1[0]['cup'] == 'Стакан 300' and p1[0]['q'] == -50 and p1[0]['other'] == 'Magic', 'C5 kapanışında −50 (bardak dışı ürün gelmez)')
check(len(p2) == 1 and p2[0]['q'] == 50 and p2[0]['other'] == 'C5', 'Magic kapanışında +50')
pay = bot.build_hash_payload(db, 10, 'Бек')
check('brx_cups=' in pay, 'payload brx_cups taşıyor')
# C5 kapanışı: bekleyen kullanıldı + elle −30 Стакан 500 → Magic
act(10, {'action': 'cash_report', 'branch_id': 1, 'cups': [], 'expenses': [], 'note': '',
         'brx_used': [p1[0]['id']], 'brx_new': [{'to': 2, 'item': 'Стакан 500', 'qty': 30}]})
check(bot.brx_cup_pending(db, 1) == [], 'C5: kapanıştan sonra tekrar düşmez')
p2 = bot.brx_cup_pending(db, 2)
check(sorted((x['cup'], x['q']) for x in p2) == [('Стакан 300', 50), ('Стакан 500', 30)], 'Magic: formdan gelen +30 Стакан 500 de bekliyor')
check(any('Стакан 500' in (t or '') and 'из закрытия' in (t or '') for c, t in sent if c == -1002), 'formdan transfer Magic grubuna bildirildi')
b = bot.brx_balance(db)[(1, 2)]
check(b[('стакан 500', 'шт')] == 30 and b[('стакан 300', 'шт')] == 50, 'borç defterine de yazıldı (Magic C5\'e borçlu)')
act(11, {'action': 'cash_report', 'branch_id': 2, 'cups': [], 'expenses': [], 'note': '', 'brx_used': [x['id'] for x in p2]})
check(bot.brx_cup_pending(db, 2) == [], 'Magic kapanışından sonra boş')
act(10, {'action': 'cash_report', 'branch_id': 1, 'cups': [], 'expenses': [], 'note': '', 'brx_used': [x['id'] for x in p2]})
check(bot.brx_balance(db)[(1, 2)][('стакан 500', 'шт')] == 30, 'başka şubenin id\'leri tüketilmez / borç değişmez')
print('FAIL' if FAIL else 'ALL OK', FAIL)
