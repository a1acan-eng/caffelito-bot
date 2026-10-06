# Долги гостей + şubeler arası alışveriş (owner 2026-10-06).
# Çalıştırma: rm -f /tmp/gb.db; PYTHONPATH=. DB_PATH=/tmp/gb.db BOT_TOKEN=123:abc python tests/gdebt_brx_test.py
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
db.commit()
print('Долги гостей')
act(10, {'action': 'gdebt_guest_add', 'name': ' Алишер  ака ', 'branch_id': 1, 'amount': 45})
v = bot.gdebt_view(db); g = v['guests'][0]
check(g['n'] == 'Алишер ака' and g['bal'] == 45000 and g['bid'] == 1, 'yeni misafir + ilk borç (45 → 45 000)')
act(10, {'action': 'gdebt_guest_add', 'name': 'алишер АКА', 'branch_id': 1})
check(len(bot.gdebt_view(db)['guests']) == 1, 'aynı ad aynı şubede tekrar açılmaz')
act(10, {'action': 'gdebt_tx', 'guest_id': g['id'], 'kind': 'pay', 'amount': 20000})
act(10, {'action': 'gdebt_tx', 'guest_id': g['id'], 'kind': 'debt', 'amount': 12000})
check(bot.gdebt_view(db)['guests'][0]['bal'] == 37000, 'ödeme düşer, yeni borç eklenir (37 000)')
tx = bot.gdebt_view(db)['tx'][0]
act(10, {'action': 'gdebt_void', 'id': tx['id']})
check(bot.gdebt_view(db)['guests'][0]['bal'] == 37000, 'çalışan kayıt silemez')
act(1, {'action': 'gdebt_void', 'id': tx['id']})
check(bot.gdebt_view(db)['guests'][0]['bal'] == 25000, 'owner siler → bakiye 25 000')
print('Şubeler arası')
act(10, {'action': 'brx_add', 'from': 2, 'to': 1, 'item': 'Стакан 300', 'unit': 'шт', 'qty': 400})
check(any(c == -1001 for c, _ in sent) and any(c == -1002 for c, _ in sent), 'iki şube grubuna bildirim')
act(10, {'action': 'brx_add', 'from': 1, 'to': 2, 'item': 'стакан 300', 'unit': 'шт', 'qty': 200})
act(10, {'action': 'brx_add', 'from': 1, 'to': 2, 'item': 'Кофе эспрессо', 'unit': 'кг', 'qty': '1,5'})
b = bot.brx_balance(db)[(1, 2)]
check(b[('стакан 300', 'шт')] == -200, 'Magic 400 verdi, C5 200 geri → C5 Magic\'e 200 borçlu')
check(b[('кофе эспрессо', 'кг')] == 1.5, 'kahve 1,5 kg: Magic C5\'e borçlu')
act(10, {'action': 'brx_add', 'from': 1, 'to': 1, 'item': 'X', 'qty': 1})
check(len(bot.brx_view(db)) == 3, 'aynı şube reddedilir')
act(1, {'action': 'brx_void', 'id': bot.brx_view(db)[0]['id']})
check(len(bot.brx_view(db)) == 2, 'owner kaydı siler')
print('Kapanış raporu')
act(10, {'action': 'cash_report', 'branch_id': 1, 'cups': [], 'expenses': [{'n': 'В долг · Новый гость', 'a': 30000}],
         'gdebts': [{'g': 0, 'n': 'Новый гость', 'a': 30000, 'k': 'debt'}, {'g': g['id'], 'n': g['n'], 'a': 5000, 'k': 'pay'}], 'note': ''})
v = bot.gdebt_view(db); gm = {x['n']: x['bal'] for x in v['guests']}
check(gm.get('Новый гость') == 30000, 'rapordaki yeni misafir + borç yazıldı')
check(gm.get('Алишер ака') == 20000, 'rapordaki ödeme düştü (25 000 → 20 000)')
check(any('Долги гостей' in (t or '') for c, t in sent if c == -1001), 'grup raporunda «Долги гостей» bölümü')
print('Payload')
pay = bot.build_hash_payload(db, 10, 'Бек')
check('gdebt=' in pay and 'brx=' in pay, 'çalışan payload\'ı ikisini de taşıyor')
print('FAIL' if FAIL else 'ALL OK', FAIL)
