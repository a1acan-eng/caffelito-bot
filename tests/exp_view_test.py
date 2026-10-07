# «Расходы» — owner'a özel masraf defteri (owner 2026-10-07).
# Çalıştırma: rm -f /tmp/ex.db; PYTHONPATH=. DB_PATH=/tmp/ex.db BOT_TOKEN=123:abc python tests/exp_view_test.py
import asyncio, json, urllib.parse as _up, bot
from datetime import datetime, timedelta
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
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized) VALUES (1,'Owner','owner',1,1)")
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (10,'Бек','barista',1,1,1)")
db.commit()
act(10, {'action': 'cash_report', 'branch_id': 1, 'cups': [], 'note': '',
         'expenses': [{'n': 'Обед', 'a': 40000}, {'n': 'Сахар  (магазин)', 'a': 25000}, {'n': 'В долг · Вафохон', 'a': 36000}, {'n': 'Свои · Брат', 'a': 9000}, {'n': 'пусто', 'a': 0}]})
act(10, {'action': 'cash_report', 'branch_id': 1, 'cups': [], 'note': '', 'expenses': [{'n': 'обед', 'a': 35000}]})
v = bot.exp_view(db)
today = datetime.now(bot.TZ).strftime('%Y-%m-%d')
check(len(v['days']) == 1 and v['days'][0]['d'] == today, 'bugün tek gün')
check(sorted(x['a'] for x in v['days'][0]['it']) == [25000, 35000, 40000], 'misafir borcu / свои / 0 satırları hariç')
check(v['mtot'].get(1) == 100000, 'ay toplamı 100 000')
o = [x for x in v['names'] if x['n'].lower() == 'обед'][0]
check(o['a'] == 75000 and o['c'] == 2, 'kalem: «Обед» iki kez (büyük/küçük harf aynı) 75 000')
check(any(x['n'] == 'Сахар (магазин)' for x in v['names']), 'yeni ürün otomatik kalem (boşluk temiz)')
def pl(uid):
    s = bot.build_hash_payload(db, uid, 'x')
    return {k: json.loads(_up.unquote(v)) for k, v in (p.split('=', 1) for p in s.lstrip('#').split('&') if '=' in p) if k in ('expv', 'exp_names')}
po, pe = pl(1), pl(10)
check('expv' in po and po['expv']['mtot'], 'owner payload «Расходы» taşıyor')
check('expv' not in pe, 'çalışan «Расходы» görmez')
check({n.lower() for n in pe.get('exp_names', [])} >= {'обед', 'сахар (магазин)'} and not any('долг' in n.lower() for n in pe['exp_names']), 'çalışana yalnız adlar (öneri için)')
print('FAIL' if FAIL else 'ALL OK', FAIL)
