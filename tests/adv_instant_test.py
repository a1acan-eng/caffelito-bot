# Аванс сразу: выдан → сразу удержан в этой декаде (owner 2026-11-16).
# Çalıştırma: rm -f /tmp/ai.db; PYTHONPATH=. DB_PATH=/tmp/ai.db BOT_TOKEN=123:abc python tests/adv_instant_test.py
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
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized,branch_id) VALUES (1,'Owner','owner',1,1,1)")
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized,branch_id) VALUES (11,'Ubay','barista',1,1,1)")
db.execute("INSERT OR REPLACE INTO branches (id,name,active) VALUES (1,'C5',1)")
db.commit()
per = bot.current_period()
s0 = bot.calc_summary(db, 11, per)
act(11, {'action': 'adv_instant', 'target': 11, 'amount': 500000})
check(not db.execute("SELECT 1 FROM advances").fetchone(), 'barista kendine veremez')
act(1, {'action': 'adv_instant', 'target': 11, 'amount': '500 000', 'note': 'наличными'})
a = db.execute("SELECT * FROM advances").fetchone()
check(a and a['amount'] == 500000 and a['total'] == 500000 and a['commission'] == 0, f'avans kaydı ({dict(a) if a else None})')
i = db.execute("SELECT * FROM advance_inst").fetchall()
check(len(i) == 1 and i[0]['due_date'] == bot._adv_today().isoformat(), 'tek parça, vadesi bugün')
s1 = bot.calc_summary(db, 11, per)
check(int(s1['adv_ded']) - int(s0['adv_ded']) == 500000 and int(s0['net']) - int(s1['net']) == 500000, f"maaştan hemen düştü ({s0['net']} → {s1['net']})")
k = 1 if bot._adv_today().day <= 10 else (2 if bot._adv_today().day <= 20 else 3)
st = bot.decade_statement(db, 11, per, k)
check(st['dec']['adv'] == 500000 and st['dec']['adj'] == 0, f"dekad: аванс −500.000, корр. 0 ({st['dec']['adv']}, {st['dec']['adj']})")
print('ALL OK' if not FAIL else 'FAILED: %s' % FAIL)
raise SystemExit(1 if FAIL else 0)
