# Stajyerlik sayacı (owner 2026-10-05).
# Çalıştırma: rm -f /tmp/intern.db; PYTHONPATH=. DB_PATH=/tmp/intern.db BOT_TOKEN=123:abc python tests/intern_test.py
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
    asyncio.run(bot.handle_webapp_data(bot._ShimUpdate(FB(), uid, f'U{uid}', None, json.dumps(p)), bot._ShimContext(FB(), {}, uid=uid)))

ct = db.execute("INSERT INTO salary_categories (name,hourly_rate,active,intern_days) VALUES ('Стажёр',12000,1,30)").lastrowid
cb = db.execute("INSERT INTO salary_categories (name,hourly_rate,active) VALUES ('Бариста',22000,1)").lastrowid
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized) VALUES (1,'Owner','owner',1,1)")
for uid, nm, c in ((10, 'Гуля', ct), (11, 'Бек', cb)):
    db.execute("INSERT OR REPLACE INTO users (user_id,name,display_name,role,branch_id,salary_cat_id,approved,authorized) "
               "VALUES (?,?,?,'barista',1,?,1,1)", (uid, nm, nm, c))
def shift(uid, d):
    db.execute("INSERT INTO shifts (user_id,hours,date,period,created_at) VALUES (?,8,?,?,?)", (uid, d, d[:7], d + 'T17:00:00'))
for d in ('2026-09-01', '2026-09-03', '2026-09-03', '2026-09-05'):   # aynı gün iki vardiya = 1 gün
    shift(10, d)
shift(11, '2026-09-01')
db.commit()
i = bot.intern_info(db, 10)
check(i and i['done'] == 3 and i['left'] == 27 and i['total'] == 30 and not i['fin'], 'çalışılan gün sayılıyor (aynı gün tek)')
check(i['from'] == '2026-09-01', 'başlangıç = ilk vardiya günü')
check(bot.intern_info(db, 11) is None, 'stajyer olmayan kategoride sayaç yok')

for k in range(27):
    shift(10, f'2026-08-{k+1:02d}')
db.commit()
i = bot.intern_info(db, 10)
check(i['done'] == 30 and i['left'] == 0 and i['fin'], '30 gün dolunca fin')
sent.clear(); asyncio.run(bot.intern_scan(FB(), db))
check(len(sent) == 1 and sent[0][0] == 1 and 'Гуля' in sent[0][1], 'owner\'a bir bildirim')
sent.clear(); asyncio.run(bot.intern_scan(FB(), db))
check(not sent, 'ikinci kez bildirim yok')

act(10, {'action': 'intern_restart', 'target': 10})
check(bot.intern_info(db, 10)['done'] == 30, 'çalışan kendi sayacını sıfırlayamaz')
act(1, {'action': 'intern_restart', 'target': 10})
i = bot.intern_info(db, 10)
check(i['done'] == 0 and i['left'] == 30 and not i['fin'], 'owner «заново» → sayaç bugünden')

act(1, {'action': 'salcat_intern', 'id': ct, 'days': 0})
check(bot.intern_info(db, 10) is None, 'kategoride kapatınca sayaç gizli')
act(1, {'action': 'salcat_intern', 'id': ct, 'days': 30})
check(bot.intern_info(db, 10) is not None, 'tekrar açılınca geri geliyor')

pay = bot.build_hash_payload(db, 10, 'Гуля')
check('intern=' in pay, 'payload intern taşıyor')
print('FAIL' if FAIL else 'ALL OK', FAIL)
