# Staj bitti → owner kategori seçer ya da uzatır; otomatik geçiş YOK (owner 2026-11-02).
# Çalıştırma: rm -f /tmp/ip.db; PYTHONPATH=. DB_PATH=/tmp/ip.db BOT_TOKEN=123:abc python tests/intern_promote_test.py
import asyncio, json, bot
FAIL = []
def check(c, m):
    print(('  ok  ' if c else '  FAIL ') + m)
    if not c: FAIL.append(m)
bot.get_db()
db = bot.get_db()
sent = []
class FB:
    async def send_message(self, chat_id=None, text=None, *a, **k): sent.append((chat_id, text, k.get('reply_markup')))
    def __getattr__(self, n):
        async def f(*a, **k): return None
        return f
def act(uid, p):
    ctx = bot._ShimContext(FB(), {}, uid=uid)
    asyncio.run(bot.handle_webapp_data(bot._ShimUpdate(FB(), uid, f'U{uid}', None, json.dumps(p)), ctx))
    return getattr(ctx, 'result', None)
cb2 = db.execute("INSERT INTO salary_categories (name,hourly_rate,active) VALUES ('Бариста 2',26000,1)").lastrowid
cb1 = db.execute("INSERT INTO salary_categories (name,hourly_rate,active) VALUES ('Бариста 1',22000,1)").lastrowid
ct = db.execute("INSERT INTO salary_categories (name,hourly_rate,active,intern_days,next_cat_id) VALUES ('Стажёр',20000,1,30,?)", (cb1,)).lastrowid
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized) VALUES (1,'Owner','owner',1,1)")
db.execute("INSERT OR REPLACE INTO users (user_id,name,display_name,role,branch_id,salary_cat_id,approved,authorized) "
           "VALUES (10,'Akmal','Akmal','barista',1,?,1,1)", (ct,))
for k in range(30):
    db.execute("INSERT INTO shifts (user_id,hours,date,period,created_at) VALUES (10,8,?,?,?)",
               (f'2026-10-{k+1:02d}', '2026-10', f'2026-10-{k+1:02d}T17:00:00'))
# ESKİ biçimle bir kez bildirilmiş (canlıdaki durum) → yeni düğmeli mesaj yine gelir
db.execute("INSERT OR REPLACE INTO meta (k,val) VALUES ('intern_done_10_2026-10-01','30')")
db.commit()
opts = bot.intern_cat_options(db, 10)
check([o['id'] for o in opts][:1] == [cb1] and opts[0]['rec'] == 1 and ct not in [o['id'] for o in opts],
      'önerilen = stajyer kategorisinin «следующая»sı, stajyer kategorisi listede yok')
asyncio.run(bot.intern_scan(FB(), db))
dm = [x for x in sent if x[0] == 1]
check(len(dm) == 1 and dm[0][2] is not None, 'owner\'a düğmeli DM (eski bildirim olsa da)')
cbs = [b.callback_data for row in dm[0][2].inline_keyboard for b in row]
check(f'int_cat:10:{cb1}' in cbs and 'int_ext:10:7' in cbs and 'int_ext:10:30' in cbs, f'düğmeler: kategori + 7/15/30 ({cbs})')
sent.clear(); asyncio.run(bot.intern_scan(FB(), db))
check(not sent, 'ikinci kez gelmez')
check(bot.barista_pay_info(db, 10)['cat_id'] == ct, 'owner seçmeden kategori DEĞİŞMEZ')
# uzat (elle 10 gün)
r = act(10, {'action': 'intern_extend', 'target': 10, 'days': 10})
check(r and not r.get('ok'), 'çalışan uzatamaz')
r = act(1, {'action': 'intern_extend', 'target': 10, 'days': 10})
i = bot.intern_info(db, 10)
check(r and r.get('ok') and i['total'] == 40 and i['left'] == 10 and not i['fin'] and i['extra'] == 10, f'10 gün uzatıldı ({i})')
check(any(c == 10 and 'продлена' in t for c, t, _ in sent), 'çalışana bildirim')
r = act(1, {'action': 'intern_extend', 'target': 10, 'days': 0})
check(r and not r.get('ok'), '0 gün reddedilir')
for k in range(10):
    db.execute("INSERT INTO shifts (user_id,hours,date,period,created_at) VALUES (10,8,?,?,?)",
               (f'2026-11-{k+1:02d}', '2026-11', f'2026-11-{k+1:02d}T17:00:00'))
db.commit()
sent.clear(); asyncio.run(bot.intern_scan(FB(), db))
check(len([x for x in sent if x[0] == 1]) == 1, 'uzatma da dolunca yeniden sorulur')
# kategoriye geçir
r = act(1, {'action': 'intern_promote', 'target': 10, 'cat_id': ct})
check(r and not r.get('ok'), 'stajyer kategorisine «geçirme» reddedilir')
r = act(1, {'action': 'intern_promote', 'target': 10, 'cat_id': cb1})
check(r and r.get('ok') and bot.barista_pay_info(db, 10)['cat_id'] == cb1 and bot.barista_pay_info(db, 10)['rate'] == 22000,
      'Бариста 1 · 22 000 oldu')
check(bot.intern_info(db, 10) is None, 'staj bitti — sayaç yok')
check(any(c == 10 and 'Стажировка завершена' in t for c, t, _ in sent), 'çalışana tebrik')
check(db.execute("SELECT 1 FROM logs WHERE action='salcat_assign'").fetchone() is not None, 'günlükte')
print('ALL OK' if not FAIL else 'FAILED: %s' % FAIL)
raise SystemExit(1 if FAIL else 0)
