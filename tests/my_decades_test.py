# Çalışan 10 günlük maaşlarını geçmiş aylar dahil görür (owner 2026-11-01).
# Çalıştırma: rm -f /tmp/md.db; PYTHONPATH=. DB_PATH=/tmp/md.db BOT_TOKEN=123:abc python tests/my_decades_test.py
import bot
from datetime import datetime, date, timedelta
from urllib.parse import parse_qs
import json
FAIL = []
def check(c, m):
    print(('  ok  ' if c else '  FAIL ') + m)
    if not c: FAIL.append(m)
bot.get_db()
db = bot.get_db()
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized,branch_id) VALUES (1,'Owner','owner',1,1,1)")
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (10,'Хасан','barista',1,1,1)")
db.execute("INSERT OR REPLACE INTO branches (id,name,active) VALUES (1,'Magic',1)")
db.commit()
def sh(d, h, mon=10):
    st = datetime(2026, mon, d, 7, 0)
    db.execute("INSERT INTO shifts (user_id,hours,total,date,period,created_at,start_time,branch_id,shift_role,rate) VALUES (10,0,0,?,?,?,?,1,'barista',20000)",
               (st.strftime('%Y-%m-%d'), st.strftime('%Y-%m'), st.isoformat(), st.isoformat())); db.commit()
    bot.end_shift(db, 10, {}, custom_end=(st + timedelta(hours=h)).isoformat())
sh(3, 10); sh(15, 10); sh(25, 10); sh(29, 5, 9)
for pa, amt in (('2026-10-11T12:00:00', 200000), ('2026-10-21T12:00:00', 150000)):
    db.execute("INSERT INTO payments (user_id,amount,period,paid_at,kind) VALUES (10,?,?,?,'salary')", (amt, '2026-10', pa))
db.commit()
md = bot.my_decades(db, 10, today=date(2026, 11, 1))
ps = [m['p'] for m in md]
check(ps[:1] == ['2026-10'] and '2026-09' in ps and '2026-11' not in ps, f'geçen aylar görünür, boş ay yok ({ps})')
o = md[0]
check([r['k'] for r in o['rows']] == [1, 2, 3], 'Ekim: üç dönem')
w = bot.barista_pay_info(db, 10, branch_id=1)['rate']
check(all(r['net'] > 0 for r in o['rows']), f'her dönemde kazanılan ({[r["net"] for r in o["rows"]]})')
check(o['rows'][0]['pays'] == [{'d': '11.10', 'a': 200000}] and o['rows'][1]['pays'] == [{'d': '21.10', 'a': 150000}]
      and o['rows'][2]['pays'] == [], '11.10 ödemesi 1–10 maaşı, 21.10 ödemesi 11–20 maaşı; 21–31 henüz ödenmedi')
net = bot.calc_summary(db, 10, '2026-10')['net']
check(o['paid'] == 350000 and o['due'] == net, f'ay: ödenen 350.000, kalan = maaş neti ({o["due"]} / {net})')
check(o['closed'] == 1, 'Ekim kapandı')
md2 = bot.my_decades(db, 10, today=date(2026, 10, 16))
check(md2[0]['rows'][-1]['cur'] == 1 and len(md2[0]['rows']) == 2, 'içinde bulunulan dönem «идёт сейчас», gelecekteki dönem yok')
pl = bot.build_hash_payload(db, 10, 'Хасан')
check('mydec=' in pl, 'payload\'da mydec')
print('ALL OK' if not FAIL else 'FAILED: %s' % FAIL)
raise SystemExit(1 if FAIL else 0)
