# Owner da ara sıra çalışıyor (owner 2026-10-31): o ay vardiyası varsa «Зарплата всех» raporlarına girer;
# vardiyası/kazancı yoksa (yalnız ceza aktarımı vb.) girmez.
# Çalıştırma: rm -f /tmp/po.db; PYTHONPATH=. DB_PATH=/tmp/po.db BOT_TOKEN=123:abc python tests/payroll_owner_test.py
import bot
from datetime import datetime
FAIL = []
def check(c, m):
    print(('  ok  ' if c else '  FAIL ') + m)
    if not c: FAIL.append(m)
db = bot.get_db()
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized,branch_id) VALUES (1,'Owner','owner',1,1,1)")
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (10,'Дамир','barista',1,1,1)")
db.execute("INSERT OR REPLACE INTO branches (id, name, active) VALUES (1,'C5',1)")
per = bot.current_period(); now = datetime.now(bot.TZ).replace(tzinfo=None)
db.execute("INSERT INTO shifts (user_id,start_time,end_time,hours,hourly_pay,total,period,branch_id,date) VALUES (10,?,?,8,200000,200000,?,1,?)",
           (now.replace(hour=8).isoformat(), now.replace(hour=16).isoformat(), per, now.strftime('%Y-%m-%d')))
db.execute("INSERT INTO adjustments (user_id,amount,note,period,branch_id,added_by,added_by_name,created_at) VALUES (1,50000,'Из штрафов в зарплату [f2s]',?,1,1,'Owner',?)",
           (per, now.isoformat()))
db.commit()
ids = [p[0] for p in bot.payroll_people(db, per)]
check(10 in ids and 1 not in ids, 'vardiyası olmayan owner listede yok (yalnız aktarım)')
db.execute("INSERT INTO shifts (user_id,start_time,end_time,hours,hourly_pay,total,period,branch_id,date) VALUES (1,?,?,6,150000,150000,?,1,?)",
           (now.replace(hour=9).isoformat(), now.replace(hour=15).isoformat(), per, now.strftime('%Y-%m-%d')))
db.commit()
pp = {p[0]: p for p in bot.payroll_people(db, per)}
check(1 in pp and 10 in pp, 'o ay çalışan owner listede')
check(len(bot.build_payroll_all_pdf(db, per)) > 1000 and len(bot.build_payroll_decade_pdf(db, per, 3)) > 1000, 'PDF\'ler owner ile oluşuyor')
print('ALL OK' if not FAIL else 'FAILED: %s' % FAIL)
raise SystemExit(1 if FAIL else 0)
