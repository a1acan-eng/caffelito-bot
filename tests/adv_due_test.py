# Tek parça avans vadesi = maaş günü, kazanç avansı karşılayınca (owner 2026-10-06).
# Çalıştırma: rm -f /tmp/advd.db; PYTHONPATH=. DB_PATH=/tmp/advd.db BOT_TOKEN=123:abc python tests/adv_due_test.py
import bot
from datetime import date
FAIL = []
def check(c, m):
    print(('  ok  ' if c else '  FAIL ') + m)
    if not c: FAIL.append(m)
D = 198000   # 22 000 × 9 saat
check(bot.adv_single_due(date(2026, 10, 5), 1800000, D) == date(2026, 10, 20), '5\'inde 1 800 000 → 20\'si (15\'i değil)')
check(bot.adv_single_due(date(2026, 10, 5), 300000, D) == date(2026, 10, 20), 'küçük tutar da sonraki maaş günü')
check(bot.adv_single_due(date(2026, 10, 10), 1800000, D) == date(2026, 10, 20), 'maaş günü alınırsa → sonraki maaş günü')
check(bot.adv_single_due(date(2026, 10, 11), 1800000, D) == date(2026, 10, 31), '11\'inde → ay sonu')
check(bot.adv_single_due(date(2026, 10, 25), 1800000, D) == date(2026, 11, 10), '25\'inde → sonraki ayın 10\'u')
check(bot.adv_single_due(date(2026, 10, 5), 2500000, D) == date(2026, 10, 31), 'bir dönemi aşan tutar → bir sonraki maaş günü')
check(bot.adv_single_due(date(2026, 10, 5), 1800000, 0) == date(2026, 10, 15), 'günlük kazanç bilinmiyorsa eski kural (+10)')

db = bot.get_db()
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized) VALUES (5,'Б','barista',1,1)")
db.commit()
bot.adv_day_pay = lambda db, uid: D
def mk(n, manual=0, total=1800000):
    aid = db.execute("INSERT INTO advances (user_id,month,amount,commission,total,status,n_inst,manual,step_days) "
                     "VALUES (5,'2026-10',?,0,?,'active',?,?,10)", (total, total, n, manual)).lastrowid
    bot.adv_write_schedule(db, db.execute("SELECT * FROM advances WHERE id=?", (aid,)).fetchone(), date(2026, 10, 5))
    return [r["due_date"] for r in db.execute("SELECT due_date FROM advance_inst WHERE advance_id=? ORDER BY n", (aid,))]
check(mk(1) == ['2026-10-20'], 'takvim: tek parça 20\'si')
check(mk(2) == ['2026-10-15', '2026-10-25'], '2 parça eskisi gibi +10/+20')
check(mk(1, manual=1) == ['2026-10-15'], 'özel avans kendi aralığıyla')
print('FAIL' if FAIL else 'ALL OK', FAIL)
