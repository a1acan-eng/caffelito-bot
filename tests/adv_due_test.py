# Taksit günü: kazanmadan kesilmez (owner 2026-10-06).
# Çalıştırma: rm -f /tmp/advd.db; PYTHONPATH=. DB_PATH=/tmp/advd.db BOT_TOKEN=123:abc python tests/adv_due_test.py
import bot
from datetime import date, timedelta
FAIL = []
def check(c, m):
    print(('  ok  ' if c else '  FAIL ') + m)
    if not c: FAIL.append(m)
D = 198000   # 22 000 × 9 saat
_orig_day_pay = bot.adv_day_pay
sd = lambda start, k, amt, bk=None: bot.adv_safe_due(start + timedelta(days=10 * k), amt, D, bk)
o = date(2026, 10, 5)
check(sd(o, 1, 1782000) == date(2026, 10, 19), 'tek parça, 5\'i → 15 değil 19 (9 gün kazanç)')
check(sd(o, 1, 772200) == date(2026, 10, 15), '3 parçanın parçası (4 gün) → 15 kaymaz')
check(sd(o, 1, 1069200) == date(2026, 10, 16), '2 parçanın parçası (6 gün) → 16')
t = date(2026, 10, 12)
check(sd(t, 1, 1782000) == date(2026, 10, 29), '12\'si tek parça → 29')
check([sd(t, k, 772200) for k in (1, 2, 3)] == [date(2026, 10, 24), date(2026, 11, 4), date(2026, 11, 14)], '12\'si 3 parça → 24 / 4 / 14')
check(sd(date(2026, 10, 9), 1, 1782000) == date(2026, 10, 19), '9\'u tek parça → 19 (kaymaz)')
check(bot.adv_safe_due(date(2026, 10, 15), 500000, 0) == date(2026, 10, 15), 'günlük kazanç bilinmiyorsa gün aynen')
# Aynı döneme düşen başka taksit birlikte karşılanır.
check(bot.adv_safe_due(date(2026, 10, 12), 300000, 100000, {'2026-10-11': 200000}) == date(2026, 10, 15), 'aynı dönemdeki diğer taksit hesaba katılır')

db = bot.get_db()
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized) VALUES (5,'Б','barista',1,1)")
db.commit()
bot.adv_day_pay = lambda db, uid: D
def mk(n, total, start, manual=0):
    aid = db.execute("INSERT INTO advances (user_id,month,amount,commission,total,status,n_inst,manual,step_days) "
                     "VALUES (5,'2026-10',?,0,?,'active',?,?,10)", (total, total, n, manual)).lastrowid
    bot.adv_write_schedule(db, db.execute("SELECT * FROM advances WHERE id=?", (aid,)).fetchone(), start)
    db.commit()
    return [r["due_date"] for r in db.execute("SELECT due_date FROM advance_inst WHERE advance_id=? ORDER BY n", (aid,))]
check(mk(1, 1782000, o) == ['2026-10-19'], 'takvim: tek parça 19')
db.execute("DELETE FROM advance_inst"); db.execute("DELETE FROM advances"); db.commit()
check(mk(3, 2316600, o) == ['2026-10-15', '2026-10-25', '2026-11-04'], 'takvim: 3 parça kaymıyor')
db.execute("DELETE FROM advance_inst"); db.execute("DELETE FROM advances"); db.commit()
# Owner'ın gerçek örneği: 4'ünde elle 200 000 (14'ü), 6'sında 300 000 → 16'sı; 11–16 arası kazanç ikisini karşılıyor.
check(mk(1, 200000, date(2026, 10, 4), manual=1) == ['2026-10-14'], 'elle 200 000 → 14')
check(mk(1, 300000, date(2026, 10, 6), manual=1) == ['2026-10-16'], 'ikinci 300 000 → 16 (yeterli kazanç)')
bot.adv_day_pay = lambda db, uid: 60000
db.execute("DELETE FROM advance_inst"); db.execute("DELETE FROM advances"); db.commit()
mk(1, 200000, date(2026, 10, 4), manual=1)
check(mk(1, 300000, date(2026, 10, 6), manual=1) == ['2026-10-19'], 'az kazanç (60 000/gün): 500 000 için 9 gün → 19')
# Rezerv açıksa günlük kazançtan rezerv katkısı düşer (owner 2026-10-06).
bot.adv_day_pay = _orig_day_pay
bot.adv_salary_info = lambda db, uid: {'salary': D * 30}
RES = {'live': 1, 'eligible': 1, 'off': 0, 'status': 'in_progress', 'per_day': 59400, 'remaining': 2000000}
bot.reserve_info = lambda db, uid: dict(RES)
check(bot.adv_day_pay(db, 5) == D - 59400, 'rezerv açık → günlük kazanç − rezerv katkısı')
dp_r = bot.adv_day_pay(db, 5)
check(bot.adv_safe_due(date(2026, 10, 15), 900000, D) == date(2026, 10, 15), 'rezerv kapalı: 900 000 → 15 (5 gün yeter)')
check(bot.adv_safe_due(date(2026, 10, 15), 900000, dp_r) == date(2026, 10, 17), 'rezerv açık: 900 000 → 17 (7 gün)')
check(bot.adv_safe_due(date(2026, 10, 15), 1782000, dp_r) == date(2026, 10, 20), 'dönem kazancı yetmezse → maaş günü (20), sonsuz döngü yok')
for st in ('completed', 'off', 'cancelled'):
    RES['status'] = st
    check(bot.adv_day_pay(db, 5) == D, f'rezerv {st} → tam kazanç')
RES.update(status='in_progress', off=1)
check(bot.adv_day_pay(db, 5) == D, 'kişiye rezerv kapalı → tam kazanç')
RES.update(off=0, eligible=0)
check(bot.adv_day_pay(db, 5) == D, 'stajyer (rezerv yok) → tam kazanç')
RES.update(eligible=1, remaining=10000)
check(bot.adv_day_pay(db, 5) == D - 10000, 'rezervin son günü: yalnız kalan tutar düşer')
print('FAIL' if FAIL else 'ALL OK', FAIL)
