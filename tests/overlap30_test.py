# Devir 1 saat → 30 dk + erken gelen bekleme ücreti almaz (owner 2026-11-15).
# Çalıştırma: rm -f /tmp/ov.db; PYTHONPATH=. DB_PATH=/tmp/ov.db BOT_TOKEN=123:abc python tests/overlap30_test.py
import json, bot
from datetime import datetime, timedelta
FAIL = []
def check(c, m):
    print(('  ok  ' if c else '  FAIL ') + m)
    if not c: FAIL.append(m)
bot.get_db()
db = bot.get_db()
db.execute("DELETE FROM shift_templates")
for uid, nm in ((10, 'Karina'), (11, 'Ubay'), (12, 'Damir')):
    db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (?,?,'barista',1,1,1)", (uid, nm))
db.execute("INSERT OR REPLACE INTO branches (id,name,active) VALUES (1,'C5',1)")
db.execute("INSERT OR REPLACE INTO branches (id,name,active) VALUES (2,'Magic',1)")
for c, b, s, e in (('c5d', 1, '07:00', '17:30'), ('c5n', 1, '16:30', '03:00'),
                   ('mgd', 2, '07:00', '16:30'), ('mgn', 2, '15:30', '01:00'), ('mgf', 2, '07:00', '00:00')):
    db.execute("INSERT OR REPLACE INTO shift_templates (code, branch_id, start_t, end_t, active) VALUES (?,?,?,?,1)", (c, b, s, e))
db.commit()
bot.op_cfg_save(db, {'windows': {'1': [{'s': '07:00', 'e': '17:30'}, {'s': '16:30', 'e': '03:00'}],
                                 '2': [{'s': '07:00', 'e': '16:30'}, {'s': '15:30', 'e': '01:00'}]}})
# ── 1) geçiş ──
ch = bot.overlap30_migrate(db)
T = {r['code']: (r['start_t'], r['end_t']) for r in db.execute("SELECT * FROM shift_templates")}
check(T['c5n'] == ('17:00', '03:00') and T['mgn'] == ('16:00', '01:00'), f'akşam şablonları 30 dk ileri ({T})')
check(T['c5d'] == ('07:00', '17:30') and T['mgd'] == ('07:00', '16:30') and T['mgf'] == ('07:00', '00:00'), 'gündüz ve tam gün değişmez')
W = bot.op_cfg(db)['windows']
check(W['1'][1]['s'] == '17:00' and W['2'][1]['s'] == '16:00' and W['1'][0]['s'] == '07:00', f'ceza pencereleri de ({W})')
check(len(ch) == 4, f'değişiklik listesi ({ch})')
check(bot.overlap30_migrate(db) == [] and {r['code']: r['start_t'] for r in db.execute("SELECT * FROM shift_templates")}['c5n'] == '17:00',
      'ikinci kez çalışmaz')
s0, e0 = bot.op_window_for(bot.op_cfg(db), 1, datetime(2026, 11, 20, 17, 10))
check(s0 == datetime(2026, 11, 20, 17, 0), f'17:10 geliş → 17:00 vardiyasına 10 dk geç ({s0})')

# ── 2) erken gelen bekleme ücreti almaz ──
def grid(uid, d, code):
    wk = (d - timedelta(days=d.weekday())).isoformat()
    db.execute("INSERT OR REPLACE INTO shift_grid (week_key, day, user_id, code) VALUES (?,?,?,?)", (wk, d.weekday(), uid, code)); db.commit()
def open_shift(uid, st, bid=1):
    db.execute("INSERT INTO shifts (user_id,hours,total,date,period,created_at,start_time,branch_id,shift_role,rate) VALUES (?,0,0,?,?,?,?,?,'barista',12000)",
               (uid, st.strftime('%Y-%m-%d'), st.strftime('%Y-%m'), st.isoformat(), st.isoformat(), bid)); db.commit()
close = lambda uid, at: bot.end_shift(db, uid, {}, custom_end=at.isoformat())
pw = bot.branch_pay_window(db, 1)
D = datetime(2026, 11, 20)
grid(10, D.date(), 'c5d'); grid(11, D.date(), 'c5n')
open_shift(10, D + timedelta(hours=7))
open_shift(11, D + timedelta(hours=16, minutes=40))          # 20 dk erken geldi, Karina içeride
close(10, D + timedelta(hours=17, minutes=30))
r = close(11, D + timedelta(hours=26))                        # 02:00
want = bot.paid_hours(D + timedelta(hours=17), D + timedelta(hours=26), pw)
check(r['pay_start'] and r['pay_start'].startswith('2026-11-20T17:00') and abs(r['hours'] - want) < 0.01,
      f'erken gelen: ödeme 17:00\'dan ({r["pay_start"]}, {r["hours"]} ≈ {want:.2f})')
check(r['start_time'].startswith('2026-11-20T16:40'), '«Пришли» gerçek saat kalır')
# önceki kişi izinle erken çıktı (16:50) → 16:50'den itibaren tek başına → ödenir
D2 = D + timedelta(days=1)
grid(10, D2.date(), 'c5d'); grid(11, D2.date(), 'c5n')
open_shift(10, D2 + timedelta(hours=7))
open_shift(11, D2 + timedelta(hours=16, minutes=35))
close(10, D2 + timedelta(hours=16, minutes=50))
r = close(11, D2 + timedelta(hours=26))
check(r['pay_start'] and r['pay_start'].startswith('2026-11-21T16:50'), f'önceki erken çıktı: onun çıkışından ödenir ({r["pay_start"]})')
# kimse yokken erken gelen (boş şube) → gerçek başlangıçtan
D3 = D + timedelta(days=2)
grid(11, D3.date(), 'c5n')
open_shift(11, D3 + timedelta(hours=16, minutes=40))
r = close(11, D3 + timedelta(hours=26))
check(r['pay_start'] is None, 'şubede kimse yoksa erken başlangıç ödenir')
# zamanında/geç gelen → değişiklik yok
D4 = D + timedelta(days=3)
grid(10, D4.date(), 'c5d'); grid(11, D4.date(), 'c5n')
open_shift(10, D4 + timedelta(hours=7))
open_shift(11, D4 + timedelta(hours=17, minutes=5))
close(10, D4 + timedelta(hours=17, minutes=30))
r = close(11, D4 + timedelta(hours=26))
check(r['pay_start'] is None, 'zamanında/geç gelen: dokunulmaz')
# paralel ikinci barista (par_of) erken katılırsa kesilmez
D5 = D + timedelta(days=4)
grid(10, D5.date(), 'c5d'); grid(12, D5.date(), 'c5n')
open_shift(10, D5 + timedelta(hours=7))
root = db.execute("SELECT id FROM shifts WHERE user_id=10 AND end_time IS NULL").fetchone()[0]
open_shift(12, D5 + timedelta(hours=16, minutes=30))
db.execute("UPDATE shifts SET par_of=? WHERE user_id=12 AND end_time IS NULL", (root,)); db.commit()
close(10, D5 + timedelta(hours=17, minutes=30))
r = close(12, D5 + timedelta(hours=26))
check(r['pay_start'] is None, 'paralel grup üyesi önceki sayılmaz')
print('ALL OK' if not FAIL else 'FAILED: %s' % FAIL)
raise SystemExit(1 if FAIL else 0)
