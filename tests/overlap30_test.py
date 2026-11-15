# Devir 30 dk, iki vardiya eşit (owner seçimi B) + erken gelen bekleme ücreti almaz (owner 2026-11-15).
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
check(T['c5d'] == ('07:00', '17:15') and T['c5n'] == ('16:45', '03:00'), f'C5: 10 sa 15 dk + 10 sa 15 dk ({T})')
check(T['mgd'] == ('07:00', '16:15') and T['mgn'] == ('15:45', '01:00'), 'Magic: 9 sa 15 dk + 9 sa 15 dk')
check(T['mgf'] == ('07:00', '00:00'), 'tam gün değişmez')
W = bot.op_cfg(db)['windows']
check(W['1'] == [{'s': '07:00', 'e': '17:15'}, {'s': '16:45', 'e': '03:00'}] and W['2'][1]['s'] == '15:45', f'ceza pencereleri de ({W})')
check(len(ch) == 8, f'değişiklik listesi ({ch})')
check(bot.overlap30_migrate(db) == [] and {r['code']: r['start_t'] for r in db.execute("SELECT * FROM shift_templates")}['c5n'] == '16:45',
      'ikinci kez çalışmaz')
s0, e0 = bot.op_window_for(bot.op_cfg(db), 1, datetime(2026, 11, 20, 17, 0))
check(s0 == datetime(2026, 11, 20, 16, 45), f'17:00 geliş → 16:45 vardiyasına 15 dk geç ({s0})')
# canlıdaki durum (ilk sürüm çalıştı: 17:30 / 17:00, devir 30) → aynı sonuç
db.execute("DELETE FROM meta WHERE k='overlap_split_mig'")
db.execute("UPDATE shift_templates SET end_t='17:30' WHERE code='c5d'"); db.execute("UPDATE shift_templates SET start_t='17:00' WHERE code='c5n'")
db.commit(); bot.overlap30_migrate(db)
T = {r['code']: (r['start_t'], r['end_t']) for r in db.execute("SELECT * FROM shift_templates")}
check(T['c5d'][1] == '17:15' and T['c5n'][0] == '16:45' and T['mgn'][0] == '15:45', f'17:30/17:00 → 17:15/16:45 ({T})')

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
open_shift(11, D + timedelta(hours=16, minutes=20))          # 25 dk erken geldi, Karina içeride
close(10, D + timedelta(hours=17, minutes=15))
r = close(11, D + timedelta(hours=26))                        # 02:00
want = bot.paid_hours(D + timedelta(hours=16, minutes=45), D + timedelta(hours=26), pw)
check(r['pay_start'] and r['pay_start'].startswith('2026-11-20T16:45') and abs(r['hours'] - want) < 0.01,
      f'erken gelen: ödeme 16:45\'ten ({r["pay_start"]}, {r["hours"]} ≈ {want:.2f})')
check(r['start_time'].startswith('2026-11-20T16:20'), '«Пришли» gerçek saat kalır')
# önceki kişi izinle erken çıktı (16:30) → 16:30'dan itibaren tek başına → ödenir
D2 = D + timedelta(days=1)
grid(10, D2.date(), 'c5d'); grid(11, D2.date(), 'c5n')
open_shift(10, D2 + timedelta(hours=7))
open_shift(11, D2 + timedelta(hours=16, minutes=15))
close(10, D2 + timedelta(hours=16, minutes=30))
r = close(11, D2 + timedelta(hours=26))
check(r['pay_start'] and r['pay_start'].startswith('2026-11-21T16:30'), f'önceki erken çıktı: onun çıkışından ödenir ({r["pay_start"]})')
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
open_shift(11, D4 + timedelta(hours=16, minutes=50))
close(10, D4 + timedelta(hours=17, minutes=15))
r = close(11, D4 + timedelta(hours=26))
check(r['pay_start'] is None, 'zamanında/geç gelen: dokunulmaz')
# paralel ikinci barista (par_of) erken katılırsa kesilmez
D5 = D + timedelta(days=4)
grid(10, D5.date(), 'c5d'); grid(12, D5.date(), 'c5n')
open_shift(10, D5 + timedelta(hours=7))
root = db.execute("SELECT id FROM shifts WHERE user_id=10 AND end_time IS NULL").fetchone()[0]
open_shift(12, D5 + timedelta(hours=16, minutes=30))
db.execute("UPDATE shifts SET par_of=? WHERE user_id=12 AND end_time IS NULL", (root,)); db.commit()
close(10, D5 + timedelta(hours=17, minutes=15))
r = close(12, D5 + timedelta(hours=26))
check(r['pay_start'] is None, 'paralel grup üyesi önceki sayılmaz')
# canlı sayaç: açık vardiyada aynı kural (owner 2026-11-15: «hâlâ yazıyor»)
D6 = D + timedelta(days=5)
grid(10, D6.date(), 'c5d'); grid(11, D6.date(), 'c5n')
open_shift(10, D6 + timedelta(hours=7))
open_shift(11, D6 + timedelta(hours=16, minutes=17))
sh = db.execute("SELECT * FROM shifts WHERE user_id=11 AND end_time IS NULL").fetchone()
check((bot.shift_pay_from_live(db, sh) or '').startswith(D6.strftime('%Y-%m-%d') + 'T16:45'), f'açık vardiya: canlı ödeme 16:45 ({bot.shift_pay_from_live(db, sh)})')
s = bot.calc_summary(db, 11, D6.strftime('%Y-%m'))
check(((s.get('active') or {}).get('pay_from') or '').endswith('16:45:00'), 'özet: active.pay_from')
# owner istisnası: «оплатить с прихода» (owner 2026-11-15, Ubay)
import json as _j, asyncio as _a
class _FB:
    async def send_message(self, *a, **k): return None
    def __getattr__(self, n):
        async def f(*a, **k): return None
        return f
def act(uid, p):
    ctx = bot._ShimContext(_FB(), {}, uid=uid)
    _a.run(bot.handle_webapp_data(bot._ShimUpdate(_FB(), uid, f'U{uid}', None, _j.dumps(p)), ctx))
    return getattr(ctx, 'result', None)
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized,branch_id) VALUES (1,'Owner','owner',1,1,1)"); db.commit()
r = act(11, {'action': 'shift_pay_full', 'id': sh['id']})
check(r and not r.get('ok'), 'barista istisna koyamaz')
act(1, {'action': 'shift_pay_full', 'id': sh['id']})
sh = db.execute("SELECT * FROM shifts WHERE id=?", (sh['id'],)).fetchone()
check(bot.shift_pay_from_live(db, sh) is None, 'istisna: canlı sayaç gelişten')
close(10, D6 + timedelta(hours=17, minutes=15)); r = close(11, D6 + timedelta(hours=26))
check(r['pay_start'] is None, 'istisna: kapanışta gelişten ödendi')
act(1, {'action': 'shift_pay_full', 'id': sh['id'], 'on': 0})
r = db.execute("SELECT * FROM shifts WHERE id=?", (sh['id'],)).fetchone()
check((r['pay_start'] or '').endswith('16:45:00') and not r['pay_full'], f'kurala dönüş: kapalı vardiya yeniden hesaplandı ({r["pay_start"]}, {r["hours"]})')
act(1, {'action': 'shift_pay_full', 'id': sh['id'], 'on': 1})
r2 = db.execute("SELECT * FROM shifts WHERE id=?", (sh['id'],)).fetchone()
check(r2['pay_start'] is None and r2['hours'] > r['hours'], f'kapalı vardiyada istisna: saat arttı ({r["hours"]} → {r2["hours"]})')
print('ALL OK' if not FAIL else 'FAILED: %s' % FAIL)
raise SystemExit(1 if FAIL else 0)
