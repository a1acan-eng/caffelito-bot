# Ay sonu maaş raporu + bugünün Grafik vardiyaları «прогноз» olarak (owner 2026-10-31).
# Çalıştırma: rm -f /tmp/pf.db; PYTHONPATH=. DB_PATH=/tmp/pf.db BOT_TOKEN=123:abc python tests/payroll_fc_test.py
import json, asyncio, bot
from datetime import datetime, date, timedelta
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
    return getattr(ctx, 'result', None)
bot.get_db()
db = bot.get_db()
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized,branch_id) VALUES (1,'Owner','owner',1,1,1)")
for uid, nm in ((10, 'Бек'), (11, 'Убай'), (12, 'Али')):
    db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (?,?,'barista',1,1,1)", (uid, nm))
db.execute("INSERT OR REPLACE INTO branches (id,name,active) VALUES (1,'Magic',1)")
db.execute("INSERT OR REPLACE INTO branches (id,name,active) VALUES (2,'C5',1)")
for c, b, s, e in (('mgd', 1, '07:00', '17:00'), ('c5n', 2, '17:00', '03:00')):
    db.execute("INSERT OR REPLACE INTO shift_templates (code, branch_id, start_t, end_t, active) VALUES (?,?,?,?,1)", (c, b, s, e))
D = date(2026, 10, 31)
wk = (D - timedelta(days=D.weekday())).isoformat()
for uid, code in ((10, 'mgd'), (11, 'c5n'), (12, 'mgd')):
    db.execute("INSERT OR REPLACE INTO shift_grid (week_key, day, user_id, code) VALUES (?,?,?,?)", (wk, D.weekday(), uid, code))
db.commit()
# Али bugünkü vardiyasını zaten kapattı → prognoza girmez
st = datetime(2026, 10, 31, 7, 5)
db.execute("INSERT INTO shifts (user_id,hours,total,date,period,created_at,start_time,branch_id,shift_role,rate) VALUES (12,0,0,?,?,?,?,1,'barista',20000)",
           (st.strftime('%Y-%m-%d'), '2026-10', st.isoformat(), st.isoformat())); db.commit()
bot.end_shift(db, 12, {}, custom_end=(st + timedelta(hours=10)).isoformat())
fc = bot.payroll_forecast(db, '2026-10', today=D)
check(set(fc) == {10, 11}, f'yalnız çalışılmamış vardiyalar ({sorted(fc)})')
r10 = bot.barista_pay_info(db, 10, branch_id=1)['rate']
check(fc[10][0]['h'] > 0 and fc[10][0]['pay'] >= int(fc[10][0]['h'] * r10) and fc[10][0]['br'] == 'Magic', f'gündüz: saat × ücret ({fc[10][0]})')
check(fc[11][0]['s'] == '17:00' and fc[11][0]['e'] == '03:00' and fc[11][0]['br'] == 'C5', 'gece vardiyası (31→1 Kasım) Ekim\'e sayılır')
check(bot.payroll_forecast(db, '2026-11', today=D) == {}, 'başka ay için prognoz yok')
check(bot.payroll_forecast(db, '2026-10', today=date(2026, 10, 30)).get(10) is None, 'yalnız o günün planı')
# Убай vardiyayı açtı (henüz kapatmadı) → gerçek başlangıçtan plan bitişine
s2 = datetime(2026, 10, 31, 17, 30)
db.execute("INSERT INTO shifts (user_id,hours,total,date,period,created_at,start_time,branch_id,shift_role,rate) VALUES (11,0,0,?,?,?,?,2,'barista',20000)",
           (s2.strftime('%Y-%m-%d'), '2026-10', s2.isoformat(), s2.isoformat())); db.commit()
fc = bot.payroll_forecast(db, '2026-10', today=D)
check(fc[11][0]['s'] == '17:30' and fc[11][0]['open'] == 1, 'açık vardiya: 17:30 → 03:00')
# aç/kapa
check(not bot.payroll_fc_on(db), 'varsayılan kapalı')
r = act(10, {'action': 'payroll_fc_set', 'on': 1})
check(r and not r.get('ok') and not bot.payroll_fc_on(db), 'barista açamaz')
r = act(1, {'action': 'payroll_fc_set', 'on': 1})
check(r and r.get('ok') and bot.payroll_fc_on(db), 'owner açtı')
check(bot.build_payroll_all_pdf(db, '2026-10')[:4] == b'%PDF', 'PDF prognozla üretiliyor')
check(bot.build_payroll_decade_pdf(db, '2026-10', 3)[:4] == b'%PDF', 'on günlük PDF de prognozla üretiliyor')
r = act(1, {'action': 'payroll_fc_set', 'on': 0})
check(r and r.get('ok') and not bot.payroll_fc_on(db), 'owner kapattı')
check(bot.calc_summary(db, 10, '2026-10')['net'] == 0, 'prognoz maaşa (bakiyeye) YAZILMAZ')
# Grafik'te yok ama vardiyası açık (owner 2026-10-31: «Ubay bugün de çalışıyor») → pencereye göre prognoz
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (13,'Убай2','barista',1,1,1)")
cfg = bot.op_cfg(db); cfg.setdefault('windows', {})['1'] = [{'s': '07:00', 'e': '16:30'}, {'s': '16:00', 'e': '01:00'}]
bot.op_cfg_save(db, {'windows': cfg['windows']})
s3 = datetime(2026, 10, 31, 16, 5)
db.execute("INSERT INTO shifts (user_id,hours,total,date,period,created_at,start_time,branch_id,shift_role,rate) VALUES (13,0,0,?,?,?,?,1,'barista',20000)",
           (s3.strftime('%Y-%m-%d'), '2026-10', s3.isoformat(), s3.isoformat())); db.commit()
fc = bot.payroll_forecast(db, '2026-10', today=D)
check(13 in fc and fc[13][0]['s'] == '16:05' and fc[13][0]['e'] == '01:00' and fc[13][0]['open'] == 1,
      f'grafikte olmayan açık vardiya: 16:05 → 01:00 ({fc.get(13)})')
check(len(fc[11]) == 1, 'grafikteki açık vardiya iki kez sayılmaz')
# Transferler raporda adıyla
db.execute("INSERT INTO xfers (from_uid,to_uid,amount,note,period,created_at) VALUES (1,11,1530000,'','2026-10','2026-10-30T21:14:00')")
db.execute("INSERT INTO xfers (from_uid,to_uid,amount,note,period,created_at,status) VALUES (1,10,5000,'','2026-10','2026-10-30T21:20:00','canceled')")
db.commit()
xf = bot.payroll_xfers(db, '2026-10')
check(xf.get(1) == [{'at': '2026-10-30T21:14:00', 'who': 'для Убай', 'a': -1530000}], f'gönderen: для Убай −1.530.000 ({xf.get(1)})')
check(xf.get(11)[0]['a'] == 1530000 and xf.get(11)[0]['who'].startswith('от '), 'alan: + 1.530.000')
check(10 not in xf, 'iptal edilen transfer görünmez')
check(bot.build_payroll_all_pdf(db, '2026-10')[:4] == b'%PDF' and bot.build_payroll_decade_pdf(db, '2026-10', 3)[:4] == b'%PDF',
      'PDF transferlerle üretiliyor')
print('ALL OK' if not FAIL else 'FAILED: %s' % FAIL)
raise SystemExit(1 if FAIL else 0)
