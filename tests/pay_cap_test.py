# Plan bitişinden sonra sменщик geldiyse ödeme plan saatinde durur (owner 2026-11-15).
# Çalıştırma: rm -f /tmp/pc.db; PYTHONPATH=. DB_PATH=/tmp/pc.db BOT_TOKEN=123:abc python tests/pay_cap_test.py
import bot
from datetime import datetime, timedelta
FAIL = []
def check(c, m):
    print(('  ok  ' if c else '  FAIL ') + m)
    if not c: FAIL.append(m)
bot.get_db()
db = bot.get_db()
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized,branch_id) VALUES (1,'Owner','owner',1,1,1)")
for uid, nm in ((10, 'Karina'), (11, 'Ubay'), (12, 'Damir')):
    db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (?,?,'barista',1,1,1)", (uid, nm))
db.execute("INSERT OR REPLACE INTO branches (id,name,active) VALUES (1,'C5',1)")
db.execute("INSERT OR REPLACE INTO branches (id,name,active) VALUES (2,'Magic',1)")
for c, b, s, e in (('c5d', 1, '07:00', '17:30'), ('c5n', 1, '16:30', '03:00')):
    db.execute("INSERT OR REPLACE INTO shift_templates (code, branch_id, start_t, end_t, active) VALUES (?,?,?,?,1)", (c, b, s, e))
db.commit()
def grid(uid, d, code):
    wk = (d - timedelta(days=d.weekday())).isoformat()
    db.execute("INSERT OR REPLACE INTO shift_grid (week_key, day, user_id, code) VALUES (?,?,?,?)", (wk, d.weekday(), uid, code)); db.commit()
def open_shift(uid, st, bid=1):
    db.execute("INSERT INTO shifts (user_id,hours,total,date,period,created_at,start_time,branch_id,shift_role,rate) VALUES (?,0,0,?,?,?,?,?,'barista',12000)",
               (uid, st.strftime('%Y-%m-%d'), st.strftime('%Y-%m'), st.isoformat(), st.isoformat(), bid)); db.commit()
    return db.execute("SELECT max(id) FROM shifts").fetchone()[0]
def close(uid, at):
    return bot.end_shift(db, uid, {}, custom_end=at.isoformat())
pw = lambda bid=1: bot.branch_pay_window(db, bid)

# 1) Karina: plan 07:00–17:30, Ubay 16:30'da geldi, Karina 17:51'de kapattı → 17:30'a kadar
D = datetime(2026, 11, 14)
grid(10, D.date(), 'c5d'); grid(11, D.date(), 'c5n')
open_shift(10, D + timedelta(hours=6, minutes=52))
open_shift(11, D + timedelta(hours=16, minutes=30))
r = close(10, D + timedelta(hours=17, minutes=51))
want = bot.paid_hours(D + timedelta(hours=6, minutes=52), D + timedelta(hours=17, minutes=30), pw())
check(r['end_time'].startswith('2026-11-14T17:51') and abs(r['hours'] - want) < 0.01 and r['pay_end'].startswith('2026-11-14T17:30') and r['pay_by'] == 11,
      f"sменщик geldi: ödeme 17:30'da durur, çıkış 17:51 kalır ({r['hours']} ≈ {want:.2f})")
check(r['hourly_pay'] == int(want * 12000), 'para da 17:30\'a göre')

# 2) Sменщик hiç gelmedi → gerçek çıkışa kadar
D2 = D + timedelta(days=1)
grid(10, D2.date(), 'c5d')
open_shift(10, D2 + timedelta(hours=7))
r = close(10, D2 + timedelta(hours=19))
check(r['pay_end'] is None and abs(r['hours'] - bot.paid_hours(D2 + timedelta(hours=7), D2 + timedelta(hours=19), pw())) < 0.01,
      'sменщик gelmedi: kapanışa kadar ödenir')

# 3) Sменщик geç geldi (18:10) → onu bekleyen 18:10'a kadar alır, 18:40 değil
D3 = D + timedelta(days=2)
grid(10, D3.date(), 'c5d'); grid(11, D3.date(), 'c5n')
open_shift(10, D3 + timedelta(hours=7))
open_shift(11, D3 + timedelta(hours=18, minutes=10))
r = close(10, D3 + timedelta(hours=18, minutes=40))
check(r['pay_end'].startswith('2026-11-16T18:10'), f"sменщик geç geldi: geliş saatine kadar ({r['pay_end']})")

# 4) Plandan önce çıkış / plan içinde → dokunulmaz
D4 = D + timedelta(days=3)
grid(10, D4.date(), 'c5d'); grid(11, D4.date(), 'c5n')
open_shift(10, D4 + timedelta(hours=7))
open_shift(11, D4 + timedelta(hours=16, minutes=30))
r = close(10, D4 + timedelta(hours=17, minutes=20))
check(r['pay_end'] is None and r['end_time'].startswith('2026-11-17T17:20'), 'plan içinde çıkış: değişmez')

# 5) Başka şubede gelen kişi sayılmaz; planı olmayan vardiya sınırlanmaz
D5 = D + timedelta(days=4)
grid(10, D5.date(), 'c5d')
open_shift(10, D5 + timedelta(hours=7))
open_shift(12, D5 + timedelta(hours=16, minutes=30), bid=2)
r = close(10, D5 + timedelta(hours=19))
check(r['pay_end'] is None, 'başka şubedeki kişi sменщик sayılmaz')
D6 = D + timedelta(days=5)
open_shift(12, D6 + timedelta(hours=7))
open_shift(11, D6 + timedelta(hours=16))
r = close(12, D6 + timedelta(hours=20))
check(r['pay_end'] is None, 'grafikte planı yoksa sınır yok')
# 6) Rapor yükü pay_end taşır
import json
from urllib.parse import unquote
check(any(x for x in [db.execute("SELECT pay_end FROM shifts WHERE user_id=10 AND pay_end IS NOT NULL").fetchone()]), 'kayıt saklandı')
# Owner saati düzeltirse kural yine uygulanır
import json, asyncio
class FB:
    async def send_message(self, *a, **k): return None
    def __getattr__(self, n):
        async def f(*a, **k): return None
        return f
def act(uid, p):
    ctx = bot._ShimContext(FB(), {}, uid=uid)
    asyncio.run(bot.handle_webapp_data(bot._ShimUpdate(FB(), uid, f'U{uid}', None, json.dumps(p)), ctx))
    return getattr(ctx, 'result', None)
sid1 = db.execute("SELECT id FROM shifts WHERE user_id=10 AND start_time LIKE '2026-11-14T06:52%'").fetchone()[0]
act(1, {'action': 'edit_shift', 'id': sid1, 'start_time': '2026-11-14T07:00:00'})
r = db.execute("SELECT * FROM shifts WHERE id=?", (sid1,)).fetchone()
check(r['start_time'].startswith('2026-11-14T07:00') and r['pay_end'] and r['pay_end'].startswith('2026-11-14T17:30')
      and abs(r['hours'] - bot.paid_hours(D + timedelta(hours=7), D + timedelta(hours=17, minutes=30), pw())) < 0.01,
      f"owner düzeltmesinde sınır korunur ({r['start_time']} → {r['pay_end']}, {r['hours']})")
print('ALL OK' if not FAIL else 'FAILED: %s' % FAIL)
raise SystemExit(1 if FAIL else 0)
