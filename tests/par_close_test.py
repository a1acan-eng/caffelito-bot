# Paralel kapanış (owner 2026-10-24): aynı şubede iki barista — kasayı SON ÇIKAN kapatır,
# önce çıkan yalnız saat; bardak bonusu eşit bölünür (önce çıkanın payı maaşa «+»).
# Çalıştırma: rm -f /tmp/pc.db; PYTHONPATH=. DB_PATH=/tmp/pc.db BOT_TOKEN=123:abc python tests/par_close_test.py
import json, asyncio, bot
from datetime import datetime, timedelta
FAIL = []
def check(c, m):
    print(('  ok  ' if c else '  FAIL ') + m)
    if not c: FAIL.append(m)
class FB:
    async def send_message(self, chat_id=None, text=None, *a, **k): return None
    def __getattr__(self, n):
        async def f(*a, **k): return None
        return f
def act(uid, p):
    ctx = bot._ShimContext(FB(), {}, uid=uid)
    asyncio.run(bot.handle_webapp_data(bot._ShimUpdate(FB(), uid, f'U{uid}', None, json.dumps(p)), ctx))
    return ctx.result
db = bot.get_db()
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized) VALUES (1,'Owner','owner',1,1)")
for uid, nm in ((10, 'Бек'), (11, 'Убай'), (12, 'Асс')):
    db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (?,?,'barista',1,1,1)", (uid, nm))
db.execute("INSERT OR REPLACE INTO branches (id, name, active) VALUES (1,'C5',1)")
db.commit()
now = datetime.now(bot.TZ).replace(tzinfo=None)
def open_shift(uid, h=6, role='barista'):
    st = now - timedelta(hours=h)
    sid = db.execute("INSERT INTO shifts (user_id,hours,total,date,period,created_at,start_time,branch_id,shift_role) "
                     "VALUES (?,0,0,?,?,?,?,1,?)",
                     (uid, st.strftime('%Y-%m-%d'), st.strftime('%Y-%m'), st.isoformat(), st.isoformat(), role)).lastrowid
    db.commit(); return sid
def row(sid): return db.execute("SELECT * FROM shifts WHERE id=?", (sid,)).fetchone()
drinks = {"ml300": 20, "ml400": 10}
full = bot.calc_bonus(drinks, bot.get_prices(db))
check(full > 0, f'bardak bonusu hesaplanıyor ({full})')
s10, s11 = open_shift(10), open_shift(11, 5)
check([r["user_id"] for r in bot.par_open_partners(db, row(s11))] == [10], 'Убай için напарник = Бек')
check(bot._par_mate_names(db, 11) == ['Бек'], 'Nero\'ya напарник adı gidiyor')
# 1) Önce çıkan bardak sayarak kapatmaya çalışır → red
act(11, {'action': 'shift_end', 'drinks': drinks})
check(row(s11)["end_time"] is None, 'напарник açıkken bardaklı kapanış reddedildi')
# 2) Hafif kapanış
act(11, {'action': 'shift_end', 'drinks': {}, 'light': 1})
r11 = row(s11)
check(r11["end_time"] is not None and r11["par_light"] == 1 and (r11["bonus"] or 0) == 0, 'Убай yalnız saatle kapattı')
n_cr = db.execute("SELECT COUNT(*) FROM cashreports").fetchone()[0]
act(11, {'action': 'cash_report', 'cups': [], 'itogo': 0, 'daily_pay': 5000, 'branch_id': 1})
check(db.execute("SELECT COUNT(*) FROM cashreports").fetchone()[0] == n_cr, 'önce çıkanın kasa raporu yazılmadı')
# 3) Son kalan «hafif» isterse → red (artık kasayı o kapatır)
act(10, {'action': 'shift_end', 'drinks': {}, 'light': 1})
check(row(s10)["end_time"] is None, 'son kalan hafif kapatamaz')
# 4) Son kalan tam kapanış → bonus yarı yarıya
act(10, {'action': 'shift_end', 'drinks': drinks})
r10, r11 = row(s10), row(s11)
half = full // 2
check(r10["end_time"] is not None and (r10["bonus"] or 0) == half and r10["par_n"] == 2, f'Бек bonusu yarısı ({half})')
check((r11["bonus"] or 0) == half and r11["par_by"] == s10, 'Убай vardiyasına payı yazıldı')
adj = db.execute("SELECT amount, note FROM adjustments WHERE user_id=11").fetchall()
check([a["amount"] for a in adj] == [half] and 'поровну' in adj[0]["note"], 'Убай maaşına «+» kaydı')
check(bot.calc_summary(db, 11, r11["period"])["adjustments"] == half, 'maaş özetinde görünüyor')
# 5) Tekrar paylaştırma yok: yeni bir vardiya eski hafif kaydı yeniden bölmez
s10b = open_shift(10, 1)
act(10, {'action': 'shift_end', 'drinks': drinks})
check((row(s10b)["bonus"] or 0) == full and not row(s10b)["par_n"], 'tek başına kapanışta bonus tam')
# 6) Asistan напарник sayılmaz
s12 = open_shift(12, 3, role='assistant'); s10c = open_shift(10, 2)
check(bot.par_open_partners(db, row(s10c)) == [], 'asistan açıkken barista kapanışı tam')
act(10, {'action': 'shift_end', 'drinks': drinks})
check((row(s10c)["bonus"] or 0) == full, 'asistanla kapanışta bonus bölünmedi')
print('ALL OK' if not FAIL else 'FAILED: %s' % FAIL)
raise SystemExit(1 if FAIL else 0)
