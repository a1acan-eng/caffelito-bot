# CPS kesintisi ödeme ekranında (owner 2026-10-30): kesinti ELLE uygulanır; uygulanmamışsa
# çalışan verisinde «cpsp» uyarısı vardır, uygulanınca kaybolur ve net düşer.
# Çalıştırma: rm -f /tmp/cp.db; PYTHONPATH=. DB_PATH=/tmp/cp.db BOT_TOKEN=123:abc python tests/cps_pay_test.py
import json, asyncio, bot
from datetime import datetime
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
db = bot.get_db()
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized,branch_id) VALUES (1,'Owner','owner',1,1,1)")
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (10,'Бот','barista',1,1,1)")
db.execute("INSERT OR REPLACE INTO branches (id, name, active) VALUES (1,'C5',1)")
per = bot.current_period(); now = datetime.now(bot.TZ).replace(tzinfo=None)
db.execute("INSERT INTO shifts (user_id,start_time,end_time,hours,hourly_pay,total,period,branch_id,date) VALUES (10,?,?,8,1000000,1000000,?,1,?)",
           ((now.replace(hour=8)).isoformat(), (now.replace(hour=16)).isoformat(), per, now.strftime('%Y-%m-%d')))
db.execute("INSERT INTO cps_events (user_id,period,kind,title,delta,added_by,created_at) VALUES (10,?,'violation','т',-40,1,?)",
           (per, now.isoformat()))
db.commit()
check(bot.cps_score(db, 10, per) == 60, 'CPS 60')
p = bot._cps_pending(db, 10, per)
check(p == {"sc": 60, "pct": 25, "amt": 250000}, f'uygulanmamış kesinti uyarısı: %25 = 250.000 {p}')
net0 = bot.calc_summary(db, 10, per)["net"]
act(1, {'action': 'cps_deduct_apply', 'user_id': 10, 'period': per})
check(bot._cps_pending(db, 10, per) is None, 'uygulanınca uyarı kalktı')
check(bot.calc_summary(db, 10, per)["net"] == net0 - 250000, 'net 250.000 düştü')
db.execute("UPDATE cps_events SET delta=-10"); db.commit()
check(bot._cps_pending(db, 10, per) is None, 'CPS 90 → kesinti yok')
print('ALL OK' if not FAIL else 'FAILED: %s' % FAIL)
raise SystemExit(1 if FAIL else 0)
