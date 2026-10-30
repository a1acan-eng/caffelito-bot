# Borç kapatma (owner 2026-10-31): avansı şimdi maaştan kapat; misafir=çalışan Nero ID ile bağlanır,
# misafir borcu maaştan düşülür.
# Çalıştırma: rm -f /tmp/dc.db; PYTHONPATH=. DB_PATH=/tmp/dc.db BOT_TOKEN=123:abc python tests/debt_close_test.py
import json, asyncio, bot
from datetime import datetime, timedelta
FAIL = []
def check(c, m):
    print(('  ok  ' if c else '  FAIL ') + m)
    if not c: FAIL.append(m)
SENT = []
class FB:
    async def send_message(self, chat_id=None, text=None, *a, **k): SENT.append((chat_id, text)); return None
    def __getattr__(self, n):
        async def f(*a, **k): return None
        return f
def act(uid, p):
    ctx = bot._ShimContext(FB(), {}, uid=uid)
    asyncio.run(bot.handle_webapp_data(bot._ShimUpdate(FB(), uid, f'U{uid}', None, json.dumps(p)), ctx))
    return getattr(ctx, 'result', None)
db = bot.get_db()
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized,branch_id) VALUES (1,'Owner','owner',1,1,1)")
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (10,'Дамир','barista',1,1,1)")
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (11,'Бек','barista',1,1,1)")
db.execute("INSERT OR REPLACE INTO branches (id, name, active) VALUES (1,'C5',1)")
per = bot.current_period(); today = bot._adv_today(); now = datetime.now(bot.TZ).replace(tzinfo=None)
db.execute("INSERT INTO shifts (user_id,start_time,end_time,hours,hourly_pay,total,period,branch_id,date) VALUES (10,?,?,8,2000000,2000000,?,1,?)",
           (now.replace(hour=8).isoformat(), now.replace(hour=16).isoformat(), per, now.strftime('%Y-%m-%d')))
aid = db.execute("INSERT INTO advances (user_id,month,amount,commission_pct,commission,total,status,created_at) VALUES (10,?,600000,0,0,600000,'active',?)",
                 (per, now.isoformat())).lastrowid
fut = today + timedelta(days=4)
db.execute("INSERT INTO advance_inst (advance_id,user_id,n,due_date,period) VALUES (?,10,1,?,?)", (aid, fut.isoformat(), fut.strftime('%Y-%m')))
db.execute("UPDATE advance_inst SET amount=600000")
db.commit()
net0 = bot.calc_summary(db, 10, per)["net"]
r = act(11, {'action': 'adv_payoff', 'target': 10})
check(r and not r.get('ok'), 'başkası başkasının avansını kapatamaz')
r = act(1, {'action': 'adv_payoff', 'target': 10})
check(r and r.get('ok') and r['amount'] == 600000, 'owner avansı şimdi kapattı (600.000)')
check(bot.calc_summary(db, 10, per)["net"] == net0 - 600000, 'bu ayın maaşından düştü')
check(db.execute("SELECT status FROM advances WHERE id=?", (aid,)).fetchone()[0] == 'done', 'avans «done»')
r = act(1, {'action': 'adv_payoff', 'target': 10})
check(r and not r.get('ok'), 'ikinci kez kapatılacak borç yok')
# misafir = çalışan
gid = db.execute("INSERT INTO guests (name,branch_id,created_at) VALUES ('Дамир',1,'x')").lastrowid
db.execute("INSERT INTO guest_tx (guest_id,branch_id,kind,amount,at) VALUES (?,1,'debt',450000,'x')", (gid,)); db.commit()
r = act(1, {'action': 'gdebt_from_salary', 'guest_id': gid, 'amount': 450000})
check(r and not r.get('ok'), 'bağlanmamış misafirden maaş düşülmez')
r = act(1, {'action': 'gdebt_guest_link', 'guest_id': gid, 'uid': 10})
check(r and r.get('ok') and db.execute("SELECT user_id FROM guests WHERE id=?", (gid,)).fetchone()[0] == 10, 'Nero ID ile bağlandı')
g = [x for x in bot.gdebt_view(db)["guests"] if x["id"] == gid][0]
check(g["uid"] == 10 and g["un"], 'görünümde çalışan adı/ID')
n1 = bot.calc_summary(db, 10, per)["net"]
r = act(1, {'action': 'gdebt_from_salary', 'guest_id': gid, 'amount': 500000})
check(r and not r.get('ok'), 'borçtan fazlası reddedildi')
r = act(1, {'action': 'gdebt_from_salary', 'guest_id': gid, 'amount': 450000})
check(r and r.get('ok') and r['left'] == 0, 'misafir borcu maaştan kapandı')
check(bot.calc_summary(db, 10, per)["net"] == n1 - 450000, 'maaştan 450.000 düştü')
g = [x for x in bot.gdebt_view(db)["guests"] if x["id"] == gid][0]
check(g["bal"] == 0, 'misafir bakiyesi 0')
check(any(c == 10 and 'Долг' in (t or '') for c, t in SENT), 'çalışana bildirim')
print('ALL OK' if not FAIL else 'FAILED: %s' % FAIL)
raise SystemExit(1 if FAIL else 0)
