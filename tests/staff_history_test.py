# Çalışanın kendi geçmişi (owner 2026-10-27): bir ayın tüm hareketleri tek listede.
# Çalıştırma: rm -f /tmp/sh.db; PYTHONPATH=. DB_PATH=/tmp/sh.db BOT_TOKEN=123:abc python tests/staff_history_test.py
import json, asyncio, bot
from datetime import datetime, timedelta
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
    return ctx.result or {}
db = bot.get_db()
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized,branch_id) VALUES (1,'Owner','owner',1,1,1)")
for uid, nm in ((10, 'Бек'), (11, 'Убай')):
    db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (?,?,'barista',1,1,1)", (uid, nm))
db.execute("INSERT OR REPLACE INTO branches (id,name,active) VALUES (1,'C5',1)")
db.commit()
P = '2026-10'
st = datetime(2026, 10, 20, 7, 0)
db.execute("INSERT INTO shifts (user_id,hours,total,date,period,created_at,start_time,branch_id,shift_role,rate) VALUES (10,0,0,?,?,?,?,1,'barista',26000)",
           (st.strftime('%Y-%m-%d'), P, st.isoformat(), st.isoformat()))
db.commit()
bot.end_shift(db, 10, {"ml300": 12}, custom_end=(st + timedelta(hours=9)).isoformat())
db.execute("INSERT INTO cashreports (user_id,user_name,date,period,created_at,daily_pay,cups_total,branch_id) VALUES (10,'Бек','2026-10-20',?,?,12000,12,1)", (P, (st + timedelta(hours=9, minutes=1)).isoformat()))
db.execute("INSERT INTO fines (user_id,amount,reason,period,created_at) VALUES (10,20000,'опоздание',?,'2026-10-21T08:00:00')", (P,))
db.execute("INSERT INTO tips (user_id,amount,note,period,created_at) VALUES (10,5000,'гость',?,'2026-10-22T10:00:00')", (P,))
db.execute("INSERT INTO payments (user_id,amount,period,paid_by,paid_by_name,paid_at,kind,note) VALUES (10,300000,?,1,'Owner','2026-10-25T18:00:00','salary','за первую половину')", (P,))
aid = db.execute("INSERT INTO advances (user_id,month,amount,commission,total,status,created_at,decided_at) VALUES (10,?,200000,10000,210000,'active','2026-10-05T09:00:00','2026-10-05T10:00:00')", (P,)).lastrowid
db.execute("INSERT INTO advance_inst (advance_id,user_id,n,due_date,period,amount) VALUES (?,10,1,'2026-10-15',?,105000)", (aid, P))
db.execute("INSERT INTO advance_inst (advance_id,user_id,n,due_date,period,amount) VALUES (?,10,2,'2026-10-30',?,105000)", (aid, P))
db.execute("INSERT INTO fines (user_id,amount,reason,period,created_at) VALUES (10,99999,'другой месяц','2026-09','2026-09-10T08:00:00')")
db.execute("INSERT INTO fines (user_id,amount,reason,period,created_at) VALUES (11,77777,'чужой','2026-10','2026-10-10T08:00:00')")
db.execute("INSERT INTO adjustments (user_id,amount,note,period,created_at) VALUES (10,15000,'Перевод от Убай · за такси',?,'2026-10-23T12:00:00')", (P,))
db.commit()
r = act(1, {'action': 'staff_history', 'target': 10, 'period': P})
check(r.get('ok') is True and r['period'] == P, 'owner Бек\'in Ekim geçmişini alır')
ks = [x['k'] for x in r['items']]
for k, nm in (('shift', 'vardiya'), ('bonus', 'bardak bonusu'), ('cash', 'kasadan günlük bonus'), ('fine', 'ceza'),
              ('tip', 'чаевые'), ('pay', 'ödeme'), ('adv', 'avans'), ('inst', 'avans taksiti'), ('xfer', 'transfer')):
    check(k in ks, f'listede: {nm}')
sh = next(x for x in r['items'] if x['k'] == 'shift')
check(sh['a'] == 9 * 26000 and '07:00–16:00' in sh['s'], 'vardiya: saat aralığı ve ücret (234.000)')
check(next(x for x in r['items'] if x['k'] == 'cash')['i'] == 12000, 'kasadan alınan bonus 12.000')
ins = [x for x in r['items'] if x['k'] == 'inst']
check(len(ins) == 2 and all('удержано' in x['s'] for x in ins) and all(x['a'] == -105000 for x in ins), 'iki avans taksiti (−105.000) tarihleriyle görünüyor')
check(all('другой месяц' not in x['s'] and 'чужой' not in x['s'] for x in r['items']), 'başka ay ve başka kişi YOK')
check([x['at'] for x in r['items']] == sorted([x['at'] for x in r['items']], reverse=True), 'yeniden eskiye sıralı')
check(r['sum'].get('net') == bot.calc_summary(db, 10, P)['net'], 'ay özeti maaş hesabıyla aynı')
check(act(11, {'action': 'staff_history', 'target': 10, 'period': P}).get('ok') is False, 'çalışan başkasının geçmişini göremez')
check(act(10, {'action': 'staff_history', 'period': P}).get('ok') is True, 'çalışan kendi geçmişini görebilir')
check(act(1, {'action': 'staff_history', 'target': 10, 'period': '2026-09'})['items'][0]['s'] == 'другой месяц', 'ay değiştirme çalışıyor')
print('ALL OK' if not FAIL else 'FAILED: %s' % FAIL)
raise SystemExit(1 if FAIL else 0)
