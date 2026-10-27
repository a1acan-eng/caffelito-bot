# 10 GÜNLÜK MAAŞ (owner 2026-10-27): maaş 10'u, 20'si, 30'unda — dönem kazancı, yürüyen hesap, ödenenler.
# Çalıştırma: rm -f /tmp/dp.db; PYTHONPATH=. DB_PATH=/tmp/dp.db BOT_TOKEN=123:abc python tests/decade_pay_test.py
import json, asyncio, bot
from datetime import datetime, timedelta, date
FAIL = []
def check(c, m):
    print(('  ok  ' if c else '  FAIL ') + m)
    if not c: FAIL.append(m)
DOCS = []
class FB:
    async def send_message(self, *a, **k): return None
    async def send_document(self, chat_id=None, document=None, filename=None, **k):
        DOCS.append((chat_id, filename, document.read() if document else b'')); return None
    def __getattr__(self, n):
        async def f(*a, **k): return None
        return f
def act(uid, p):
    ctx = bot._ShimContext(FB(), {}, uid=uid)
    asyncio.run(bot.handle_webapp_data(bot._ShimUpdate(FB(), uid, f'U{uid}', None, json.dumps(p)), ctx))
# Testi geçmiş bir aya sabitle: bütün taksitlerin günü gelmiş olsun
bot._adv_today = lambda: date(2026, 9, 30)
db = bot.get_db()
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized,branch_id) VALUES (1,'Owner','owner',1,1,1)")
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (10,'Бек','barista',1,1,1)")
db.execute("INSERT OR REPLACE INTO branches (id,name,active) VALUES (1,'Magic',1)")
db.execute("INSERT OR REPLACE INTO branches (id,name,active) VALUES (2,'C5',1)")
db.commit()
P = '2026-09'
def sh(d, bid, h):
    st = datetime(2026, 9, d, 7, 0)
    db.execute("INSERT INTO shifts (user_id,hours,total,date,period,created_at,start_time,branch_id,shift_role,rate) VALUES (10,0,0,?,?,?,?,?,'barista',20000)",
               (st.strftime('%Y-%m-%d'), P, st.isoformat(), st.isoformat(), bid)); db.commit()
    bot.end_shift(db, 10, {}, custom_end=(st + timedelta(hours=h)).isoformat())
sh(2, 1, 10); sh(5, 2, 8); sh(12, 1, 10); sh(25, 2, 10)
db.execute("INSERT INTO fines (user_id,amount,reason,period,created_at) VALUES (10,30000,'опоздание',?,'2026-09-13T08:00:00')", (P,))
db.execute("INSERT INTO tips (user_id,amount,note,period,created_at) VALUES (10,10000,'',?,'2026-09-26T10:00:00')", (P,))
aid = db.execute("INSERT INTO advances (user_id,month,amount,commission,total,status,created_at,decided_at) VALUES (10,?,100000,0,100000,'active','2026-09-03','2026-09-03')", (P,)).lastrowid
db.execute("INSERT INTO advance_inst (advance_id,user_id,n,due_date,period,amount) VALUES (?,10,1,'2026-09-15',?,50000)", (aid, P))
db.execute("INSERT INTO advance_inst (advance_id,user_id,n,due_date,period,amount) VALUES (?,10,2,'2026-09-25',?,50000)", (aid, P))
# 10'unda 1. dönem ödendi
db.execute("INSERT INTO payments (user_id,amount,period,paid_by,paid_by_name,paid_at,kind) VALUES (10,360000,?,1,'Owner','2026-09-10T20:00:00','salary')", (P,))
db.commit()
# Eylül'e ait ama 2 Ekim'de girilmiş düzeltme → son döneme sayılmalı
db.execute("INSERT INTO adjustments (user_id,amount,note,period,created_at) VALUES (10,40000,'за сентябрь',?,'2026-10-02T09:00:00')", (P,))
db.commit()
d1, d2, d3 = (bot.decade_statement(db, 10, P, k) for k in (1, 2, 3))
check(d1['dec']['work'] == 360000 and [b['name'] for b in d1['dec']['branches']] == ['Magic', 'C5'], '1–10: Magic 200.000 + C5 160.000 = 360.000')
check(d1['due'] == 0, '1–10: 10\'unda ödendi → kalan 0')
check(d2['dec']['work'] == 200000 and d2['dec']['fines'] == 30000 and d2['dec']['adv'] == 50000 and d2['dec']['net'] == 120000,
      '11–20: kazanç 200.000 − ceza 30.000 − avans 50.000 = 120.000')
check(d2['due'] == 120000 and d2['paid'] == 360000, '20\'sinde ödenecek: yalnız 2. dönem (ilk ödeme düşüldü)')
check(d3['dec']['net'] == 200000 + 10000 - 50000 + 40000 and d3['due'] == 120000 + 200000, '21–30: kazanç+bahşiş−avans + ay dışı düzeltme 40.000 = 200.000; ödenmemiş toplam 320.000')
su = bot.calc_summary(db, 10, P)
check(d3['due'] == su['net'], 'son dönemin «к выплате» = maaş kartının net\'i')
check(d1['dec']['net'] + d2['dec']['net'] + d3['dec']['net'] == su['net'] + su['paid'], '3 dönemin toplamı = ayın toplamı')
blob = bot.build_payroll_decade_pdf(db, P, 2)
check(blob[:4] == b'%PDF', 'dönem PDF\'i üretiliyor')
act(1, {'action': 'payroll_pdf_all', 'period': P, 'decade': 2})
check(len(DOCS) == 1 and DOCS[0][1] == 'Nero_zarplata_vse_2026-09_11-20.pdf', 'owner dönem PDF\'ini alır (11–20)')
act(1, {'action': 'payroll_pdf_all', 'period': P})
check(len(DOCS) == 2 and DOCS[1][1] == 'Nero_zarplata_vse_2026-09.pdf', 'aylık PDF de duruyor')
one = bot.build_payroll_pdf(db, 10, P, 'Бек')
check(one[:4] == b'%PDF', 'tek kişilik PDF (ПО ДЕКАДАМ bölümüyle) üretiliyor')
print('ALL OK' if not FAIL else 'FAILED: %s' % FAIL)
raise SystemExit(1 if FAIL else 0)
