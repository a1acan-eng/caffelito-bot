# Maaş raporu ŞUBE kırılımı + «Зарплата всех» PDF (owner 2026-10-27).
# Çalıştırma: rm -f /tmp/pb.db; PYTHONPATH=. DB_PATH=/tmp/pb.db BOT_TOKEN=123:abc python tests/payroll_branch_test.py
import json, asyncio, bot
from datetime import datetime, timedelta
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
db = bot.get_db()
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized,branch_id) VALUES (1,'Owner','owner',1,1,1)")
for uid, nm in ((10, 'Бек'), (11, 'Убай'), (12, 'Никто')):
    db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (?,?,'barista',1,1,1)", (uid, nm))
db.execute("INSERT OR REPLACE INTO branches (id,name,active) VALUES (1,'Magic',1)")
db.execute("INSERT OR REPLACE INTO branches (id,name,active) VALUES (2,'C5',1)")
db.commit()
def sh(uid, d, bid, h):
    st = datetime(2026, 10, d, 7, 0)
    db.execute("INSERT INTO shifts (user_id,hours,total,date,period,created_at,start_time,branch_id,shift_role,rate) VALUES (?,0,0,?,?,?,?,?,'barista',20000)",
               (uid, st.strftime('%Y-%m-%d'), '2026-10', st.isoformat(), st.isoformat(), bid)); db.commit()
    bot.end_shift(db, uid, {}, custom_end=(st + timedelta(hours=h)).isoformat())
sh(10, 1, 1, 8); sh(10, 2, 1, 8); sh(10, 3, 2, 10); sh(11, 3, 2, 9)
su = bot.calc_summary(db, 10, '2026-10')
sp = {b['name']: b for b in bot.payroll_branch_split(db, su['shifts'])}
check(sp['Magic']['days'] == 2 and sp['Magic']['hours'] == 16 and sp['Magic']['pay'] == 320000, 'Magic: 2 gün · 16 sa · 320.000')
check(sp['C5']['days'] == 1 and sp['C5']['hours'] == 10 and sp['C5']['pay'] == 200000, 'C5: 1 gün · 10 sa · 200.000')
check(sum(b['pay'] for b in sp.values()) == su['hourly'], 'şube toplamı = saatlik ücret toplamı')
ppl = [p[1] for p in bot.payroll_people(db, '2026-10')]
check(ppl == ['Бек', 'Убай'], 'toplu raporda yalnız o ay hareketi olanlar (Никто yok, owner yok)')
one = bot.build_payroll_pdf(db, 10, '2026-10', 'Бек'); allp = bot.build_payroll_all_pdf(db, '2026-10')
check(one[:4] == b'%PDF' and allp[:4] == b'%PDF', 'iki PDF de üretiliyor')
act(11, {'action': 'payroll_pdf_all', 'period': '2026-10'})
check(not DOCS, 'barista toplu raporu alamaz')
act(1, {'action': 'payroll_pdf_all', 'period': '2026-10'})
check(len(DOCS) == 1 and DOCS[0][0] == 1 and DOCS[0][1] == 'Nero_zarplata_vse_2026-10.pdf' and DOCS[0][2][:4] == b'%PDF', 'owner toplu PDF\'i kendi sohbetine alır')
print('ALL OK' if not FAIL else 'FAILED: %s' % FAIL)
raise SystemExit(1 if FAIL else 0)
