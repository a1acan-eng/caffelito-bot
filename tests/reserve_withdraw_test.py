# Rezervden çekme (owner 2026-10-30): yalnız owner, istediği tutar (≤ bakiye); para bu ayın
# maaşına AYRI kalem («Из резерва», res_out) olarak eklenir; geri alınabilir.
# Çalıştırma: rm -f /tmp/rw.db; PYTHONPATH=. DB_PATH=/tmp/rw.db BOT_TOKEN=123:abc python tests/reserve_withdraw_test.py
import json, asyncio, bot
from datetime import datetime
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
db.execute("INSERT OR REPLACE INTO branches (id, name, active) VALUES (1,'C5',1)")
per = bot.current_period(); today = bot._adv_today().isoformat(); now = datetime.now(bot.TZ).isoformat()
for i, d in enumerate((per + "-01", per + "-02", per + "-03")):
    db.execute("INSERT INTO reserve_tx (user_id,kind,date,period,amount,target,note,created_by,created_by_name,created_at) "
               "VALUES (10,'contrib',?,?,100000,1000000,'Взнос',0,'Nero',?)", (d, per, now))
db.commit()
check(bot.reserve_balance(db, 10) == 300000, 'rezerv 300.000')
net0 = bot.calc_summary(db, 10, per)["net"]
r = act(10, {'action': 'reserve_withdraw', 'target': 10, 'amount': 1000})
check(r and not r.get('ok'), 'çalışan kendi rezervinden çekemez')
r = act(1, {'action': 'reserve_withdraw', 'target': 10, 'amount': 400000})
check(r and not r.get('ok') and '300' in r.get('error', ''), 'bakiyeden fazlası reddedildi')
r = act(1, {'action': 'reserve_withdraw', 'target': 10, 'amount': 120000, 'note': 'на лечение'})
check(r and r.get('ok') and r['res']['balance'] == 180000, 'çekildi → rezerv 180.000')
sm = bot.calc_summary(db, 10, per)
check(sm["res_out"] == 120000 and sm["net"] == net0 + 120000, 'maaşa ayrı kalem +120.000 (res_out)')
check(sm["adjustments"] == 0, 'düzeltmelere karışmadı')
check(any(c == 10 and 'Из вашего резерва снято' in (t or '') for c, t in SENT), 'çalışana bildirim gitti')
ds = bot.decade_statement(db, 10, per, 3)
check(ds["cum"] == sm["net"] + sm["paid"], f'son dekadın yürüyen hesabı aylık net ile aynı ({ds["cum"]} vs {sm["net"]})')
check(len(bot.build_payroll_all_pdf(db, per)) > 1000 and len(bot.build_payroll_decade_pdf(db, per, 3)) > 1000,
      'PDF\'ler «Из резерва» ile oluşuyor')
tx = db.execute("SELECT id FROM reserve_tx WHERE kind='withdraw'").fetchone()["id"]
r = act(1, {'action': 'reserve_withdraw_undo', 'id': tx})
check(r and r.get('ok') and r['res']['balance'] == 300000, 'geri alındı → rezerv 300.000')
check(bot.calc_summary(db, 10, per)["net"] == net0, 'maaştan düştü')
check(db.execute("SELECT COUNT(*) AS n FROM logs WHERE action IN ('reserve_withdraw','reserve_withdraw_undo')").fetchone()["n"] == 2, 'günlükte 2 kayıt')
v = bot.reserve_view(db, 10)
check(all(h["kind"] != "withdraw" for h in v["hist"]), 'geri alınan çekim geçmişte görünmez')
print('ALL OK' if not FAIL else 'FAILED: %s' % FAIL)
raise SystemExit(1 if FAIL else 0)
