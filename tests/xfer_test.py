# ПЕРЕВОДЫ (owner 2026-10-25): çalışanlar arası para gönderme (Nero ID ile) — gönderen −, alan +,
# geçmiş kaydı, bakiye kontrolü, bildirim.
# Çalıştırma: rm -f /tmp/xf.db; PYTHONPATH=. DB_PATH=/tmp/xf.db BOT_TOKEN=123:abc python tests/xfer_test.py
import json, asyncio, bot
FAIL = []
def check(c, m):
    print(('  ok  ' if c else '  FAIL ') + m)
    if not c: FAIL.append(m)
SENT = []
class FB:
    async def send_message(self, chat_id=None, text=None, *a, **k):
        if chat_id is None and a: chat_id = a[0]
        if text is None and len(a) > 1: text = a[1]
        SENT.append((chat_id, text or '')); return None
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
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized,archived) VALUES (12,'Арх','barista',1,1,1,1)")
db.execute("INSERT OR REPLACE INTO branches (id,name,active) VALUES (1,'C5',1)")
db.commit()
per = bot.current_period()
net = lambda u: int(bot.calc_summary(db, u, per)["net"] or 0)
# Бек'e 300 000 alacak ver (maaş +)
db.execute("INSERT INTO adjustments (user_id,amount,note,period,created_at) VALUES (10,300000,'test',?,'x')", (per,)); db.commit()
b0, u0 = net(10), net(11)
r = act(10, {'action': 'xfer_send', 'to': 11, 'amount': 120000, 'note': 'за такси'})
check(r.get('ok') is True, 'gönderim başarılı')
check(net(10) == b0 - 120000 and net(11) == u0 + 120000, 'Бек −120 000, Убай +120 000')
x = db.execute("SELECT * FROM xfers ORDER BY id DESC LIMIT 1").fetchone()
check(x and x["from_uid"] == 10 and x["to_uid"] == 11 and x["amount"] == 120000 and x["note"] == 'за такси', 'geçmiş kaydı (kim, kime, ne kadar, neden)')
check(any(str(c) == '11' and '120' in t and 'за такси' in t for c, t in SENT), 'Убай\'a bildirim gitti')
rows10 = bot.xfer_rows(db, 10, 'barista'); rows11 = bot.xfer_rows(db, 11, 'barista')
check(len(rows10) == 1 and len(rows11) == 1, 'ikisi de geçmişte görüyor')
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (13,'Третий','barista',1,1,1)"); db.commit()
check(bot.xfer_rows(db, 13, 'barista') == [] and len(bot.xfer_rows(db, 1, 'owner')) == 1, 'başkası görmüyor, owner hepsini görüyor')
adj = [a["note"] for a in bot.calc_summary(db, 10, per)["adjustments_list"]]
check(any('Перевод → ' in n for n in adj), '«Из чего сложилось»da görünüyor')
# Bakiye yetmezse
r = act(10, {'action': 'xfer_send', 'to': 11, 'amount': 10_000_000})
check(r.get('ok') is False and 'Недостаточно' in (r.get('error') or ''), 'bakiyeden fazlası gönderilemez')
check(act(10, {'action': 'xfer_send', 'to': 10, 'amount': 1000}).get('ok') is False, 'kendine gönderilemez')
check(act(10, {'action': 'xfer_send', 'to': 12, 'amount': 1000}).get('ok') is False, 'arşivdekine gönderilemez')
check(act(10, {'action': 'xfer_send', 'to': 999, 'amount': 1000}).get('ok') is False, 'olmayan ID\'ye gönderilemez')
check(act(10, {'action': 'xfer_send', 'to': 11, 'amount': -5}).get('ok') is False, 'eksi tutar olmaz')
check(db.execute("SELECT COUNT(*) FROM xfers").fetchone()[0] == 1, 'hatalı denemeler kayıt açmadı')
# ── Owner düzeltme / iptal ──
xid = db.execute("SELECT id FROM xfers ORDER BY id DESC LIMIT 1").fetchone()[0]
b1, u1 = net(10), net(11)
check(act(10, {'action': 'xfer_edit', 'id': xid, 'amount': 1}).get('ok') is False, 'barista transferi düzeltemez')
check(act(11, {'action': 'xfer_cancel', 'id': xid}).get('ok') is False, 'barista transferi iptal edemez')
r = act(1, {'action': 'xfer_edit', 'id': xid, 'amount': 100000, 'note': 'исправлено'})
check(r.get('ok') is True and net(10) == b1 + 20000 and net(11) == u1 - 20000, 'owner 120.000 → 100.000: iki taraf da düzeldi')
x = db.execute("SELECT * FROM xfers WHERE id=?", (xid,)).fetchone()
check(x["amount"] == 100000 and x["orig_amount"] == 120000 and x["note"] == 'исправлено' and x["edited_by"] == 1, 'ilk tutar korunuyor, kim düzeltti yazılı')
check(act(1, {'action': 'xfer_edit', 'id': xid, 'amount': 0}).get('ok') is False, '0 ile düzeltme yok (iptal ayrı)')
SENT.clear()
r = act(1, {'action': 'xfer_cancel', 'id': xid})
check(r.get('ok') is True and net(10) == b0 and net(11) == u0, 'iptal: iki taraf ilk haline döndü')
x = db.execute("SELECT * FROM xfers WHERE id=?", (xid,)).fetchone()
check(x is not None and x["status"] == 'canceled' and x["canceled_by"] == 1, 'kayıt silinmedi, «отменён» olarak duruyor')
check(any(str(c) in ('10', '11') and 'отменил' in t for c, t in SENT), 'taraflara iptal bildirimi')
check(act(1, {'action': 'xfer_cancel', 'id': xid}).get('ok') is False, 'iki kez iptal edilemez')
check(bot.xfer_rows(db, 1, 'owner')[0]["st"] == 'canceled', 'geçmişte durum görünüyor')
lg = db.execute("SELECT COUNT(*) FROM logs WHERE action IN ('xfer_edit','xfer_cancel')").fetchone()[0]
check(lg == 2, 'düzeltme ve iptal günlükte')
print('ALL OK' if not FAIL else 'FAILED: %s' % FAIL)
raise SystemExit(1 if FAIL else 0)
