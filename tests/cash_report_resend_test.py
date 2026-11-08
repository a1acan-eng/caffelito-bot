# Kasa raporu gruba (yeniden) gönderilir; HTML reddedilirse sade metin (owner 2026-11-09:
# grupta yalnız «закрыл(а) смену» vardı, bardak dökümü yoktu).
# Çalıştırma: rm -f /tmp/cr.db; PYTHONPATH=. DB_PATH=/tmp/cr.db BOT_TOKEN=123:abc python tests/cash_report_resend_test.py
import json, asyncio, types, bot
FAIL = []
def check(c, m):
    print(('  ok  ' if c else '  FAIL ') + m)
    if not c: FAIL.append(m)
sent = []
class FB:
    def __init__(self, bad_html=False): self.bad = bad_html
    async def send_message(self, chat_id=None, text=None, parse_mode=None, **k):
        if self.bad and parse_mode == 'HTML':
            raise Exception("Can't parse entities")
        sent.append((chat_id, text, parse_mode))
        return types.SimpleNamespace(chat_id=int(chat_id), message_id=len(sent) + 100, text=text)
    async def edit_message_text(self, **k): raise Exception('message to edit not found')
    def __getattr__(self, n):
        async def f(*a, **k): return None
        return f
def act(uid, p, fb):
    ctx = bot._ShimContext(fb, {}, uid=uid)
    asyncio.run(bot.handle_webapp_data(bot._ShimUpdate(fb, uid, f'U{uid}', None, json.dumps(p)), ctx))
    return getattr(ctx, 'result', None)
bot.get_db()
db = bot.get_db()
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized,branch_id) VALUES (1,'Owner','owner',1,1,1)")
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (10,'Suhrob','barista',1,1,1)")
db.execute("INSERT OR REPLACE INTO branches (id,name,active,group_chat_id) VALUES (1,'Magic',1,'-1001')")
rid = db.execute("INSERT INTO cashreports (user_id,user_name,date,period,created_at,bylo,restock,ostalos,sold,cups_total,expenses,expenses_total,branch_id,start_time,end_time) "
                 "VALUES (10,'Suhrob','2026-11-09','2026-11','2026-11-09T01:16:00',?,?,?,?,24,'[]',0,1,'2026-11-08T15:31:00','2026-11-09T01:16:00')",
                 (json.dumps({'Стакан 500': 172}), json.dumps({}), json.dumps({'Стакан 500': 170}), json.dumps({'Стакан 500': 2}))).lastrowid
db.commit()
r = act(10, {'action': 'cash_report_resend', 'id': rid}, FB())
check(r and not r.get('ok') and not sent, 'barista gönderemez')
r = act(1, {'action': 'cash_report_resend', 'id': rid}, FB())
g = [x for x in sent if x[0] == -1001]
check(r and r.get('ok') and r['mode'] == 'sent' and g and 'Стакан 500: ' in g[0][1] and 'ИТОГО ПРОДАНО:  24' in g[0][1], f'gruba bardak dökümüyle gönderildi ({r})')
sent.clear()
m = asyncio.run(bot.cash_report_post(FB(bad_html=True), db, rid))
g = [x for x in sent if x[0] == -1001]
check(m is not None and g and g[0][2] is None and '<b>' not in g[0][1] and 'Стакан 500' in g[0][1], 'HTML reddedilirse sade metin gider')
r = act(1, {'action': 'cash_report_resend', 'id': 999}, FB())
check(r and not r.get('ok'), 'olmayan rapor')
print('ALL OK' if not FAIL else 'FAILED: %s' % FAIL)
raise SystemExit(1 if FAIL else 0)
