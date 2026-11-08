# Kapanış + kasa raporu TEK istekte (owner 2026-11-09, canlı: Сухроб kapattı, rapor gelmedi).
# /api/action shift_end + report → önce kapanış, sonra rapor; rapor yoksa güvenlik ağı uyarır.
# Çalıştırma: rm -f /tmp/co.db; PYTHONPATH=. DB_PATH=/tmp/co.db BOT_TOKEN=123:abc python tests/close_one_request_test.py
import json, asyncio, types, bot
from datetime import datetime, timedelta
FAIL = []
def check(c, m):
    print(('  ok  ' if c else '  FAIL ') + m)
    if not c: FAIL.append(m)
sent = []
class FB:
    async def send_message(self, chat_id=None, text=None, *a, **k):
        sent.append((chat_id, text)); return types.SimpleNamespace(chat_id=int(chat_id), message_id=len(sent) + 10, text=text)
    def __getattr__(self, n):
        async def f(*a, **k): return None
        return f
class Req:
    def __init__(self, body):
        self._b = body; self.app = {"tg_app": types.SimpleNamespace(bot=FB(), bot_data={})}
    async def text(self): return json.dumps(self._b)
bot.get_db()
db = bot.get_db()
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized,branch_id) VALUES (1,'Owner','owner',1,1,1)")
for u, n in ((10, 'Suhrob'), (11, 'Akmal')):
    db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (?,?,'barista',1,1,1)", (u, n))
db.execute("INSERT OR REPLACE INTO branches (id,name,active,group_chat_id) VALUES (1,'Magic',1,'-1001')")
db.commit()
bot.CR_WATCH_SEC = 0
def api(uid, payload):
    body = {"token": bot.make_web_token(uid), "data": json.dumps(payload)}
    async def go():
        r = await bot.api_action(Req(body))
        await asyncio.sleep(0.2)   # güvenlik ağı (0 sn) çalışsın
        return r
    return asyncio.run(go())
def start(uid):
    st = (datetime.now(bot.TZ).replace(tzinfo=None) - timedelta(hours=9)).isoformat()
    db.execute("INSERT INTO shifts (user_id,hours,date,period,created_at,start_time,branch_id,shift_role,rate) VALUES (?,0,?,?,?,?,1,'barista',20000)",
               (uid, st[:10], st[:7], st, st)); db.commit()
cups = [{'n': 'Стакан 500', 'b': 172, 'r': 0, 'o': 170, 's': 2}, {'n': 'Стакан 400', 'b': 153, 'r': 0, 'o': 147, 's': 6}]
start(10)
end = datetime.now(bot.TZ).isoformat()
r = api(10, {'action': 'shift_end', 'drinks': {'k5': 2, 'k4': 6}, 'end_time': end,
             'report': {'cups': cups, 'branch_id': 1, 'branch': 1, 'itogo': 0, 'vyshlo': 0, 'daily_pay': 0,
                        'expenses': [], 'end_time': end, 'note': ''}})
check(r.status == 200, 'istek başarılı')
check(bot.get_active_shift(db, 10) is None, 'vardiya kapandı')
rep = db.execute("SELECT * FROM cashreports WHERE user_id=10").fetchone()
check(rep is not None and rep['cups_total'] == 8, 'kasa raporu AYNI istekte yazıldı (8 bardak)')
g = [t for c, t in sent if c == -1001]
check(any('закрыл' in t for t in g) and any('СМЕННЫЙ ОТЧЁТ' in t for t in g), 'grupta kapanış + bardak dökümü')
check(not any('отчёт кассы не пришёл' in (t or '') for c, t in sent), 'rapor varken uyarı yok')
# Eski istemci: rapor ayrı istekle hiç gelmezse → güvenlik ağı
sent.clear(); start(11)
api(11, {'action': 'shift_end', 'drinks': {'k5': 1}, 'end_time': datetime.now(bot.TZ).isoformat()})
check(any(c == 1 and 'отчёт кассы не пришёл' in t for c, t in sent), 'rapor gelmedi → owner\'a uyarı')
check(any(c == 11 and 'не дошёл' in t for c, t in sent), 'baristaya uyarı')
print('ALL OK' if not FAIL else 'FAILED: %s' % FAIL)
raise SystemExit(1 if FAIL else 0)
