# Смена перенесена в другой филиал → обе группы узнают (owner 2026-11-16).
# Çalıştırma: rm -f /tmp/sm.db; PYTHONPATH=. DB_PATH=/tmp/sm.db BOT_TOKEN=123:abc python tests/shift_move_test.py
import json, asyncio, types, bot
from datetime import datetime, timedelta
FAIL = []
def check(c, m):
    print(('  ok  ' if c else '  FAIL ') + m)
    if not c: FAIL.append(m)
sent, edits = [], []
class FB:
    async def send_message(self, chat_id=None, text=None, *a, **k):
        sent.append((str(chat_id), text)); return types.SimpleNamespace(chat_id=chat_id, message_id=900 + len(sent))
    async def edit_message_text(self, chat_id=None, message_id=None, text=None, **k):
        edits.append((str(chat_id), message_id, text))
    def __getattr__(self, n):
        async def f(*a, **k): return None
        return f
def act(uid, p):
    ctx = bot._ShimContext(FB(), {}, uid=uid)
    asyncio.run(bot.handle_webapp_data(bot._ShimUpdate(FB(), uid, f'U{uid}', None, json.dumps(p)), ctx))
bot.get_db()
db = bot.get_db()
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized,branch_id) VALUES (11,'Ubay','barista',1,1,1)")
db.execute("INSERT OR REPLACE INTO branches (id,name,active,group_chat_id) VALUES (1,'C5',1,'-101')")
db.execute("INSERT OR REPLACE INTO branches (id,name,active,group_chat_id) VALUES (2,'Magic',1,'-102')")
st = datetime.now(bot.TZ).replace(tzinfo=None) - timedelta(minutes=6)
db.execute("INSERT INTO shifts (user_id,hours,total,date,period,created_at,start_time,branch_id,shift_role,rate) VALUES (11,0,0,?,?,?,?,2,'barista',12000)",
           (st.strftime('%Y-%m-%d'), st.strftime('%Y-%m'), st.isoformat(), st.isoformat()))
sid = db.execute("SELECT id FROM shifts WHERE user_id=11").fetchone()[0]
db.execute("INSERT INTO sent_msgs (chat_id,message_id,text,ents,created_at,kind,shift_id) VALUES (-102,50,'x','[]',?,'shift_start',?)",
           (st.isoformat(), sid))
db.commit()
act(11, {'action': 'move_shift_branch', 'branch_id': 1})
check(db.execute("SELECT branch_id FROM shifts WHERE id=?", (sid,)).fetchone()[0] == 1, 'vardiya C5\'e taşındı')
check(any(c == '-102' and m == 50 and 'перенесена в <b>C5</b>' in tx for c, m, tx in edits), f'Magic mesajı düzeltildi ({edits})')
g = [tx for c, tx in sent if c == '-101']
check(g and 'начал(а) смену' in g[0] and 'из Magic' in g[0] and st.strftime('%H:%M') in g[0], f'C5 grubuna mesaj ({g})')
check(not [1 for c, _t in sent if c == '-102'], 'Magic grubuna ayrıca mesaj atılmadı')
# Aynı şubeye tekrar → sessiz
sent.clear(); edits.clear()
act(11, {'action': 'move_shift_branch', 'branch_id': 1})
check(not [1 for c, _t in sent if c.startswith('-')] and not edits, 'aynı şube: mesaj yok')
# Başlangıç mesajı bulunamazsa eski gruba kısa not
db.execute("DELETE FROM sent_msgs"); db.commit()
act(11, {'action': 'move_shift_branch', 'branch_id': 2})
check(any(c == '-101' and 'перешёл(а) в <b>Magic</b>' in tx for c, tx in sent), 'mesaj yoksa eski gruba not')
print('ALL OK' if not FAIL else 'FAILED: %s' % FAIL)
raise SystemExit(1 if FAIL else 0)
