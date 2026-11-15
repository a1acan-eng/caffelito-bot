# Между филиалами: пакет, 10 минут «Изменить / Отменить», сообщение в группе исправляется (owner 2026-11-15).
# Çalıştırma: rm -f /tmp/bb.db; PYTHONPATH=. DB_PATH=/tmp/bb.db BOT_TOKEN=123:abc python tests/brx_batch_test.py
import json, asyncio, types, bot
from datetime import datetime, timedelta
FAIL = []
def check(c, m):
    print(('  ok  ' if c else '  FAIL ') + m)
    if not c: FAIL.append(m)
sent, edits = [], []
_mid = [100]
class FB:
    async def send_message(self, chat_id=None, text=None, *a, **k):
        _mid[0] += 1; sent.append((str(chat_id), _mid[0], text)); return types.SimpleNamespace(message_id=_mid[0])
    async def edit_message_text(self, chat_id=None, message_id=None, text=None, **k):
        edits.append((str(chat_id), message_id, text))
    def __getattr__(self, n):
        async def f(*a, **k): return None
        return f
def act(uid, p):
    ctx = bot._ShimContext(FB(), {}, uid=uid)
    asyncio.run(bot.handle_webapp_data(bot._ShimUpdate(FB(), uid, f'U{uid}', None, json.dumps(p)), ctx))
    return getattr(ctx, 'result', None)
bot.get_db()
db = bot.get_db()
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized,branch_id) VALUES (1,'Owner','owner',1,1,1)")
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized,branch_id) VALUES (5,'Akmal','barista',1,1,1)")
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized,branch_id) VALUES (6,'Karina','barista',1,1,1)")
for i, n, g in ((1, 'C5', '-101'), (2, 'Magic', '-102'), (3, 'Bodomzor', '-103')):
    db.execute("INSERT OR REPLACE INTO branches (id,name,active,group_chat_id) VALUES (?,?,1,?)", (i, n, g))
db.commit()
grp = lambda: {c for c, _m, _t in sent if c.startswith('-')}
live = lambda b: [dict(r) for r in db.execute("SELECT * FROM brx_tx WHERE batch=? AND COALESCE(voided,0)=0", (b,))]
r = act(5, {'action': 'brx_add', 'from': 1, 'to': 2, 'batch': 'b1', 'items': [{'item': 'Круассан', 'unit': 'шт', 'qty': 2}]})
check(r and r.get('ok') and r.get('batch') == 'b1', f'gönderildi ({r})')
check(grp() == {'-101', '-102'} and len(live('b1')) == 1, 'iki gruba mesaj, 1 satır')
m = json.loads(db.execute("SELECT msgs FROM brx_batch WHERE batch='b1'").fetchone()[0])
check(len(m) == 2, f'mesaj kimlikleri saklandı ({m})')
# aynı paket tekrar → yazılmaz
act(5, {'action': 'brx_add', 'from': 1, 'to': 2, 'batch': 'b1', 'items': [{'item': 'Круассан', 'unit': 'шт', 'qty': 2}]})
check(len(live('b1')) == 1 and len([1 for c, _m, _t in sent if c.startswith('-')]) == 2, 'tekrar gönderim yazılmaz')
# düzelt: miktar 3, şube Bodomzor → C5 mesajı güncellenir, Magic «исправлена», Bodomzor yeni
sent.clear()
r = act(5, {'action': 'brx_add', 'from': 1, 'to': 3, 'batch': 'b1', 'edit': 1, 'items': [{'item': 'Круассан', 'unit': 'шт', 'qty': 3}]})
L = live('b1')
check(r.get('ok') and len(L) == 1 and L[0]['qty'] == 3 and L[0]['to_bid'] == 3, f'düzeltildi ({L})')
check(any(c == '-101' and 'исправлено' in tx and '3' in tx for c, _m, tx in edits), 'C5 grubundaki mesaj güncellendi')
check(any(c == '-102' and 'исправлена' in tx for c, _m, tx in edits), 'Magic mesajı «исправлена»')
check([c for c, _m, _t in sent if c.startswith('-')] == ['-103'], f'Bodomzor yeni mesaj ({sent})')
# başkası düzeltemez
r = act(6, {'action': 'brx_add', 'from': 1, 'to': 2, 'batch': 'b1', 'edit': 1, 'items': [{'item': 'X', 'unit': 'шт', 'qty': 1}]})
check(r and not r.get('ok') and live('b1')[0]['item'] == 'Круассан', 'başka çalışan düzeltemez')
# 10 dk sonra yazan düzeltemez, owner düzeltir
db.execute("UPDATE brx_batch SET at=? WHERE batch='b1'", ((datetime.now(bot.TZ) - timedelta(minutes=11)).isoformat(),)); db.commit()
r = act(5, {'action': 'brx_batch_cancel', 'batch': 'b1'})
check(r and not r.get('ok') and live('b1'), '11 dk sonra çalışan iptal edemez')
edits.clear()
r = act(1, {'action': 'brx_batch_cancel', 'batch': 'b1'})
check(r and r.get('ok') and not live('b1'), 'owner iptal etti')
check(edits and all('отменена' in tx for _c, _m, tx in edits), f'gruplarda «отменена» ({edits})')
# brx_edit: yazan 10 dk
act(5, {'action': 'brx_add', 'from': 1, 'to': 2, 'batch': 'b2', 'items': [{'item': 'Чай', 'unit': 'шт', 'qty': 1}]})
tid = live('b2')[0]['id']
db.execute("UPDATE brx_tx SET at=? WHERE id=?", ((datetime.now(bot.TZ) - timedelta(minutes=15)).isoformat(), tid)); db.commit()
act(5, {'action': 'brx_edit', 'id': tid, 'qty': 5})
check(live('b2')[0]['qty'] == 1, 'geçmişten düzeltme: yazan için 10 dk')
print('ALL OK' if not FAIL else 'FAILED: %s' % FAIL)
raise SystemExit(1 if FAIL else 0)
