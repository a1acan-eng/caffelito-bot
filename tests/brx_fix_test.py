# Eski «Ad (упак, …)» kayıtları → katalog adları (owner 2026-10-21): önizleme + onaylı uygulama.
# Çalıştırma: rm -f /tmp/bf.db; PYTHONPATH=. DB_PATH=/tmp/bf.db BOT_TOKEN=123:abc python tests/brx_fix_test.py
import json, asyncio, bot
from datetime import datetime
FAIL = []
def check(c, m):
    print(('  ok  ' if c else '  FAIL ') + m)
    if not c: FAIL.append(m)
class FB:
    async def send_message(self, chat_id=None, text=None, *a, **k): return None
    def __getattr__(self, n):
        async def f(*a, **k): return None
        return f
def act(uid, p):
    ctx = bot._ShimContext(FB(), {}, uid=uid)
    asyncio.run(bot.handle_webapp_data(bot._ShimUpdate(FB(), uid, f'U{uid}', None, json.dumps(p)), ctx))
    return ctx.result
db = bot.get_db()
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized) VALUES (1,'Owner','owner',1,1)")
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (10,'Бек','barista',1,1,1)")
for bid, nm in ((1, 'C5'), (2, 'Magic')):
    db.execute("INSERT OR REPLACE INTO branches (id, name, active) VALUES (?,?,1)", (bid, nm))
now = datetime.now(bot.TZ).isoformat()
for it, u, q in (("Кофе эспрессо смесь (упак, 1 кг)", "кг", 2), ("Молоко 3,2% (упак, 12 л)", "уп", 1),
                 ("Сливки 11% 0,2 л (упак, 27 шт)", "уп", 2), ("Пюре странное (1 кг)", "г", 500)):
    db.execute("INSERT INTO brx_tx (from_bid,to_bid,item,unit,qty,by_id,by_name,at) VALUES (1,2,?,?,?,10,'Бек',?)", (it, u, q, now))
db.commit()
# Yeni adla geri dönüş: Magic → C5 2 шт kahve (eski 2 кг borcu kapatmalı)
act(1, {'action': 'brx_add', 'from': 2, 'to': 1, 'items': [{'item': 'Кофе эспрессо смесь', 'unit': 'шт', 'qty': 2}]})
bal = bot.brx_balance(db)[(1, 2)]
check(bal.get(('кофе эспрессо смесь (упак, 1 кг)', 'кг')) == 2 and bal.get(('кофе эспрессо смесь', 'шт')) == -2, 'düzeltmeden ÖNCE: iki ayrı satır (sorun)')
check(act(10, {'action': 'brx_fix_preview'}) == {'ok': False, 'error': 'Только владелец.'}, 'çalışan önizleyemez')
pv = act(1, {'action': 'brx_fix_preview'})
check(pv['ok'] and len(pv['rows']) == 3 and len(pv['bad']) == 1, 'önizleme: 3 değişecek, 1 elle bakılacak')
check(db.execute("SELECT COUNT(*) c FROM brx_tx WHERE item LIKE '%(%'").fetchone()['c'] == 4, 'önizleme hiçbir şeyi değiştirmedi')
ap = act(1, {'action': 'brx_fix_apply', 'ids': [r['id'] for r in pv['rows']]})
check(ap['ok'] and ap['done'] == 3, 'uygulandı: 3 kayıt')
bal = bot.brx_balance(db).get((1, 2), {})
check(abs(bal.get(('кофе эспрессо смесь', 'шт'), 0)) < 1e-9, 'kahve: eski 2 кг ile yeni 2 шт birbirini kapattı')
check(bal.get(('молоко 3,2%', 'шт')) == 1, 'süt: 1 уп → 1 шт')
check(bal.get(('сливки 11% 0,2 л', 'шт')) == 54, 'krema: 2 уп × 27 (paket boyutu kaydedildi) = 54 шт')
check(bal.get(('пюре странное (1 кг)', 'г')) == 500, 'çevrilemeyen kayıt olduğu gibi kaldı')
print('ALL OK' if not FAIL else 'FAILED: %s' % FAIL)
