# Yeni çizelge «Сегодня»: gerçek giriş/çıkış payload'ı `att` (owner 2026-10-07).
# Çalıştırma: rm -f /tmp/at.db; PYTHONPATH=. DB_PATH=/tmp/at.db BOT_TOKEN=123:abc python tests/att_payload_test.py
import json, urllib.parse as _up, bot
from datetime import datetime, timedelta
FAIL = []
def check(c, m):
    print(('  ok  ' if c else '  FAIL ') + m)
    if not c: FAIL.append(m)
db = bot.get_db()
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized) VALUES (1,'Owner','owner',1,1)")
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (10,'Бек','barista',1,1,1)")
now = datetime.now(bot.TZ)
def sh(uid, a, b):
    db.execute("INSERT INTO shifts (user_id,hours,total,date,period,created_at,start_time,end_time,branch_id) VALUES (?,?,?,?,?,?,?,?,?)",
               (uid, 0, 123456, a.strftime('%Y-%m-%d'), a.strftime('%Y-%m'), a.isoformat(), a.isoformat(), b.isoformat() if b else None, 1))
sh(10, now - timedelta(hours=10), now - timedelta(hours=1))
sh(10, now - timedelta(minutes=30), None)
sh(10, now - timedelta(days=3), now - timedelta(days=3, hours=-8))
db.commit()
def att(uid):
    s = bot.build_hash_payload(db, uid, 'x')
    d = dict(p.split('=', 1) for p in s.lstrip('#').split('&') if '=' in p)
    return json.loads(_up.unquote(d['att']))
a = att(10)
check(len(a) == 2, 'son ~40 saatin kayıtları (3 gün önceki yok)')
check(a[-1]['out'] is None and a[0]['out'], 'açık vardiya out=None, biten out dolu')
check(all(set(x) == {'uid', 'bid', 'in', 'out'} for x in a), 'yalnız uid/bid/in/out — para yok')
check(len(att(1)) == 2, 'owner da alıyor')
print('FAIL' if FAIL else 'ALL OK', FAIL)
