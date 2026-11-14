# Çalışan fotoğrafı: Telegram'dan anında yenileme, elle yükleme, Telegram'a dönüş (owner 2026-11-14).
# Çalıştırma: rm -f /tmp/av.db; PYTHONPATH=. DB_PATH=/tmp/av.db BOT_TOKEN=123:abc python tests/ava_test.py
import json, asyncio, base64, types, bot
FAIL = []
def check(c, m):
    print(('  ok  ' if c else '  FAIL ') + m)
    if not c: FAIL.append(m)
NS = types.SimpleNamespace
PHOTOS = {10: 'tgA', 11: None}
class FB:
    async def get_user_profile_photos(self, uid, limit=1):
        u = PHOTOS.get(uid)
        if not u: return NS(photos=[])
        return NS(photos=[[NS(width=160, file_id='f' + u, file_unique_id=u)]])
    async def get_file(self, fid):
        async def dl(): return bytearray(b'\xff\xd8TG' + fid.encode())
        return NS(download_as_bytearray=dl)
    async def send_message(self, *a, **k): return None
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
for uid, nm in ((10, 'Akmal'), (11, 'Karina')):
    db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (?,?,'barista',1,1,1)", (uid, nm))
db.commit()
data = lambda u: bytes(db.execute("SELECT data FROM avatars WHERE user_id=?", (u,)).fetchone()["data"] or b'')

# Telegram'da fotoğraf değişti → «hemen yenile» TTL beklemeden alır
asyncio.run(bot.ava_fetch(FB(), db, 10))
check(data(10).endswith(b'ftgA'), 'ilk foto')
PHOTOS[10] = 'tgB'
r = act(1, {'action': 'ava_refresh'})
check(r and r['ok'] and r['hidden'] == [x for x in r['hidden'] if x != 'Akmal'] and 'Karina' in r['hidden'] and data(10).endswith(b'ftgB'),
      f'hepsini yenile: yeni foto + gizleyenler listesi ({r})')
# elle yükleme
jpg = b'\xff\xd8MANUAL' + b'x' * 100
r = act(1, {'action': 'ava_set', 'target': 11, 'data': 'data:image/jpeg;base64,' + base64.b64encode(jpg).decode()})
check(r and r['ok'] and data(11) == jpg, 'elle yükleme')
m = bot.ava_map(db)
check('v=m' in m.get('11', ''), f'harita: elle yüklenen işaretli ({m.get("11")})')
PHOTOS[11] = 'tgK'
act(1, {'action': 'ava_refresh'})
asyncio.run(bot.ava_fetch(FB(), db, 11))
check(data(11) == jpg, 'yenileme elle yüklenen fotoya dokunmaz')
r = act(1, {'action': 'ava_reset', 'target': 11})
check(r and r['n'] == 1 and data(11).endswith(b'ftgK'), 'Telegram fotoğrafına dönüş')
# güvenlik
check(act(10, {'action': 'ava_refresh'})['ok'] is False, 'barista yenileyemez')
check(act(1, {'action': 'ava_set', 'target': 10, 'data': base64.b64encode(b'<svg>').decode()})['ok'] is False, 'JPEG olmayan reddedilir')
check(act(1, {'action': 'ava_set', 'target': 10, 'data': base64.b64encode(b'\xff\xd8' + b'x' * 500_000).decode()})['ok'] is False, 'çok büyük reddedilir')
check(bot.AVA_TTL_H <= 6, 'otomatik yenileme sıklaştı')
print('ALL OK' if not FAIL else 'FAILED: %s' % FAIL)
raise SystemExit(1 if FAIL else 0)
