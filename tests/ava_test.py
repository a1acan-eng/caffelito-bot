# Çalışanların Telegram avatarı (owner 2026-10-11): indir, sakla, imzalı bağlantı, gizli foto.
# Çalıştırma: rm -f /tmp/av.db; PYTHONPATH=. DB_PATH=/tmp/av.db BOT_TOKEN=123:abc python tests/ava_test.py
import json, asyncio, urllib.parse as up, bot
FAIL = []
def check(c, m):
    print(('  ok  ' if c else '  FAIL ') + m)
    if not c: FAIL.append(m)
class Sz:
    def __init__(s, w, fid): s.width, s.file_id, s.file_unique_id = w, fid, 'u' + fid
class Ph:
    def __init__(s, photos): s.photos = photos
class F:
    async def download_as_bytearray(s): return bytearray(b'\xff\xd8JPEGDATA')
class FB:
    calls = 0
    async def get_user_profile_photos(s, uid, limit=1):
        return Ph([[Sz(160, 'a'), Sz(320, 'b'), Sz(640, 'c')]]) if uid == 10 else Ph([])
    async def get_file(s, fid):
        FB.calls += 1; FB.last = fid; return F()
db = bot.get_db()
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (10,'Бек','barista',1,1,1)")
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (11,'Али','barista',1,1,1)")
db.commit()
fb = FB()
check(asyncio.run(bot.ava_fetch(fb, db, 10)) is True and FB.last == 'a', 'foto indirildi (en küçük ≥160px boy)')
check(asyncio.run(bot.ava_fetch(fb, db, 11)) is False, 'fotoğrafı olmayan/gizleyen: none')
asyncio.run(bot.ava_fetch(fb, db, 10))
check(FB.calls == 1, 'aynı foto tekrar indirilmez')
m = bot.ava_map(db)
check(set(m) == {'10'} and ('s=' + bot.ava_sig(10)) in m['10'], 'payload yalnız fotoğrafı olanı imzalı bağlantıyla verir')
s = bot.build_hash_payload(db, 10, 'x')
d = dict(p.split('=', 1) for p in s.lstrip('#').split('&') if '=' in p)
check('10' in json.loads(up.unquote(d['ava'])), 'çalışan payload\'ında ava var')
check(bot.ava_sig(10) != bot.ava_sig(11), 'imza kişiye özel')
print('FAIL' if FAIL else 'ALL OK', FAIL)
