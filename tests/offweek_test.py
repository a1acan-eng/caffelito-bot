# İzin yalnız GELECEK HAFTAYA + owner toplu temizleme (owner 2026-10-11).
# Çalıştırma: rm -f /tmp/ow.db; PYTHONPATH=. DB_PATH=/tmp/ow.db BOT_TOKEN=123:abc python tests/offweek_test.py
import json, asyncio, bot
FAIL = []
def check(c, m):
    print(('  ok  ' if c else '  FAIL ') + m)
    if not c: FAIL.append(m)
SAID = []
class FB:
    async def send_message(self, chat_id=None, text=None, *a, **k): SAID.append((chat_id, text)); return None
    def __getattr__(self, n):
        async def f(*a, **k): return None
        return f
class Msg:
    async def reply_text(self, t, *a, **k): SAID.append(('reply', t))
def act(uid, p):
    u = bot._ShimUpdate(FB(), uid, f'U{uid}', None, json.dumps(p))
    asyncio.run(bot.handle_webapp_data(u, bot._ShimContext(FB(), {}, uid=uid)))
db = bot.get_db()
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized) VALUES (1,'Owner','owner',1,1)")
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (10,'Бек','barista',1,1,1)")
db.execute("INSERT OR REPLACE INTO meta (k,val) VALUES ('weekly_off_limit','7')")
db.commit()
cnt = lambda w: db.execute("SELECT COUNT(*) c FROM shift_grid WHERE user_id=10 AND week_key=? AND code='off'", (bot.grid_week_key(w),)).fetchone()['c']
req = lambda w: db.execute("SELECT COUNT(*) c FROM dayoff_requests WHERE user_id=10 AND week_key=?", (bot.grid_week_key(w),)).fetchone()['c']
act(10, {'action': 'shift_grid_set', 'target_uid': 10, 'week': 0, 'day': 6, 'code': 'off'})
check(cnt(0) == 0, 'bu haftaya kendine izin koyamaz')
act(10, {'action': 'shift_grid_set', 'target_uid': 10, 'week': 2, 'day': 3, 'code': 'off'})
check(cnt(2) == 0, 'iki hafta sonrasına da koyamaz')
act(10, {'action': 'shift_grid_set', 'target_uid': 10, 'week': 1, 'day': 3, 'code': 'off'})
check(cnt(1) == 1, 'gelecek haftaya koyabilir')
act(10, {'action': 'dayoff_request', 'week': 0, 'day': 6, 'note': ''})
check(req(0) == 0, 'bu hafta için заявка da gönderemez')
act(10, {'action': 'dayoff_request', 'week': 1, 'day': 5, 'note': ''})
check(req(1) == 1, 'gelecek hafta için заявка gönderebilir')
check(bot.grid_off_week_msg(bot.grid_week_key(0)) and not bot.grid_off_week_msg(bot.grid_week_key(1)), 'mesaj: yalnız gelecek hafta')
# Owner: toplu temizleme
for d in range(7):
    db.execute("INSERT OR REPLACE INTO shift_grid (week_key, day, user_id, code) VALUES (?,?,10,?)", (bot.grid_week_key(1), d, 'off' if d < 5 else 'x'))
db.commit()
act(10, {'action': 'shift_grid_clear_week', 'target_uid': 10, 'week': 1, 'only_off': 1})
check(cnt(1) == 5, 'çalışan toplu temizleyemez')
act(1, {'action': 'shift_grid_clear_week', 'target_uid': 10, 'week': 1, 'only_off': 1})
check(cnt(1) == 0 and db.execute("SELECT COUNT(*) c FROM shift_grid WHERE user_id=10 AND week_key=?", (bot.grid_week_key(1),)).fetchone()['c'] == 2, 'owner yalnız OFF\'ları tek seferde sildi (vardiyalar kaldı)')
act(1, {'action': 'shift_grid_clear_week', 'target_uid': 10, 'week': 1})
check(db.execute("SELECT COUNT(*) c FROM shift_grid WHERE user_id=10 AND week_key=?", (bot.grid_week_key(1),)).fetchone()['c'] == 0, 'owner tüm haftayı temizledi')
act(1, {'action': 'shift_grid_set', 'target_uid': 10, 'week': 0, 'day': 6, 'code': 'off'})
check(cnt(0) == 1, 'owner bu haftaya da izin koyabilir')
print('FAIL' if FAIL else 'ALL OK', FAIL)
