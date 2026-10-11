# Çizelge bildirimleri owner «Уведомить» deyince gider (owner 2026-10-11).
# Çalıştırma: rm -f /tmp/gn.db; PYTHONPATH=. DB_PATH=/tmp/gn.db BOT_TOKEN=123:abc python tests/grid_notify_test.py
import json, asyncio, urllib.parse as up, bot
FAIL = []
def check(c, m):
    print(('  ok  ' if c else '  FAIL ') + m)
    if not c: FAIL.append(m)
SENT = []
class FB:
    async def send_message(self, chat_id=None, text=None, *a, **k): SENT.append((chat_id, text)); return None
    def __getattr__(self, n):
        async def f(*a, **k): return None
        return f
def act(uid, p):
    asyncio.run(bot.handle_webapp_data(bot._ShimUpdate(FB(), uid, f'U{uid}', None, json.dumps(p)), bot._ShimContext(FB(), {}, uid=uid)))
db = bot.get_db()
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized) VALUES (1,'Owner','owner',1,1)")
for u, n in ((10, 'Бек'), (11, 'Али')):
    db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (?,?,'barista',1,1,1)", (u, n))
db.execute("INSERT OR REPLACE INTO branches (id, name, active) VALUES (1,'C5',1)")
db.execute("INSERT OR REPLACE INTO shift_templates (code, branch_id, start_t, end_t, active) VALUES ('d1',1,'07:00','17:30',1)")
db.commit()
dm = lambda u: [t for c, t in SENT if c == u and str(t).startswith('🗓 Ваш график')]
act(1, {'action': 'shift_grid_set', 'target_uid': 10, 'week': 1, 'day': 2, 'code': 'd1'})
act(1, {'action': 'shift_grid_set', 'target_uid': 10, 'week': 1, 'day': 3, 'code': 'off'})
act(1, {'action': 'shift_grid_set', 'target_uid': 11, 'week': 1, 'day': 4, 'code': 'd1'})
act(1, {'action': 'shift_grid_set', 'target_uid': 11, 'week': 1, 'day': 4, 'code': 'none'})   # geri alındı
check(dm(10) == [] and dm(11) == [], 'owner değiştirirken çalışana anında mesaj GİTMEZ')
ch = bot.grid_pending_changes(db)
check(set(ch) == {10} and len(ch[10]) == 2, 'bekleyen: yalnız gerçekten değişen günler (geri alınan Али yok)')
s = bot.build_hash_payload(db, 1, 'x')
d = dict(p.split('=', 1) for p in s.lstrip('#').split('&') if '=' in p)
gp = json.loads(up.unquote(d['grid_pend']))
check(gp == [{'uid': 10, 'n': 'Бек', 'd': 2}], 'owner payload\'ında bekleyen liste')
act(10, {'action': 'grid_notify_send'})
check(dm(10) == [], 'çalışan bildirim gönderemez')
act(1, {'action': 'grid_notify_send'})
m = dm(10)
check(len(m) == 1 and '07:00–17:30 · C5' in m[0] and 'выходной' in m[0], 'Бек TEK özet mesaj aldı (iki gün birlikte)')
check(dm(11) == [], 'Али\'ye (geri alınan) mesaj gitmedi')
check(bot.grid_pending_changes(db) == {}, 'gönderince bekleyen liste temizlendi')
act(1, {'action': 'shift_grid_set', 'target_uid': 11, 'week': 1, 'day': 5, 'code': 'd1'})
act(1, {'action': 'grid_notify_drop'})
check(dm(11) == [] and bot.grid_pending_changes(db) == {}, '«без уведомлений»: mesajsız temizlenir')
act(10, {'action': 'shift_grid_set', 'target_uid': 10, 'week': 1, 'day': 6, 'code': 'off'})
check(bot.grid_pending_changes(db) == {}, 'çalışanın kendi izni bekleyene girmez')
print('FAIL' if FAIL else 'ALL OK', FAIL)
