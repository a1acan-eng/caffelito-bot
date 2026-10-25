# Şube başına kişi sınırı kaldırıldı (owner 2026-10-25): aynı gün aynı şubeye istenildiği kadar kişi.
# Çalıştırma: rm -f /tmp/nc.db; PYTHONPATH=. DB_PATH=/tmp/nc.db BOT_TOKEN=123:abc python tests/grid_nocap_test.py
import json, asyncio, bot
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
db = bot.get_db()
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized,branch_id) VALUES (1,'Owner','owner',1,1,1)")
for uid in (10, 11, 12, 13):
    db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (?,?,'barista',1,1,1)", (uid, f'B{uid}'))
db.execute("INSERT OR REPLACE INTO branches (id,name,active) VALUES (1,'Magic',1)")
db.execute("INSERT OR REPLACE INTO shift_templates (code,branch_id,start_t,end_t,active) VALUES ('mgd',1,'07:00','16:30',1)")
db.commit()
wk = bot.grid_week_key(1)
for uid in (10, 11, 12, 13):
    act(1, {"action": "shift_grid_set", "target_uid": uid, "week": 1, "day": 2, "code": "mgd"})
n = db.execute("SELECT COUNT(*) FROM shift_grid WHERE week_key=? AND day=2 AND code='mgd'", (wk,)).fetchone()[0]
check(n == 4, f'aynı gün aynı şubeye 4 kişi yazıldı (max_staff=2 olsa da) — {n}')
ok, why = bot.grid_check(db, wk, 3, 99, "mgd") if hasattr(bot, "grid_check") else (True, "")
check(ok and "Максимум" not in why, 'kural motoru sınır mesajı vermiyor')
print('ALL OK' if not FAIL else 'FAILED: %s' % FAIL)
raise SystemExit(1 if FAIL else 0)
