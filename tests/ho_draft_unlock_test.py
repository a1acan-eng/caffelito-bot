# Owner kilitli devir taslağını açar (owner 2026-10-30): taslak silinir, çalışan yeniden kaydeder.
# Çalıştırma: rm -f /tmp/du.db; PYTHONPATH=. DB_PATH=/tmp/du.db BOT_TOKEN=123:abc python tests/ho_draft_unlock_test.py
import json, asyncio, bot
from datetime import datetime, timedelta
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
    ctx = bot._ShimContext(FB(), {}, uid=uid)
    asyncio.run(bot.handle_webapp_data(bot._ShimUpdate(FB(), uid, f'U{uid}', None, json.dumps(p)), ctx))
    return getattr(ctx, 'result', None)
db = bot.get_db()
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized) VALUES (1,'Owner','owner',1,1)")
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (10,'Дамир','barista',1,1,1)")
db.execute("INSERT OR REPLACE INTO branches (id, name, active) VALUES (1,'C5',1)")
db.commit()
now = datetime.now(bot.TZ).replace(tzinfo=None)
hm = lambda d: d.strftime('%H:%M')
bot.op_cfg_save(db, {"handover_flow": 1, "windows": {"1": [
    {"s": hm(now - timedelta(minutes=30)), "e": hm(now + timedelta(hours=9))},
    {"s": hm(now + timedelta(hours=8)), "e": hm(now + timedelta(hours=17))}]}})
db.commit()
act(10, {'action': 'shift_start', 'branch_id': 1})
sh = bot.get_active_shift(db, 10)
act(10, {'action': 'ho_draft_save', 'cups': [{'k': 'ml300', 's': 5}]})
check(bool(bot.ho_row_for_shift(db, sh["id"])["draft"]), 'taslak kaydedildi (kilitli)')
act(10, {'action': 'ho_draft_save', 'cups': [{'k': 'ml300', 's': 7}]})
check('"s": 5' in bot.ho_row_for_shift(db, sh["id"])["draft"], 'çalışan kilitliyi değiştiremez')
r = act(10, {'action': 'ho_draft_unlock', 'uid': 10})
check(r and not r.get('ok'), 'çalışan kendi kilidini açamaz')
r = act(1, {'action': 'ho_draft_unlock', 'uid': 10})
check(r and r.get('ok') and not bot.ho_row_for_shift(db, sh["id"])["draft"], 'owner açtı → taslak boş')
check(any(c == 10 and 'открыл ваш черновик' in (t or '') for c, t in SENT), 'çalışana bildirim')
check(db.execute("SELECT 1 FROM logs WHERE action='ho_draft_unlock'").fetchone() is not None, 'günlüğe yazıldı (eski taslakla)')
act(10, {'action': 'ho_draft_save', 'cups': [{'k': 'ml300', 's': 7}]})
check('"s": 7' in (bot.ho_row_for_shift(db, sh["id"])["draft"] or ''), 'çalışan yeniden kaydetti')
r = act(1, {'action': 'ho_draft_unlock', 'uid': 99})
check(r and not r.get('ok'), 'olmayan kişi → hata')
print('ALL OK' if not FAIL else 'FAILED: %s' % FAIL)
raise SystemExit(1 if FAIL else 0)
