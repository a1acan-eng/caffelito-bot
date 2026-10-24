# Paralel pencere + DEVİR AKIŞI açık (owner 2026-10-24): birincinin başlangıcından 2 saat içinde
# gelen barista «erken gelen sменщик» SAYILMAZ — ücreti geldiği saatten, devir başlamaz.
# Pencere kapalıysa (0) eski davranış: ikinci vardiya, ücret plan saatinden.
# Çalıştırma: rm -f /tmp/pw.db; PYTHONPATH=. DB_PATH=/tmp/pw.db BOT_TOKEN=123:abc python tests/par_window_ho_test.py
import json, asyncio, bot
from datetime import datetime, timedelta
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
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized) VALUES (1,'Owner','owner',1,1)")
for uid, nm in ((10, 'Бек'), (11, 'Убай'), (12, 'Хасан')):
    db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (?,?,'barista',1,1,1)", (uid, nm))
db.execute("INSERT OR REPLACE INTO branches (id, name, active) VALUES (1,'C5',1)")
db.commit()
now = datetime.now(bot.TZ).replace(tzinfo=None)
hm = lambda d: d.strftime('%H:%M')
bot.op_cfg_save(db, {"handover_flow": 1, "windows": {"1": [
    {"s": hm(now - timedelta(minutes=30)), "e": hm(now + timedelta(hours=9))},
    {"s": hm(now + timedelta(hours=8)), "e": hm(now + timedelta(hours=17))}]}})
db.commit()
check(bot.ho_enabled(None, db), 'devir akışı açık')
act(10, {'action': 'shift_start', 'branch_id': 1})
a = bot.get_active_shift(db, 10)
hr = bot.ho_row_for_shift(db, a["id"])
check(hr is not None and hr["b2_arrived_at"] is None, 'birincinin devir satırı açık')
# Paralel gelen (pencere içinde)
act(11, {'action': 'shift_start', 'branch_id': 1})
p = bot.get_active_shift(db, 11)
check(p is not None and p["par_of"] == a["id"], 'ikinci barista paralel gruba katıldı')
check(abs((datetime.fromisoformat(p["start_time"]) - now).total_seconds()) < 300, 'ücret GELDİĞİ saatten (plana çekilmedi)')
check(bot.ho_row_for_shift(db, a["id"])["b2_arrived_at"] is None, 'birincinin devri BAŞLAMADI (paralel sменщик değil)')
check(bot.ho_row_for_shift(db, p["id"]) is None, 'paralel gelene ayrı devir satırı açılmadı')
# Birinci, напарник açıkken erken çıkabilir (kilit yok) — yalnız saat
act(10, {'action': 'shift_end', 'drinks': {}, 'light': 1})
check(bot.get_active_shift(db, 10) is None, 'birinci (hafif) erken çıkabildi — kilit yok')
check(not bot.ho_row_for_shift(db, a["id"])["unauth"], 'izinsiz erken çıkış YAZILMADI')
act(11, {'action': 'shift_end', 'drinks': {"ml300": 10}})
check(bot.get_active_shift(db, 11) is None, 'son kalan tam kapattı')
# Pencere kapalı → eski davranış: ikinci vardiya, ücret plandan
bot.op_cfg_save(db, {"par_window": 0})
act(10, {'action': 'shift_start', 'branch_id': 1})
a2 = bot.get_active_shift(db, 10)
act(12, {'action': 'shift_start', 'branch_id': 1})
q = bot.get_active_shift(db, 12)
check(q is not None and not q["par_of"], 'pencere kapalıyken ikinci vardiya (grup yok)')
check(datetime.fromisoformat(q["start_time"]) > now + timedelta(hours=7), 'ikinci vardiyanın ücreti plan saatinden (eski kural)')
# Ayar: owner Nero'dan değiştirir, barista değiştiremez
act(1, {'action': 'op_cfg_save', 'par_window': 90})
check(bot.par_window_min(db) == 90, 'owner pencereyi 90 dk yaptı')
act(10, {'action': 'op_cfg_save', 'par_window': 300})
check(bot.par_window_min(db) == 90, 'barista pencereyi değiştiremez')
check(bot.op_cfg(db).get("handover_flow") == 1, 'diğer ayarlar korunuyor (devir açık)')
print('ALL OK' if not FAIL else 'FAILED: %s' % FAIL)
raise SystemExit(1 if FAIL else 0)
