# Geç kalan sabahçı (owner 2026-10-28, canlı: Дамир 07:00, Хасан 09:02 → «ikinci vardiya»
# sanılıp ücreti 16:30'a çekilmişti; owner 09:02'ye düzeltince ceza hiç yazılmamıştı).
# Kural: 2 saatlik pencereden sonra gelen, ikinci vardiyanın başlangıcından çok kökün
# başlangıcına YAKINSA sabahçıdır (paralel grup), cezası kökün planından ölçülür.
# Çalıştırma: rm -f /tmp/pl.db; PYTHONPATH=. DB_PATH=/tmp/pl.db BOT_TOKEN=123:abc python tests/par_late_test.py
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
for uid, nm in ((10, 'Дамир'), (11, 'Хасан'), (12, 'Бек'), (13, 'Акмаль')):
    db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (?,?,'barista',1,1,1)", (uid, nm))
db.execute("INSERT OR REPLACE INTO branches (id, name, active) VALUES (1,'C5',1)")
db.commit()
now = datetime.now(bot.TZ).replace(tzinfo=None).replace(second=0, microsecond=0)
hm = lambda d: d.strftime('%H:%M')
iso = lambda d: d.strftime('%Y-%m-%dT%H:%M')
r0 = now - timedelta(minutes=122)                      # «07:00»
b2 = r0 + timedelta(hours=9, minutes=30)               # «16:30»
bot.op_cfg_save(db, {"on": 1, "auto": 1, "grace": 10, "per_hour": 60000, "handover_flow": 1,
                     "windows": {"1": [{"s": hm(r0), "e": hm(r0 + timedelta(hours=10, minutes=30))},
                                       {"s": hm(b2), "e": hm(b2 + timedelta(hours=10))}]}})
db.commit()
fine = lambda sid: db.execute("SELECT * FROM opening_delays WHERE shift_id=?", (sid,)).fetchone()
# 1) Дамир 07:00, Хасан 09:02 kendisi açar
act(10, {'action': 'shift_start', 'branch_id': 1, 'start_time': iso(r0)})
a = bot.get_active_shift(db, 10)
check(a is not None and hm(datetime.fromisoformat(a["start_time"])) == hm(r0), 'Дамир «07:00»de açtı')
act(11, {'action': 'shift_start', 'branch_id': 1})
h = bot.get_active_shift(db, 11)
check(h is not None and h["par_of"] == a["id"], '09:02 gelen sabah grubuna katıldı (ikinci vardiya DEĞİL)')
check(abs((datetime.fromisoformat(h["start_time"]) - now).total_seconds()) < 180, 'ücret geldiği saatten (16:30a çekilmedi)')
check(bot.ho_row_for_shift(db, a["id"])["b2_arrived_at"] is None, 'Дамир için devir başlamadı')
f = fine(h["id"])
check(f is not None and int(f["delay_min"]) >= 121 and hm(datetime.fromisoformat(f["scheduled"])) == hm(r0),
      f'ceza «07:00»den ölçüldü ({f and f["delay_min"]} dk)')
check(f is not None and int(f["amount"] or 0) > 0, 'ceza yazıldı')
act(11, {'action': 'shift_end', 'drinks': {}, 'light': 1})
act(10, {'action': 'shift_end', 'drinks': {"ml300": 5}})
db.execute("DELETE FROM opening_delays"); db.commit()
# 2) Paralel pencere içinde (08:30) gelen de 07:00'den geç sayılır
r1 = now - timedelta(minutes=90)
act(10, {'action': 'shift_start', 'branch_id': 1, 'start_time': iso(r1)})
a = bot.get_active_shift(db, 10)
act(12, {'action': 'shift_start', 'branch_id': 1})
q = bot.get_active_shift(db, 12)
f = fine(q["id"])
check(q["par_of"] == a["id"] and f is not None and int(f["delay_min"]) >= 89, 'pencere içinde gelen paralel de geç sayıldı')
act(12, {'action': 'shift_end', 'drinks': {}, 'light': 1})
act(10, {'action': 'shift_end', 'drinks': {}})
# 3) 16:30'a yakın gelen hâlâ ikinci vardiya (erken gelen sменщик)
r2 = now - timedelta(hours=7)
db.execute("DELETE FROM handover"); db.execute("DELETE FROM shifts"); db.commit()
bot.op_cfg_save(db, {"windows": {"1": [{"s": hm(r2), "e": hm(r2 + timedelta(hours=10, minutes=30))},
                                       {"s": hm(r2 + timedelta(hours=9, minutes=30)), "e": hm(r2 + timedelta(hours=19))}]}})
act(10, {'action': 'shift_start', 'branch_id': 1, 'start_time': iso(r2)})
a = bot.get_active_shift(db, 10)
act(13, {'action': 'shift_start', 'branch_id': 1})
s = bot.get_active_shift(db, 13)
check(s is not None and not s["par_of"], '«14:00»de gelen ikinci vardiya (gruba katılmadı)')
check(datetime.fromisoformat(s["start_time"]) > now + timedelta(hours=2), 'ikinci vardiyanın ücreti plandan (16:30)')
db.execute("DELETE FROM shifts")
db.execute("DELETE FROM handover"); db.execute("DELETE FROM opening_delays"); db.commit()
# 4) Eski davranış (canlıdaki Хасан): ikinci vardiya sanılmış, owner saati 09:02'ye düzeltir
bot.op_cfg_save(db, {"windows": {"1": [{"s": hm(r0), "e": hm(r0 + timedelta(hours=10, minutes=30))},
                                       {"s": hm(b2), "e": hm(b2 + timedelta(hours=10))}]}})
act(10, {'action': 'shift_start', 'branch_id': 1, 'start_time': iso(r0)})
a = bot.get_active_shift(db, 10)
bot.ho_get_or_create(db, bot.op_cfg(db), a)
sid = bot.start_shift(db, 11, custom_start=iso(now), branch_id=1)["id"]
db.execute("UPDATE shifts SET start_time=? WHERE id=?", (b2.isoformat(), sid))
db.execute("UPDATE handover SET b2_uid=11, b2_name='Хасан', b2_arrived_at=? WHERE shift_id=?", (b2.isoformat(), a["id"]))
db.commit()
act(1, {'action': 'edit_shift', 'id': sid, 'start_time': iso(now)})
h = db.execute("SELECT * FROM shifts WHERE id=?", (sid,)).fetchone()
check(h["par_of"] == a["id"], 'owner düzeltince vardiya sabah grubuna alındı')
check(bot.ho_row_for_shift(db, a["id"])["b2_uid"] is None, 'Дамир satırındaki yanlış devir işareti geri alındı')
f = fine(sid)
check(f is not None and int(f["delay_min"]) >= 121, f'düzeltmeden sonra ceza «07:00»den yazıldı ({f and f["delay_min"]} dk)')
check(db.execute("SELECT 1 FROM logs WHERE action='par_regroup'").fetchone() is not None, 'günlüğe yazıldı')
# 5) pencere 0 = kapalı → gruplama yok (eski kural)
bot.op_cfg_save(db, {"par_window": 0})
check(bot.par_window_root(db, 12, 1, now) is None, 'pencere kapalıyken gruplama yok')
print('ALL OK' if not FAIL else 'FAILED: %s' % FAIL)
raise SystemExit(1 if FAIL else 0)
