# Erken gelen AKŞAMCI 1. barista sayılmaz (owner 2026-11-06, canlı: Akmal plan 15:30, 14:29'da
# geldi → sabah penceresine düştü, 16:30'da «сменщик не пришёл — сверхурочно» başladı).
# Devir penceresi kişinin График planına göre seçilir; yanlış açılmış satır silinir.
# Çalıştırma: rm -f /tmp/ha.db; PYTHONPATH=. DB_PATH=/tmp/ha.db BOT_TOKEN=123:abc python tests/ho_anchor_test.py
import bot
from datetime import datetime, date, timedelta
FAIL = []
def check(c, m):
    print(('  ok  ' if c else '  FAIL ') + m)
    if not c: FAIL.append(m)
bot.get_db()
db = bot.get_db()
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized) VALUES (1,'Owner','owner',1,1)")
for uid, nm in ((10, 'Akmal'), (11, 'Suhrob'), (12, 'Ali')):
    db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (?,?,'barista',1,1,1)", (uid, nm))
db.execute("INSERT OR REPLACE INTO branches (id, name, active) VALUES (1,'Magic',1)")
for c, s, e in (('mgd', '07:00', '16:30'), ('mgn', '15:30', '01:00')):
    db.execute("INSERT OR REPLACE INTO shift_templates (code, branch_id, start_t, end_t, active) VALUES (?,1,?,?,1)", (c, s, e))
D = date(2026, 11, 6)
wk = (D - timedelta(days=D.weekday())).isoformat()
for uid, code in ((10, 'mgn'), (11, 'mgd')):
    db.execute("INSERT OR REPLACE INTO shift_grid (week_key, day, user_id, code) VALUES (?,?,?,?)", (wk, D.weekday(), uid, code))
db.commit()
bot.op_cfg_save(db, {"handover_flow": 1, "windows": {"1": [{"s": "07:00", "e": "16:30"}, {"s": "15:30", "e": "01:00"}]}})
cfg = bot.op_cfg(db)
def sh(uid, h, m):
    st = datetime(D.year, D.month, D.day, h, m)
    sid = db.execute("INSERT INTO shifts (user_id,hours,date,period,created_at,start_time,branch_id,shift_role) "
                     "VALUES (?,0,?,?,?,?,1,'barista')", (uid, D.isoformat(), '2026-11', st.isoformat(), st.isoformat())).lastrowid
    db.commit()
    return db.execute("SELECT * FROM shifts WHERE id=?", (sid,)).fetchone()
s_suh = sh(11, 6, 58)
r = bot.ho_get_or_create(db, cfg, s_suh)
check(r is not None and r['sched_end'].endswith('16:30:00') and r['b2_start'].endswith('15:30:00'), 'Suhrob (sabahçı) 1. barista: 16:30, sменщик 15:30')
s_akm = sh(10, 14, 29)
check(bot.ho_anchor(db, 10, datetime(2026, 11, 6, 14, 29)) == datetime(2026, 11, 6, 15, 30), 'Akmal için çapa = planı 15:30')
check(bot.ho_get_or_create(db, cfg, s_akm) is None, 'Akmal (akşamcı, 61 dk erken) 1. barista SAYILMAZ')
s_ali = sh(12, 14, 29)
r2 = bot.ho_get_or_create(db, cfg, s_ali)
check(r2 is not None and r2['sched_end'].endswith('16:30:00'), 'planı olmayan kişide eski davranış (geliş saatine göre)')
db.execute("DELETE FROM handover WHERE shift_id=?", (s_ali['id'],)); db.commit()
# Canlıdaki gibi YANLIŞ açılmış satır: ho_tick 16:31'de onu siler, sverхурочно başlatmaz
db.execute("INSERT INTO handover (shift_id,user_id,user_name,branch_id,date,period,sched_start,sched_end,b2_start,created_at,extra_from) "
           "VALUES (?,10,'Akmal',1,?,?,?,?,?,?,?)", (s_akm['id'], D.isoformat(), '2026-11', '2026-11-06T07:00:00',
           '2026-11-06T16:30:00', '2026-11-06T15:30:00', 'x', '2026-11-06T16:30:00'))
db.commit()
out = bot.ho_tick(db, datetime(2026, 11, 6, 16, 31), cfg)
check(not any(int(r_['user_id']) == 10 for r_, _ in out), 'Akmal için «сменщик не пришёл» yok')
check(db.execute("SELECT 1 FROM handover WHERE shift_id=?", (s_akm['id'],)).fetchone() is None, 'yanlış devir satırı silindi')
check(db.execute("SELECT 1 FROM handover WHERE shift_id=?", (s_suh['id'],)).fetchone() is not None, 'Suhrob\'un doğru satırı duruyor')
print('ALL OK' if not FAIL else 'FAILED: %s' % FAIL)
raise SystemExit(1 if FAIL else 0)
