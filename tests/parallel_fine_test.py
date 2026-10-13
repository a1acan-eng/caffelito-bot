# Paralel vardiya açılış gecikmesi cezasına «şube penceresine geç» SAYILMAZ (owner 2026-10-13).
# Çalıştırma: rm -f /tmp/pf.db; PYTHONPATH=. DB_PATH=/tmp/pf.db BOT_TOKEN=123:abc python tests/parallel_fine_test.py
import json, bot
from datetime import datetime, timedelta
FAIL = []
def check(c, m):
    print(('  ok  ' if c else '  FAIL ') + m)
    if not c: FAIL.append(m)
db = bot.get_db()
db.execute("INSERT OR REPLACE INTO branches (id,name,sort_order,active) VALUES (2,'Magic',2,1)")
db.execute("INSERT OR REPLACE INTO meta (k,val) VALUES ('opening_penalty', ?)",
           (json.dumps({"on": 1, "grace": 10, "per_hour": 60000, "auto": 0, "branches": [],
                        "windows": {"2": [{"s": "07:00", "e": "17:00"}]}}),))
db.execute("INSERT OR REPLACE INTO meta (k,val) VALUES ('op_grace_migrated','1')")
for uid, nm in ((30, 'Обычный'), (31, 'Параллельный'), (32, 'ПараллельныйПлан')):
    db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (?,?,'barista',2,1,1)", (uid, nm))
db.commit()
d = datetime.now(bot.TZ).replace(tzinfo=None).date()
st = datetime(d.year, d.month, d.day, 12, 0)
def mk(uid, note):
    cur = db.execute("INSERT INTO shifts (user_id,hours,drinks,date,period,created_at,start_time,note,branch_id,shift_role) "
                     "VALUES (?,0,'{}',?,?,?,?,?,2,'barista')", (uid, st.strftime('%Y-%m-%d'), st.strftime('%Y-%m'),
                                                                    st.isoformat(), st.isoformat(), note))
    db.commit()
    return db.execute("SELECT * FROM shifts WHERE id=?", (cur.lastrowid,)).fetchone()
r1 = bot.op_register(db, mk(30, ''))
check(r1 is not None, 'normal vardiya 12:00 → 07:00 penceresine göre gecikme yazıldı (eski davranış korunur)')
why = []
r2 = bot.op_register(db, mk(31, 'параллельная смена'), why)
check(r2 is None, 'paralel vardiya 12:00 → ceza YOK')
check(any('параллельная' in w for w in why), 'sebep owner\'a yazıldı: ' + '; '.join(why))
# Paralel ama kişinin kendi График planı 11:00 → 60 dk geç sayılır (kendi planı geçerli)
mon = d - timedelta(days=d.weekday())
db.execute("INSERT OR REPLACE INTO shift_templates (code, branch_id, start_t, end_t, active) VALUES ('P11', 2, '11:00', '20:00', 1)")
db.execute("INSERT OR REPLACE INTO shift_grid (week_key, day, user_id, code) VALUES (?,?,?,?)", (mon.isoformat(), d.weekday(), 32, 'P11'))
_c = json.loads(db.execute("SELECT val FROM meta WHERE k='opening_penalty'").fetchone()['val']); _c['codes'] = ['P11']
db.execute("INSERT OR REPLACE INTO meta (k,val) VALUES ('opening_penalty', ?)", (json.dumps(_c),))
db.commit()
r3 = bot.op_register(db, mk(32, 'параллельная смена'))
check(r3 is not None, 'paralel ama График planı 11:00 olan kişi 12:00 gelince kendi planına göre gecikme yazıldı')
print('ALL OK' if not FAIL else 'FAILED: %s' % FAIL)
