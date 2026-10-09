# Devir taslağı («Сохранить черновик») → 17:30 raporu: «Долги гостей» kaybolmasın (owner 2026-10-09).
# Çalıştırma: rm -f /tmp/hg.db; PYTHONPATH=. DB_PATH=/tmp/hg.db BOT_TOKEN=123:abc python tests/ho_gdebt_test.py
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
db = bot.get_db()
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (10,'Бек','barista',1,1,1)")
gid = db.execute("INSERT INTO guests (name, branch_id, free, created_at) VALUES ('Ислом ака',1,1,?)",
                 (datetime.now(bot.TZ).isoformat(),)).lastrowid
now = datetime.now(bot.TZ)
st = now.replace(tzinfo=None) - timedelta(hours=9)
sid = db.execute("INSERT INTO shifts (user_id,hours,total,date,period,created_at,start_time,branch_id) VALUES (10,0,0,?,?,?,?,1)",
                 (st.strftime('%Y-%m-%d'), st.strftime('%Y-%m'), st.isoformat(), st.isoformat())).lastrowid
db.execute("INSERT INTO handover (shift_id,user_id,user_name,branch_id,date,sched_end,created_at) VALUES (?,10,'Бек',1,?,?,?)",
           (sid, now.strftime('%Y-%m-%d'), now.isoformat(), now.isoformat()))
db.commit()
row = bot.ho_row_for_shift(db, sid)
row, err = bot.ho_draft_save(db, row, {"cups": [], "expenses": [{"n": "Свои · Ислом ака", "a": 1800000}], "note": "",
                                        "daily_pay": 0, "hours": 9, "start_time": st.isoformat(), "branch_id": 1,
                                        "gdebts": [{"g": gid, "n": "Ислом ака", "a": 1800000, "k": "debt"}]})
check(not err and "gdebts" in json.loads(row["draft"]), 'taslak gdebts saklıyor')
asyncio.run(bot.ho_finalize(FB(), {}, db, row, "sched_end"))
tx = db.execute("SELECT kind, amount FROM guest_tx WHERE guest_id=?", (gid,)).fetchall()
check([(r["kind"], r["amount"]) for r in tx] == [("free", 1800000)], 'rapor taslaktan gidince Свой kaydı yazıldı (free 1.800.000)')
print('FAIL' if FAIL else 'ALL OK', FAIL)
