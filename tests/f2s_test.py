# Штрафы → в зарплату (owner 2026-10-29/30): açık cezalar (en eskiden, bölünmeden) bu ayın
# maaşına geçer ve «в зарплату» olarak KAPANIR. Elden alınan («получено») ceza havuza girmez.
# Alınan ceza kilitli (geri açılmaz, silinmez). Owner maaşını aldıktan sonra aktarım geri alınmaz.
# Çalıştırma: rm -f /tmp/fs.db; PYTHONPATH=. DB_PATH=/tmp/fs.db BOT_TOKEN=123:abc python tests/f2s_test.py
import json, asyncio, bot
from datetime import datetime, timedelta
FAIL = []
def check(c, m):
    print(('  ok  ' if c else '  FAIL ') + m)
    if not c: FAIL.append(m)
SAID = []
class FB:
    async def send_message(self, chat_id=None, text=None, *a, **k): SAID.append(text); return None
    def __getattr__(self, n):
        async def f(*a, **k): return None
        return f
def act(uid, p):
    ctx = bot._ShimContext(FB(), {}, uid=uid)
    asyncio.run(bot.handle_webapp_data(bot._ShimUpdate(FB(), uid, f'U{uid}', None, json.dumps(p)), ctx))
    return getattr(ctx, 'result', None)
db = bot.get_db()
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized,branch_id) VALUES (1,'Owner','owner',1,1,1)")
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (10,'Дамир','barista',1,1,1)")
db.execute("INSERT OR REPLACE INTO branches (id, name, active) VALUES (1,'C5',1)")
db.execute("INSERT OR REPLACE INTO meta (k,val) VALUES ('f2s_hand_once_1','x')")   # bir kerelik kapatma bu testte yok
per = bot.current_period(); t0 = datetime.now(bot.TZ) - timedelta(days=2)
def fine(uid, amt, typ, by, k):
    return db.execute("INSERT INTO fines (user_id,amount,reason,type,period,added_by,created_at) VALUES (?,?,?,?,?,?,?)",
                      (uid, amt, 'т', typ, per, by, (t0 + timedelta(minutes=k)).isoformat())).lastrowid
f1 = fine(10, 50000, 'manual', 1, 1)            # en eski
f2 = fine(10, 30000, 'opening_delay', 0, 2)     # sistem → birincil owner
f3 = fine(10, 20000, 'manual', 1, 3)
fine(10, 1000000, 'insp_70', 1, 4)              # marka → sayılmaz
fine(1, 20000, 'manual', 1, 5)                  # kendi üstüne → sayılmaz
db.commit()
net0 = bot.calc_summary(db, 1, per)["net"]
check(bot.f2s_pool(db, 1) == (100000, 0, 100000), f'havuz: açık 100.000 {bot.f2s_pool(db, 1)}')
r = act(10, {'action': 'f2s_move', 'amount': 1000})
check(r and not r.get('ok'), 'barista geçiremez')
r = act(1, {'action': 'f2s_move', 'amount': 30000})
check(r and not r.get('ok') and '50' in r.get('error', ''), 'en eski ceza bölünmez → «минимум 50.000»')
r = act(1, {'action': 'f2s_move', 'amount': 90000})
check(r and r.get('ok') and r['f2s']['rows'][0]['amount'] == 80000 and r['f2s']['left'] == 20000,
      'istenen 90.000 → tam cezalar 50+30 = 80.000 geçti, açık 20.000')
check(bot.calc_summary(db, 1, per)["net"] == net0 + 80000, 'owner maaşı +80.000')
v = {x["id"]: (x["settled"], x["settle_via"]) for x in db.execute("SELECT id, settled, settle_via FROM fines")}
check(v[f1] == (1, 'salary') and v[f2] == (1, 'salary') and v[f3][0] == 0, 'geçen cezalar «в зарплату» olarak kapandı')
# elden alındı → havuzdan çıkar, kilitli
act(1, {'action': 'fine_settle', 'id': f3, 'settled': 1})
check(bot.f2s_pool(db, 1)[2] == 0, 'elden alınan ceza havuzda yok (alacak gibi görünmez)')
act(1, {'action': 'fine_settle', 'id': f3, 'settled': 0})
check(db.execute("SELECT settled FROM fines WHERE id=?", (f3,)).fetchone()["settled"] == 1, '«получено» geri alınamaz')
act(1, {'action': 'delete_record', 'kind': 'fine', 'id': f1})
check(db.execute("SELECT 1 FROM fines WHERE id=?", (f1,)).fetchone() is not None, 'alınmış ceza silinemez')
# aktarım: maaş alınmadan geri alınabilir
aid = bot.f2s_view(db, 1)['rows'][0]['id']
check(bot.f2s_view(db, 1)['rows'][0]['lock'] == 0, 'maaş ödenmeden kilitli değil')
r = act(1, {'action': 'f2s_undo', 'id': aid})
check(r and r.get('ok') and r['f2s']['left'] == 80000, 'geri alındı → cezalar yeniden açık')
check(bot.calc_summary(db, 1, per)["net"] == net0, 'maaştan düştü')
r = act(1, {'action': 'f2s_move', 'amount': 80000})
aid = r['f2s']['rows'][0]['id']
db.execute("INSERT INTO payments (user_id,amount,period,paid_by,paid_by_name,paid_at) VALUES (1,500000,?,1,'Owner',?)",
           (per, (datetime.now(bot.TZ) + timedelta(seconds=5)).isoformat()))
db.commit()
check(bot.f2s_view(db, 1)['rows'][0]['lock'] == 1, 'maaş alındıktan sonra aktarım geçmişte (kilitli)')
r = act(1, {'action': 'f2s_undo', 'id': aid})
check(r and not r.get('ok'), 'maaş alındıktan sonra geri alınamaz')
# eski (tutar bazlı) aktarım göçü
f4 = fine(10, 40000, 'manual', 1, 10); f5 = fine(10, 60000, 'manual', 1, 11); db.commit()
db.execute("INSERT INTO adjustments (user_id,amount,note,period,branch_id,added_by,added_by_name,created_at) "
           "VALUES (1,40000,'Из штрафов в зарплату [f2s]',?,1,1,'Owner',?)", (per, datetime.now(bot.TZ).isoformat()))
db.commit()
check(bot.f2s_pool(db, 1)[2] == 60000, 'eski aktarım en eski açık cezayla eşleşti (havuz 60.000)')
check(db.execute("SELECT settle_via FROM fines WHERE id=?", (f4,)).fetchone()["settle_via"] == 'salary', 'göç: ceza «в зарплату»')
# alınmış açılış cezası: saat düzeltmesi / iptal dokunmaz
oid = db.execute("INSERT INTO opening_delays (user_id,branch_id,shift_id,status,fine_id,amount) VALUES (10,1,999,'approved',?,30000)",
                 (f2,)).lastrowid
db.commit()
_w = []
check(bot.op_recalc(db, 999, _w) == (None, 0) and 'закрыт' in _w[-1], 'alınmış ceza yeniden hesaplanmaz')
check(bot.op_reject(db, oid, 1, 'O')[0] is None and db.execute("SELECT 1 FROM fines WHERE id=?", (f2,)).fetchone(),
      'alınmış ceza iptal edilemez')
# «Все получены» TÜM AYLAR: önceki ayın açık cezası da kapanır
fo = db.execute("INSERT INTO fines (user_id,amount,reason,type,period,added_by,created_at) VALUES (10,70000,'т','manual','2026-08',1,'2026-08-05T10:00:00')").lastrowid
db.commit()
check(bot.f2s_pool(db, 1)[2] == 130000, 'açık: bu ay 60.000 + ağustos 70.000')
act(1, {'action': 'fine_settle_all', 'all': 1})
check(bot.f2s_pool(db, 1)[2] == 0 and db.execute("SELECT settle_via FROM fines WHERE id=?", (fo,)).fetchone()[0] == 'hand',
      '«все получены» tüm aylar → доступно 0')
# bir kerelik kapatma (owner 2026-10-31): eşikten önceki açık cezalar elden alınmış sayılır
fx = db.execute("INSERT INTO fines (user_id,amount,reason,type,period,added_by,created_at) VALUES (10,270000,'т','manual','2026-10','1','2026-10-20T10:00:00')").lastrowid
db.execute("DELETE FROM meta WHERE k='f2s_hand_once_1'"); db.commit()
check(bot.f2s_pool(db, 1)[2] == 0 and db.execute("SELECT settled FROM fines WHERE id=?", (fx,)).fetchone()[0] == 1, 'bir kerelik: eski açık 270.000 kapandı')
fy = db.execute("INSERT INTO fines (user_id,amount,reason,type,period,added_by,created_at) VALUES (10,15000,'т','manual','2026-10','1','2026-10-20T11:00:00')").lastrowid
db.commit()
check(bot.f2s_pool(db, 1)[2] == 15000, 'bir kerelik kapatma tekrar çalışmaz')
print('ALL OK' if not FAIL else 'FAILED: %s' % FAIL)
raise SystemExit(1 if FAIL else 0)
