# Проверка: кто был на смене + премия ≥95% / удержание (owner 2026-11-15).
# Çalıştırma: rm -f /tmp/im.db; PYTHONPATH=. DB_PATH=/tmp/im.db BOT_TOKEN=123:abc python tests/insp_money_test.py
import json, asyncio, bot
from datetime import datetime, timedelta
FAIL = []
def check(c, m):
    print(('  ok  ' if c else '  FAIL ') + m)
    if not c: FAIL.append(m)
sent = []
class FB:
    async def send_message(self, chat_id=None, text=None, *a, **k): sent.append((chat_id, text))
    def __getattr__(self, n):
        async def f(*a, **k): return None
        return f
def act(uid, p):
    ctx = bot._ShimContext(FB(), {}, uid=uid)
    asyncio.run(bot.handle_webapp_data(bot._ShimUpdate(FB(), uid, f'U{uid}', None, json.dumps(p)), ctx))
    return getattr(ctx, 'result', None)
bot.get_db()
db = bot.get_db()
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized,branch_id) VALUES (1,'Owner','owner',1,1,1)")
for uid, nm in ((10, 'Damir'), (11, 'Ubay'), (12, 'Karina')):
    db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (?,?,'barista',1,1,1)", (uid, nm))
db.execute("INSERT OR REPLACE INTO branches (id,name,active,group_chat_id) VALUES (1,'C5',1,'-100')")
now = datetime.now(bot.TZ).replace(tzinfo=None)
for uid in (10, 11):
    db.execute("INSERT INTO shifts (user_id,hours,total,date,period,created_at,start_time,branch_id,shift_role,rate) "
               "VALUES (?,0,0,?,?,?,?,1,'barista',12000)", (uid, now.strftime('%Y-%m-%d'), now.strftime('%Y-%m'),
                                                          (now - timedelta(hours=2)).isoformat(), (now - timedelta(hours=2)).isoformat()))
db.commit()
per = bot.current_period()
adj = lambda uid: [dict(r) for r in db.execute("SELECT * FROM adjustments WHERE user_id=? AND note LIKE '%[insp:%'", (uid,))]
# adaylar: şu an vardiyadakiler önce, işaretli
c = bot.insp_candidates(db, 1)
check([x['id'] for x in c if x['on']] == [10, 11] and 12 in [x['id'] for x in c], f'adaylar ({c})')
# 1) 96% → listede olmayan (people yok) → şu an vardiyadakilere prim 300.000
sent.clear()
act(1, {'action': 'cps_insp_add', 'branch_id': 1, 'score': 96.5, 'period': per})
check(not adj(10) and not adj(11), 'kişi seçilmedi + vardiyada 2 kişi → kimseye yazılmaz')
r0 = db.execute("SELECT id FROM cps_inspections ORDER BY id DESC").fetchone()
act(1, {'action': 'cps_insp_del', 'id': r0['id']})
act(1, {'action': 'cps_insp_add', 'branch_id': 1, 'score': 96.5, 'people': [10], 'period': per})
check(len(adj(10)) == 1 and adj(10)[0]['amount'] == 600000 and not adj(11) and not adj(12), f'96.5 → yalnız seçilen kişiye 600.000 ({adj(10)})')
check(any(ch == 10 and 'Сюрприз' in (tx or '') for ch, tx in sent), 'kişiye DM')
check(any(str(ch) == '-100' and 'Сюрприз-премия' in (tx or '') for ch, tx in sent), 'gruba duyuru')
r = db.execute("SELECT * FROM cps_inspections ORDER BY id DESC").fetchone()
check([p['n'] for p in json.loads(r['people'])] == ['Damir'], 'kontrolü geçen kaydedildi')
# 2) silince prim geri alınır
act(1, {'action': 'cps_insp_del', 'id': r['id']})
check(not adj(10) and not adj(11), 'silince prim geri alındı')
# 3) kesinti kapalıyken (varsayılan 0) düşük sonuç → para yok
act(1, {'action': 'cps_insp_add', 'branch_id': 1, 'score': 70, 'people': [12], 'period': per})
check(not adj(12), 'kesinti 0 → yok')
r = db.execute("SELECT id FROM cps_inspections ORDER BY id DESC").fetchone()
act(1, {'action': 'cps_insp_del', 'id': r['id']})
# 4) ayar: prim 500.000 ≥95, kesinti 100.000 <80
act(1, {'action': 'cps_insp_cfg', 'insp_bonus_amt': '500000', 'insp_bonus_step': '100000', 'insp_fail_amt': '100000', 'insp_fail_below': '80', 'insp_bonus_at': '95'})
cfg = bot.cps_cfg(db)
check(cfg['insp_bonus_amt'] == 500000 and cfg['insp_fail_amt'] == 100000, 'ayar kaydedildi')
act(10, {'action': 'cps_insp_cfg', 'insp_bonus_amt': '1'})
check(bot.cps_cfg(db)['insp_bonus_amt'] == 500000, 'barista ayar değiştiremez')
act(1, {'action': 'cps_insp_add', 'branch_id': 1, 'score': '79,9', 'people': '12', 'period': per})
check(len(adj(12)) == 1 and adj(12)[0]['amount'] == -100000 and not adj(10), f'79.9 → seçilen kişiye −100.000 ({adj(12)})')
act(1, {'action': 'cps_insp_add', 'branch_id': 1, 'score': 95, 'people': [10, 12], 'period': per})
check([a['amount'] for a in adj(10)] == [500000] and sorted(a['amount'] for a in adj(12)) == [-100000, 500000], 'tam 95 → prim')
# 5) 80–95 arası → hiçbir şey (3. kontrol yeri yok, doğrudan fonksiyon)
db.execute("INSERT INTO cps_inspections (period,branch_id,idx,score,people) VALUES (?,1,9,88,?)", (per, json.dumps([{'id': 11, 'n': 'Ubay'}])))
iid = db.execute("SELECT last_insert_rowid()").fetchone()[0]
check(bot.insp_apply_money(db, iid) == [] and not adj(11), '88 → para yok')
# kademe: 95→500k … 100→1M
db.execute("INSERT INTO cps_inspections (period,branch_id,idx,score,people) VALUES (?,1,8,?,?)", (per, 0, json.dumps([{'id': 11, 'n': 'Ubay'}])))
iid = db.execute("SELECT last_insert_rowid()").fetchone()[0]
got = []
for sc in (95, 96, 97.9, 99, 100):
    db.execute("DELETE FROM adjustments WHERE note LIKE ?", (f"%[insp:{iid}]%",))
    db.execute("UPDATE cps_inspections SET score=? WHERE id=?", (sc, iid)); db.commit()
    got.append(bot.insp_apply_money(db, iid)[0][1])
check(got == [500000, 600000, 700000, 900000, 1000000], f'kademe ({got})')
# elle prim (owner 2026-11-17: kural öncesi söz)
iid2 = db.execute("SELECT id FROM cps_inspections ORDER BY id DESC").fetchone()[0]
r = act(10, {'action': 'cps_insp_bonus', 'id': iid2, 'uid': 11, 'amount': 300000})
check(r and not r.get('ok'), 'barista elle prim veremez')
r = act(1, {'action': 'cps_insp_bonus', 'id': iid2, 'uid': 11, 'amount': '300 000'})
check(r and r.get('ok') and any(a['amount'] == 300000 and 'от владельца' in a['note'] for a in adj(11)), f'elle prim yazıldı ({adj(11)})')
check(any(int(q['id']) == 11 for q in json.loads(db.execute("SELECT people FROM cps_inspections WHERE id=?", (iid2,)).fetchone()[0])), 'kişi kontrol satırına eklendi')
print('ALL OK' if not FAIL else 'FAILED: %s' % FAIL)
raise SystemExit(1 if FAIL else 0)
