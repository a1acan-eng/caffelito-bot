# Bir günde iki vardiya (owner 2026-10-30: «Pazar sabah Magic, akşam C5»).
# Hücre kodu «A+B»: kontrol, ceza planı, şube tespiti ve kapsama ikisini de görür.
# Çalıştırma: rm -f /tmp/gd2.db; PYTHONPATH=. DB_PATH=/tmp/gd2.db BOT_TOKEN=123:abc python tests/grid_dual_test.py
import json, asyncio, bot
from datetime import datetime, timedelta
FAIL = []
def check(c, m):
    print(('  ok  ' if c else '  FAIL ') + m)
    if not c: FAIL.append(m)
class FB:
    async def send_message(self, *a, **k): return None
    def __getattr__(self, n):
        async def f(*a, **k): return None
        return f
def act(uid, p):
    ctx = bot._ShimContext(FB(), {}, uid=uid)
    asyncio.run(bot.handle_webapp_data(bot._ShimUpdate(FB(), uid, f'U{uid}', None, json.dumps(p)), ctx))
    return getattr(ctx, 'result', None)
bot.get_db()
db = bot.get_db()   # ikinci çağrı: taze DB'de sonradan eklenen sütunlar (max_staff)
db.execute("DELETE FROM shift_grid")
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized) VALUES (1,'Owner','owner',1,1)")
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (10,'Хасанбей','barista',1,1,1)")
for i, n in ((1, 'C5'), (2, 'Magic')):
    db.execute("INSERT OR REPLACE INTO branches (id, name, active, max_staff) VALUES (?,?,1,3)", (i, n))
for c, b, s, e in (('m1', 2, '07:00', '17:00'), ('c5e', 1, '17:00', '03:00'), ('c5d', 1, '09:00', '19:00')):
    db.execute("INSERT OR REPLACE INTO shift_templates (code, branch_id, start_t, end_t, active) VALUES (?,?,?,?,1)", (c, b, s, e))
db.commit()
wk = bot.grid_week_key(1)
sun = bot.grid_day_date(wk, 6)

check(bot.grid_check(db, wk, 6, 10, 'm1+c5e')[0], 'Magic sabah + C5 akşam atanabilir')
check(not bot.grid_check(db, wk, 6, 10, 'm1+c5d')[0], 'çakışan iki vardiya reddedilir')
check(not bot.grid_check(db, wk, 6, 10, 'm1+m1')[0], 'aynı vardiya iki kez reddedilir')
check(not bot.grid_check(db, wk, 6, 10, 'm1+c5e+c5d')[0], 'üç vardiya reddedilir')
check(not bot.grid_check(db, wk, 6, 10, 'm1+off')[0], 'vardiya + выходной reddedilir')

act(10, {'action': 'shift_grid_set', 'target_uid': 10, 'week': 1, 'day': 6, 'code': 'm1+c5e'})
check(bot.cov_cell_code(db, 10, sun) == '', 'çalışan kendine iki vardiya yazamaz')
act(1, {'action': 'shift_grid_set', 'target_uid': 10, 'week': 1, 'day': 6, 'code': 'm1+c5e'})
check(bot.cov_cell_code(db, 10, sun) == 'm1+c5e', 'owner iki vardiyayı kaydeder')
act(1, {'action': 'shift_grid_set', 'target_uid': 10, 'week': 1, 'day': 5, 'code': 'm1+c5d'})
check(bot.cov_cell_code(db, 10, sun - timedelta(days=1)) == '', 'çakışan çift kaydedilmez')

txt = bot.grid_code_text(db, 'm1+c5e')
check('Magic' in txt and 'C5' in txt and '07:00' in txt and '17:00–03:00' in txt, f'okunur metin: {txt}')

d = datetime(sun.year, sun.month, sun.day)
pl, code = bot.op_scheduled(db, 10, d + timedelta(hours=7, minutes=10))
check(code == 'm1' and pl == d + timedelta(hours=7), f'sabah gelişi → Magic 07:00 planı ({code})')
pl, code = bot.op_scheduled(db, 10, d + timedelta(hours=17, minutes=20))
check(code == 'c5e' and pl == d + timedelta(hours=17), f'akşam gelişi → C5 17:00 planı ({code})')
check(bot.grid_branch_for(db, 10, d + timedelta(hours=7)) == 2, 'sabah şubesi Magic')
check(bot.grid_branch_for(db, 10, d + timedelta(hours=17)) == 1, 'akşam şubesi C5')

cells = bot.cov_cells(db, sun, sun, uid=10)
check(sorted((c['code'], c['bid']) for c in cells) == [('c5e', 1), ('m1', 2)], 'kapsama iki vardiyayı da sayar')
check(len(bot.cov_cells(db, sun, sun, branch_id=1)) == 1, 'C5 tabelasında akşam vardiyası var')

# kapasite: C5 dolu (3 kişi) → kişi C5 parçasını alamaz
for u in (21, 22, 23):
    db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (?,?,'barista',1,1,1)", (u, f'B{u}'))
    db.execute("INSERT OR REPLACE INTO shift_grid (week_key, day, user_id, code) VALUES (?,5,?,'c5e')", (wk, u))
db.commit()
ok, why = bot.grid_check(db, wk, 5, 10, 'm1+c5e')
check(not ok and 'Максимум' in why, 'dolu şubeye ikinci vardiya da konmaz')
# iki vardiyalı hücre başka birinin kapasitesinde bir kez sayılır
check(bot.grid_check(db, wk, 6, 21, 'c5e')[0], 'C5 pazar: 1 kişi + yeni → serbest')

# şablon silinirken iki vardiyalı hücrede kullanıldığı görülür → pasife alınır
act(1, {'action': 'shift_tpl_delete', 'code': 'm1'})
r = db.execute("SELECT active FROM shift_templates WHERE code='m1'").fetchone()
check(r is not None and r['active'] == 0, 'çift hücrede kullanılan şablon silinmez, pasife alınır')
print('ALL OK' if not FAIL else 'FAILED: %s' % FAIL)
raise SystemExit(1 if FAIL else 0)
