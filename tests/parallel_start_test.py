# Aynı şubede aynı pozisyonda ikinci kişi vardiya açabilmeli (owner 2026-10-13,
# canli: Magic'te stajyer Убай, Акмаль açıkken «Текущий бариста должен завершить…» aldı).
# Çalıştırma: rm -f /tmp/ps.db; PYTHONPATH=. DB_PATH=/tmp/ps.db BOT_TOKEN=123:abc python tests/parallel_start_test.py
import asyncio, json, bot
FAIL = []
def check(c, m):
    print(('  ok  ' if c else '  FAIL ') + m)
    if not c: FAIL.append(m)
db = bot.get_db()
sent = []
class FB:
    async def send_message(self, chat_id=None, text=None, *a, **k): sent.append((chat_id, text))
    def __getattr__(self, n):
        async def f(*a, **k): return None
        return f
def act(uid, p):
    sent.clear()
    asyncio.run(bot.handle_webapp_data(bot._ShimUpdate(FB(), uid, f'U{uid}', None, json.dumps(p)), bot._ShimContext(FB(), {}, uid=uid)))
db.execute("UPDATE branches SET name='C5' WHERE id=1")
db.execute("INSERT OR REPLACE INTO branches (id,name,sort_order,active,trainee_enabled) VALUES (2,'Magic',2,1,0)")
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized) VALUES (1,'Owner','owner',1,1)")
for uid, nm in ((20, 'Акмаль'), (21, 'Убай')):
    db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (?,?,'barista',2,1,1)", (uid, nm))
db.commit()
act(20, {'action': 'shift_start', 'branch_id': 2})
check(bot.get_active_shift(db, 20) is not None, 'Акмаль açtı')
act(21, {'action': 'shift_start', 'branch_id': 2, 'shift_mode': 'single'})
a21 = bot.get_active_shift(db, 21)
check(a21 is not None, 'Убай da açtı (pozisyon dolu ama engel yok)')
check(a21 is not None and a21['note'] == 'параллельная смена', 'paralel olarak işaretlendi')
check(not any('должен завершить' in (t or '') for c, t in sent), '«…должен завершить» mesajı yok')
check(any('параллельно' in (t or '') and 'U20' in (t or '') for c, t in sent), 'kime paralel olduğu söylendi (shim adı U20)')
check(bot.get_active_shift(db, 20) is not None, 'Акмаль\'ın vardiyası açık kaldı')
# «Нет позиции ассистента» (owner ayarı) hâlâ engel
cid = db.execute("INSERT INTO salary_categories (name, hourly_rate, slot_role, does_kasa, active) VALUES ('Стажёр X', 15000, 'assistant', 0, 1)").lastrowid
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized,salary_cat_id) VALUES (22,'Стаж','barista',2,1,1,?)", (cid,))
db.commit()
if bot.barista_pay_info(db, 22, branch_id=2).get('slot_role') == 'assistant':
    act(22, {'action': 'shift_start', 'branch_id': 2})
    check(bot.get_active_shift(db, 22) is None, '«Нет позиции ассистента» hâlâ engelliyor')
else:
    print('  (kategori atama şeması farklı — asistan kontrolü atlandı)')
# Owner başkası adına başlatınca da pozisyon dolu engel değil
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (23,'Третий','barista',2,1,1)")
db.commit()
act(1, {'action': 'force_start_shift', 'target': 23, 'branch_id': 2})
check(bot.get_active_shift(db, 23) is not None, 'owner üçüncü kişiyi de başlatabildi')
# График esas: owner kişiyi bugün Magic'e koyduysa — uygulama C5 gönderse bile Magic'te,
# asistan pozisyonu kapalı olsa bile açılır.
from datetime import datetime, timedelta
now = datetime.now(bot.TZ).replace(tzinfo=None)
d = now.date() if now.hour >= 6 else now.date()
mon = d - timedelta(days=d.weekday())
db.execute("INSERT OR REPLACE INTO shift_templates (code, branch_id, start_t, end_t, active) VALUES ('MG', 2, '08:00', '20:00', 1)")
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized,salary_cat_id) VALUES (24,'Стаж2','barista',1,1,1,?)", (cid,))
db.execute("INSERT OR REPLACE INTO shift_grid (week_key, day, user_id, code) VALUES (?,?,?,?)", (mon.isoformat(), d.weekday(), 24, 'MG'))
db.commit()
check(bot.grid_branch_for(db, 24, now) == 2, 'grid_branch_for: bugün Magic')
act(24, {'action': 'shift_start', 'branch_id': 1})
a24 = bot.get_active_shift(db, 24)
check(a24 is not None, 'grafikteki stajyer açtı (asistan pozisyonu kapalıyken bile)')
check(a24 is not None and int(a24['branch_id'] or 0) == 2, 'vardiya grafikteki şubede (Magic) açıldı')
print('ALL OK' if not FAIL else 'FAILED: %s' % FAIL)
