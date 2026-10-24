# Paralel kapanış (owner 2026-10-24): aynı şubede iki barista — kasayı SON ÇIKAN kapatır,
# önce çıkan yalnız saat; bardak bonusu eşit bölünür, kasadan tamamı çıkar (pay elden verilir).
# Çalıştırma: rm -f /tmp/pc.db; PYTHONPATH=. DB_PATH=/tmp/pc.db BOT_TOKEN=123:abc python tests/par_close_test.py
import json, asyncio, bot
from datetime import datetime, timedelta
FAIL = []
def check(c, m):
    print(('  ok  ' if c else '  FAIL ') + m)
    if not c: FAIL.append(m)
SENT = []
class FB:
    async def send_message(self, chat_id=None, text=None, *a, **k):
        if chat_id is None and a: chat_id = a[0]
        if text is None and len(a) > 1: text = a[1]
        SENT.append((chat_id, text or '')); return None
    def __getattr__(self, n):
        async def f(*a, **k): return None
        return f
def act(uid, p):
    ctx = bot._ShimContext(FB(), {}, uid=uid)
    asyncio.run(bot.handle_webapp_data(bot._ShimUpdate(FB(), uid, f'U{uid}', None, json.dumps(p)), ctx))
    return ctx.result
db = bot.get_db()
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized) VALUES (1,'Owner','owner',1,1)")
for uid, nm in ((10, 'Бек'), (11, 'Убай'), (12, 'Асс')):
    db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (?,?,'barista',1,1,1)", (uid, nm))
db.execute("INSERT OR REPLACE INTO branches (id, name, active) VALUES (1,'C5',1)")
db.commit()
now = datetime.now(bot.TZ).replace(tzinfo=None)
P = 'параллельная смена'
def open_shift(uid, h=6, role='barista', note='', par_of=None):
    st = now - timedelta(hours=h)
    sid = db.execute("INSERT INTO shifts (user_id,hours,total,date,period,created_at,start_time,branch_id,shift_role,note,par_of) "
                     "VALUES (?,0,0,?,?,?,?,1,?,?,?)",
                     (uid, st.strftime('%Y-%m-%d'), st.strftime('%Y-%m'), st.isoformat(), st.isoformat(), role, note, par_of)).lastrowid
    db.commit(); return sid
def row(sid): return db.execute("SELECT * FROM shifts WHERE id=?", (sid,)).fetchone()
drinks = {"ml300": 20, "ml400": 10}
full = bot.calc_bonus(drinks, bot.get_prices(db))
check(full > 0, f'bardak bonusu hesaplanıyor ({full})')
s10 = open_shift(10); s11 = open_shift(11, 5, note=P, par_of=s10)
check([r["user_id"] for r in bot.par_open_partners(db, row(s11))] == [10], 'Убай için напарник = Бек')
check(bot._par_mate_names(db, 11) == ['Бек'], 'Nero\'ya напарник adı gidiyor')
# 1) Önce çıkan bardak sayarak kapatmaya çalışır → red
act(11, {'action': 'shift_end', 'drinks': drinks})
check(row(s11)["end_time"] is None, 'напарник açıkken bardaklı kapanış reddedildi')
# 2) Hafif kapanış
act(11, {'action': 'shift_end', 'drinks': {}, 'light': 1})
r11 = row(s11)
check(r11["end_time"] is not None and r11["par_light"] == 1 and (r11["bonus"] or 0) == 0, 'Убай yalnız saatle kapattı')
n_cr = db.execute("SELECT COUNT(*) FROM cashreports").fetchone()[0]
act(11, {'action': 'cash_report', 'cups': [], 'itogo': 0, 'daily_pay': 5000, 'branch_id': 1})
check(db.execute("SELECT COUNT(*) FROM cashreports").fetchone()[0] == n_cr, 'önce çıkanın kasa raporu yazılmadı')
# 3) Son kalan «hafif» isterse → red (artık kasayı o kapatır)
act(10, {'action': 'shift_end', 'drinks': {}, 'light': 1})
check(row(s10)["end_time"] is None, 'son kalan hafif kapatamaz')
# 4) Son kalan tam kapanış → bonus yarı yarıya
SENT.clear()
act(10, {'action': 'shift_end', 'drinks': drinks})
check(any(str(c) == '11' and 'Ваша доля' in t for c, t in SENT), 'önce çıkana «Ваша доля» bildirimi gitti')
r10, r11 = row(s10), row(s11)
half = full // 2
check(r10["end_time"] is not None and (r10["bonus"] or 0) == half and r10["par_n"] == 2, f'Бек bonusu yarısı ({half})')
check((r11["bonus"] or 0) == half and r11["par_by"] == s10, 'Убай vardiyasına payı yazıldı')
check(db.execute("SELECT COUNT(*) FROM adjustments WHERE user_id=11").fetchone()[0] == 0, 'ay sonuna kayıt YOK (bonus nakit)')
check(bot.calc_summary(db, 11, r11["period"])["adjustments"] == 0, 'Убай maaşına bonus eklenmedi')
# Kapatan kasadan TAMAMINI alır (kendi payı + напарник payı)
act(10, {'action': 'cash_report', 'cups': [], 'itogo': 0, 'vyshlo': 100000, 'daily_pay': full, 'branch_id': 1,
         'end_time': r10["end_time"], 'start_time': r10["start_time"]})
cr = db.execute("SELECT daily_pay FROM cashreports WHERE user_id=10 ORDER BY id DESC LIMIT 1").fetchone()
check(cr is not None and cr["daily_pay"] == full, f'kasadan çıkan dnevnoy bonus = tamamı ({cr["daily_pay"] if cr else None})')
# 5) Tekrar paylaştırma yok: yeni bir vardiya eski hafif kaydı yeniden bölmez
s10b = open_shift(10, 1)
act(10, {'action': 'shift_end', 'drinks': drinks})
check((row(s10b)["bonus"] or 0) == full and not row(s10b)["par_n"], 'tek başına kapanışta bonus tam')
# 6) Asistan напарник sayılmaz
s12 = open_shift(12, 3, role='assistant'); s10c = open_shift(10, 2)
check(bot.par_open_partners(db, row(s10c)) == [], 'asistan açıkken barista kapanışı tam')
act(10, {'action': 'shift_end', 'drinks': drinks})
check((row(s10c)["bonus"] or 0) == full, 'asistanla kapanışta bonus bölünmedi')
# 7) Barista yuvasındaki STAJYER: kasa yine tek kişide, ama bonus PAYLAŞILMAZ
cid = db.execute("INSERT INTO salary_categories (name, hourly_rate, active, intern_days) VALUES ('Стажёр', 15000, 1, 30)").lastrowid
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized,salary_cat_id) VALUES (13,'Стаж','barista',1,1,1,?)", (cid,))
db.commit()
check(bot.par_is_intern(db, 13, 1) and not bot.par_is_intern(db, 10, 1), 'stajyer tanınıyor, barista değil')
# 7a) stajyer önce çıkar → yalnız saat; barista kapatır → bonus TAM, stajyere pay yok
s13 = open_shift(13, 4); s10d = open_shift(10, 4, note=P, par_of=s13)
act(13, {'action': 'shift_end', 'drinks': {}, 'light': 1})
check(row(s13)["par_light"] == 1, 'stajyer barista açıkken yalnız saatle kapattı')
SENT.clear()
act(10, {'action': 'shift_end', 'drinks': drinks})
check((row(s10d)["bonus"] or 0) == full and not row(s10d)["par_n"], 'barista bonusu tam (stajyerle bölünmedi)')
check((row(s13)["bonus"] or 0) == 0 and row(s13)["par_by"] is None, 'stajyere pay yazılmadı')
check(not any(str(c) == '13' and 'Ваша доля' in t for c, t in SENT), 'stajyere «доля» bildirimi gitmedi')
# 7b) barista önce çıkar, stajyer kapatır → bölme yok
s10e = open_shift(10, 4); s13b = open_shift(13, 4, note=P, par_of=s10e)
act(10, {'action': 'shift_end', 'drinks': {}, 'light': 1})
act(13, {'action': 'shift_end', 'drinks': drinks})
check(row(s13b)["end_time"] is not None and not row(s13b)["par_n"] and row(s10e)["par_by"] is None, 'stajyer kapatınca bölme yok')
# 8) NORMAL DEVİR (canlı hata 2026-10-24): sabahçı açık, akşamcı normal açtı (not yok) →
#    sabahçı kasayı/bardakları HER ZAMANKİ GİBİ kapatır, bölme yok.
s10f, s11f = open_shift(10, 10), open_shift(11, 1, note='передача смены')
check(bot.par_open_partners(db, row(s10f)) == [], 'devirde напарник sayılmıyor')
act(10, {'action': 'shift_end', 'drinks': drinks})
check(row(s10f)["end_time"] is not None and (row(s10f)["bonus"] or 0) == full and not row(s10f)["par_light"],
      'sabahçı devirde bardaklarla tam kapattı')
act(11, {'action': 'shift_end', 'drinks': drinks})
check(row(s11f)["end_time"] is not None and (row(s11f)["bonus"] or 0) == full, 'akşamcı da kendi bardaklarıyla kapattı')
# 9) AÇILIŞ: birincinin başlangıcından 2 saat içinde gelen → paralel grup (par_of);
#    sonra gelen → ikinci vardiya (grup yok). Akşamcı sabahki gruba KATILMAZ.
s10g = open_shift(10, 1)                           # birinci: 1 saat önce açtı
check(bot.par_window_root(db, 11, 1, now) == s10g, '1 saat sonra gelen → paralel grup')
act(11, {'action': 'shift_start', 'branch_id': 1})
r11g = bot.get_active_shift(db, 11)
check(r11g is not None and r11g["par_of"] == s10g, 'açılışta par_of = birincinin vardiyası')
check(abs((datetime.fromisoformat(r11g["start_time"]) - now).total_seconds()) < 300, 'ücret GELDİĞİ saatten (plana çekilmedi)')
check([r["user_id"] for r in bot.par_open_partners(db, row(s10g))] == [11], 'birinci için напарник = paralel gelen')
act(11, {'action': 'shift_end', 'drinks': {}, 'light': 1}); act(10, {'action': 'shift_end', 'drinks': drinks})
s10h = open_shift(10, 3)                           # birinci: 3 saat önce açtı
check(bot.par_window_root(db, 11, 1, now) is None, '3 saat sonra gelen → ikinci vardiya (grup yok)')
act(11, {'action': 'shift_start', 'branch_id': 1})
r11h = bot.get_active_shift(db, 11)
check(r11h is not None and not r11h["par_of"], 'ikinci vardiyada par_of yok')
check(bot.par_open_partners(db, row(s10h)) == [], 'ikinci vardiya напарник sayılmaz — herkes kendi kasasını kapatır')
bot.op_cfg_save(db, {"par_window": 240})
check(bot.par_window_min(db) == 240 and bot.par_window_root(db, 12, 1, now) == s10h, 'pencere ayarlanabilir (4 saat → 3 saat önce açan gruba)')
print('ALL OK' if not FAIL else 'FAILED: %s' % FAIL)
raise SystemExit(1 if FAIL else 0)
