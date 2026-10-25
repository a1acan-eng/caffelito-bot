# GECE VARDİYASI ÇARPANI (owner 2026-10-25): ayar (owner, doğrulama, günlük) + hesap (gece aralığına
# düşen ÖDENEN saat × ставка × (k−1)) + geçmişin korunması.
# Çalıştırma: rm -f /tmp/np.db; PYTHONPATH=. DB_PATH=/tmp/np.db BOT_TOKEN=123:abc python tests/night_pay_test.py
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
    return ctx.result or {}
db = bot.get_db()
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized,branch_id) VALUES (1,'Owner','owner',1,1,1)")
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (10,'Бек','barista',1,1,1)")
db.execute("INSERT OR REPLACE INTO branches (id,name,active) VALUES (1,'C5',1)")
# Ödeme penceresi: kapalı 03:00–07:00 (şube kuralı) — test 10 bunu kullanır
for k, v in (("pay_open", 7), ("pay_close", 3), ("pay_unpaid", 1)):
    db.execute("INSERT OR REPLACE INTO meta (k,val) VALUES (?,?)", (k, str(v)))
db.commit()
RATE = 26000
D = datetime(2026, 10, 20)
def run(st, en):
    """Vardiyayı ставка 26.000 ile aç → end_shift ile kapat (gerçek kapanış yolu)."""
    sid = db.execute("INSERT INTO shifts (user_id,hours,total,date,period,created_at,start_time,branch_id,shift_role,rate) "
                     "VALUES (10,0,0,?,?,?,?,1,'barista',?)",
                     (st.strftime('%Y-%m-%d'), st.strftime('%Y-%m'), st.isoformat(), st.isoformat(), RATE)).lastrowid
    db.commit()
    sh = bot.end_shift(db, 10, {}, custom_end=en.isoformat())
    assert sh and sh["id"] == sid, 'kapanış'
    return db.execute("SELECT * FROM shifts WHERE id=?", (sid,)).fetchone()
# ── 9) Yetkisiz kullanıcı ayarı değiştiremez ──
r = act(10, {'action': 'night_pay_save', 'on': 1, 'k': 2})
check(r.get('ok') is False and bot.night_cfg(db)['on'] == 0, '9) barista ayarı değiştiremez')
# Sunucu doğrulaması
check(act(1, {'action': 'night_pay_save', 'k': 9}).get('ok') is False, 'doğrulama: çarpan 1–3 dışında reddedilir')
check(act(1, {'action': 'night_pay_save', 'start': '25:00'}).get('ok') is False, 'doğrulama: saat ЧЧ:ММ')
check(act(1, {'action': 'night_pay_save', 'start': '06:00', 'end': '06:00'}).get('ok') is False, 'doğrulama: başlangıç = bitiş reddedilir')
# ── 7) Kapalıyken çarpan uygulanmaz ──
s7 = run(D.replace(hour=16, minute=30), D.replace(hour=16, minute=30) + timedelta(hours=10, minutes=30))
check(s7["hourly_pay"] == 273000 and not s7["night_pay"], '7) kapalı: 16:30–03:00 = 273.000 (normal)')
# Owner açar → günlük kaydı
r = act(1, {'action': 'night_pay_save', 'on': 1, 'start': '22:00', 'end': '06:00', 'k': 1.5})
check(r.get('ok') is True and bot.night_cfg(db) == {"on": 1, "start": "22:00", "end": "06:00", "k": 1.5}, 'owner açtı (1.5x, 22–06)')
lg = db.execute("SELECT actor_id, details, created_at FROM logs WHERE action='night_pay_set' ORDER BY id DESC LIMIT 1").fetchone()
check(lg is not None and lg["actor_id"] == 1 and lg["created_at"] and json.loads(lg["details"])["old"]["on"] == 0
      and json.loads(lg["details"])["new"]["on"] == 1, 'günlük: kim (owner) · ne zaman · eski → yeni')
# ── 1) Tamamen gece dışı ──
s1 = run(D.replace(hour=8), D.replace(hour=16))
check(s1["night_h"] == 0 and s1["hourly_pay"] == 8 * RATE, '1) 08:00–16:00: gece 0, ücret 208.000')
# ── 2) Tamamen gece içinde (kapalı pencere yok: 22:00–02:00) ──
s2 = run(D.replace(hour=22), D.replace(hour=22) + timedelta(hours=4))
check(s2["night_h"] == 4 and s2["night_pay"] == 52000 and s2["hourly_pay"] == 4 * 39000, '2) 22:00–02:00: 4 sa × 39.000 = 156.000')
# ── 3) Geceye giren vardiya + 5) 03:00'te biten + 6) 1.5x — örnek ──
s3 = run(D.replace(hour=16, minute=30), D.replace(hour=16, minute=30) + timedelta(hours=10, minutes=30))
check(s3["hours"] == 10.5 and s3["night_h"] == 5, '3/5) 16:30–03:00: 10,5 sa ödenen, 5 sa gece')
check(s3["night_pay"] == 65000 and s3["hourly_pay"] == 338000, '6) 1.5x: 143.000 + 195.000 = 338.000 (prim 65.000)')
# ── 4) Gece yarısını geçen (23:00–01:00) ──
s4 = run(D.replace(hour=23), D.replace(hour=23) + timedelta(hours=2))
check(s4["night_h"] == 2 and s4["night_pay"] == 26000, '4) 23:00–01:00: 2 sa gece, prim 26.000')
# ── 10) Mevcut kural: kapalı pencere (03–07) ödenmez → prim de yok ──
s10 = run(D.replace(hour=20), D.replace(hour=20) + timedelta(hours=11))   # 20:00–07:00
check(s10["hours"] == 7 and s10["night_h"] == 5, '10) 20:00–07:00: ödenen 7 sa (03–07 kapalı), gece primi yalnız 22–03 = 5 sa')
check(s10["hourly_pay"] == 7 * RATE + 65000, '10) ücret = 7 × 26.000 + 65.000')
# ── 8) Ayar değişince GEÇMİŞ değişmez; saat düzeltmesi KAPANIŞTAKİ ayarla ──
before = [(x["id"], x["hourly_pay"], x["night_pay"]) for x in db.execute("SELECT * FROM shifts ORDER BY id")]
act(1, {'action': 'night_pay_save', 'k': 2.0})
act(1, {'action': 'night_pay_save', 'on': 0})
after = [(x["id"], x["hourly_pay"], x["night_pay"]) for x in db.execute("SELECT * FROM shifts ORDER BY id")]
check(before == after, '8) ayar değişti/kapandı → kapanmış vardiyalar aynen kaldı')
# Owner s3'ün çıkışını 02:00'ye düzeltir → prim KAPANIŞTAKİ 1.5x ile (bugün kapalı olsa da)
act(1, {'action': 'edit_shift', 'id': s3["id"], 'end_time': (D + timedelta(days=1)).replace(hour=2).isoformat()})
e3 = db.execute("SELECT * FROM shifts WHERE id=?", (s3["id"],)).fetchone()
check(e3["night_h"] == 4 and e3["night_pay"] == 52000 and e3["hourly_pay"] == round(9.5 * RATE) + 52000,
      '8) düzeltme kapanıştaki ayarla (1.5x): 9,5 sa + 4 sa gece primi')
# Özellikten ÖNCE kapanmış vardiya (s7, ayar kopyası kapalıydı) düzeltilince prim almaz
act(1, {'action': 'edit_shift', 'id': s7["id"], 'end_time': (D + timedelta(days=1)).replace(hour=2).isoformat()})
e7 = db.execute("SELECT * FROM shifts WHERE id=?", (s7["id"],)).fetchone()
check(not e7["night_pay"] and e7["hourly_pay"] == round(9.5 * RATE), '8) kapalıyken kapanmış vardiyaya sonradan prim eklenmez')
# Maaş özeti primi içerir (hourly_pay toplamı)
sm = bot.calc_summary(db, 10, '2026-10')
check(sm["hourly"] == sum(x["hourly_pay"] or 0 for x in db.execute("SELECT hourly_pay FROM shifts WHERE user_id=10 AND period='2026-10'")), 'maaş özeti = vardiyaların ücret toplamı (prim dahil)')
# ── Kişiye özel (owner 2026-10-25): genel KAPALI iken bu kişiye AÇIK / genel AÇIK iken bu kişiye KAPALI ──
check(act(10, {'action': 'night_user_set', 'target': 10, 'mode': 'on'}).get('ok') is False, 'barista kendine kişisel ayar açamaz')
act(1, {'action': 'night_pay_save', 'on': 0, 'k': 1.5})
check(act(1, {'action': 'night_user_set', 'target': 10, 'mode': 'on'}).get('ok') is True, 'owner Бек için kişisel AÇIK')
p1 = run(D.replace(hour=16, minute=30), D.replace(hour=16, minute=30) + timedelta(hours=10, minutes=30))
check(p1["night_pay"] == 65000 and json.loads(p1["night_cfg"])["src"] == 'personal', 'genel kapalı + kişisel açık → prim 65.000, kopyada «personal»')
act(1, {'action': 'night_pay_save', 'on': 1})
act(1, {'action': 'night_user_set', 'target': 10, 'mode': 'off'})
p2 = run(D.replace(hour=16, minute=30), D.replace(hour=16, minute=30) + timedelta(hours=10, minutes=30))
check(not p2["night_pay"] and p2["hourly_pay"] == 273000, 'genel açık + kişisel kapalı → prim yok')
act(1, {'action': 'night_user_set', 'target': 10, 'mode': 'default'})
p3 = run(D.replace(hour=16, minute=30), D.replace(hour=16, minute=30) + timedelta(hours=10, minutes=30))
check(p3["night_pay"] == 65000 and json.loads(p3["night_cfg"])["src"] == 'general', '«по общей» → genel ayar (açık) uygulanır')
check(p1["night_pay"] == 65000 and db.execute("SELECT night_pay FROM shifts WHERE id=?", (p2["id"],)).fetchone()[0] in (0, None), 'kişisel ayar değişikliği eski vardiyalara dokunmadı')
check(db.execute("SELECT COUNT(*) FROM logs WHERE action='night_user_set'").fetchone()[0] == 3, 'kişisel değişiklikler günlükte')
check(act(1, {'action': 'night_user_set', 'target': 10, 'mode': 'xx'}).get('ok') is False, 'geçersiz mod reddedilir')
print('ALL OK' if not FAIL else 'FAILED: %s' % FAIL)
raise SystemExit(1 if FAIL else 0)
