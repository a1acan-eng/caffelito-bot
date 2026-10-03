# Vardiya & İzin Karar Motoru — 13 edge-case testi + ek akışlar.
# Çalıştırma (boş veritabanıyla): rm -f /tmp/cov.db; PYTHONPATH=. DB_PATH=/tmp/cov.db BOT_TOKEN=123:abc python tests/cov_test.py
import asyncio, json, bot
from datetime import datetime, timedelta
FAIL = []
def check(c, m):
    print(('  ok  ' if c else '  FAIL ') + m)
    if not c: FAIL.append(m)

db = bot.get_db()
sent = []
class FB:
    async def send_message(self, chat_id=None, text=None, *a, **k):
        sent.append((chat_id, text, k.get('reply_markup')))
    def __getattr__(self, n):
        async def f(*a, **k): return None
        return f
def act(uid, p):
    sent.clear()
    asyncio.run(bot.handle_webapp_data(bot._ShimUpdate(FB(), uid, f'U{uid}', None, json.dumps(p)), bot._ShimContext(FB(), {}, uid=uid)))
    return ' | '.join(str(t) for c, t, _ in sent if c == uid)
def run(coro):
    sent.clear(); asyncio.run(coro)

# ── kurulum ──
db.execute("UPDATE branches SET name='C5', sort_order=1, group_chat_id='-1001' WHERE id=1")
db.execute("INSERT OR REPLACE INTO branches (id,name,group_chat_id,sort_order,active) VALUES (2,'Magic','-1002',2,1)")
db.execute("INSERT OR REPLACE INTO branches (id,name,group_chat_id,sort_order,active) VALUES (3,'NewWay','-1003',3,1)")
db.execute("DELETE FROM shift_templates")
for code, b, s, e in (("c5d", 1, "07:00", "17:00"), ("c5n", 1, "17:00", "03:00"), ("mgx", 2, "07:00", "17:00"), ("mgn", 2, "17:00", "00:00")):
    db.execute("INSERT INTO shift_templates (code,branch_id,start_t,end_t,active) VALUES (?,?,?,?,1)", (code, b, s, e))
cat_b = db.execute("INSERT INTO salary_categories (name,hourly_rate,active) VALUES ('Бариста',22000,1)").lastrowid
cat_t = db.execute("INSERT INTO salary_categories (name,hourly_rate,active,reserve_on) VALUES ('Стажёр',12000,1,0)").lastrowid
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized) VALUES (1,'Owner','owner',1,1)")
def staff(uid, nm, bid, cat=None, extra=None, work=None):
    db.execute("INSERT OR REPLACE INTO users (user_id,name,display_name,role,branch_id,salary_cat_id,approved,authorized,cov_extra,cov_work) "
               "VALUES (?,?,?,'barista',?,?,1,1,?,?)", (uid, nm, nm, bid, cat or cat_b, extra, work))
for uid, nm in ((10, 'Ахмет'), (11, 'Бек'), (12, 'Вали')):
    staff(uid, nm, 1)
staff(13, 'Стажёр Гуля', 1, cat_t)
for uid, nm in ((20, 'Даня'), (21, 'Ерлан'), (22, 'Жасур')):
    staff(uid, nm, 2)
db.execute("INSERT OR REPLACE INTO meta (k,val) VALUES ('weekly_off_limit','2')")
db.commit()
today = datetime.now(bot.TZ).replace(tzinfo=None).date()
mon1 = today - timedelta(days=today.weekday()) + timedelta(days=7)   # gelecek hafta
D = mon1 + timedelta(days=2)                                          # gelecek Çarşamba
WK, DAY = mon1.isoformat(), 2
def put(uid, code, d=D): bot.cov_set_cell(db, uid, d, code); db.commit()
def clear(): db.execute("DELETE FROM shift_grid"); db.execute("DELETE FROM open_shifts"); db.execute("DELETE FROM cov_offers"); db.execute("DELETE FROM dayoff_requests"); db.commit()
def gap_of(uid): return db.execute("SELECT * FROM open_shifts WHERE from_uid=? ORDER BY id DESC", (uid,)).fetchone()
def cell(uid, d=D): return bot.cov_cell_code(db, uid, d)
bot.cov_needs_save(db, 1, [{"s": "07:00", "e": "17:00", "n": 2, "nb": 1}, {"s": "17:00", "e": "03:00", "n": 1, "nb": 1}])
bot.cov_needs_save(db, 2, [{"s": "07:00", "e": "17:00", "n": 1, "nb": 1}])

print('TEST 1 · 3 kişilik şube, 1 kişi izin (onsuz açık var, yedek yok → yönetici)')
clear(); put(10, 'c5d'); put(11, 'c5d'); put(12, 'c5n')
m = act(10, {"action": "shift_grid_set", "target_uid": 10, "week": 1, "day": DAY, "code": "off"})
g = gap_of(10)
check(cell(10) == 'c5d', 'izin hemen yazılmadı (açık oluşuyordu)')
check(g is not None and g['kind'] == 'dayoff', 'izin açığı açıldı')
check(g['cov'] == 'escalated', f"aday yok → yöneticiye ({g['cov']})")
ok, bad = bot.cov_candidates(db, g)
why = {b['uid']: b['why'] for b in bad}
check('подряд' in why.get(12, ''), f"12: aynı gün gece vardiyası → 20 saat blok reddi ({why.get(12)})")
check('допуска' in why.get(13, ''), f"stajyer barista yerine geçemez ({why.get(13)})")
check('свой филиал' in why.get(20, ''), f"Magic çalışanı yalnız kendi şubesine açık ({why.get(20)})")
check(any(c == 1 and 'не удалось' in str(t) for c, t, _ in sent), 'owner\'a 🚨 eskalasyon DM')
check(any(c == 10 and 'невозможен' in str(t) for c, t, _ in sent), 'çalışana «şu an uygun değil» bildirimi')

print('TEST 2 · 4 kişilik şube, 1 kişi izin → aynı şubeden yedek')
clear(); staff(14, 'Ислом', 1); db.commit()
put(10, 'c5d'); put(11, 'c5d'); put(12, 'c5n')
act(10, {"action": "shift_grid_set", "target_uid": 10, "week": 1, "day": DAY, "code": "off"})
g = gap_of(10)
check(g['cov'] == 'offered', 'teklif gönderildi')
of = db.execute("SELECT * FROM cov_offers WHERE os_id=?", (g['id'],)).fetchall()
check([o['uid'] for o in of] == [14], f"teklif 14'e ({[o['uid'] for o in of]})")
r, info = bot.cov_accept(db, of[0]['id'], 14)
check(r == 'ok', 'kabul')
check(cell(14) == 'c5d' and cell(10) == 'off', 'plan: 14 → c5d, 10 → izin')
check(db.execute("SELECT status FROM dayoff_requests WHERE id=?", (g['dayoff_req_id'],)).fetchone()['status'] == 'ok', 'izin talebi onaylandı')
check(not bot.cov_shortfalls(db, 1, *bot.cov_window(D, '07:00', '17:00')), 'şube ihtiyacı yine karşılanıyor')

print('TEST 3 · başka şubeden transfer, kaynak yeterli → uyarısız')
clear(); db.execute("UPDATE users SET archived=1 WHERE user_id=14"); db.execute("UPDATE users SET cov_extra='other' WHERE user_id=20"); db.commit()
put(10, 'c5d'); put(11, 'c5d'); put(12, 'c5n'); put(20, 'mgx'); put(21, 'mgx')
act(10, {"action": "shift_grid_set", "target_uid": 10, "week": 1, "day": DAY, "code": "off"})
g = gap_of(10)
of = db.execute("SELECT * FROM cov_offers WHERE os_id=?", (g['id'],)).fetchall()
check(len(of) == 1 and of[0]['uid'] == 20 and of[0]['kind'] == 'transfer', 'Magic → C5 transfer teklifi')
check(of[0]['warn'] == 0, 'kaynak şube yeterli → uyarı yok')
r, info = bot.cov_accept(db, of[0]['id'], 20)
check(r == 'ok' and cell(20) == 'c5d', '✅ transfer: 20 artık C5 c5d')

print('TEST 4 · kaynak şube minimumun altına düşüyor → ⚠️ uyarı')
clear(); put(10, 'c5d'); put(11, 'c5d'); put(12, 'c5n'); put(20, 'mgx')
act(10, {"action": "shift_grid_set", "target_uid": 10, "week": 1, "day": DAY, "code": "off"})
g = gap_of(10)
of = db.execute("SELECT * FROM cov_offers WHERE os_id=?", (g['id'],)).fetchone()
check(of['warn'] == 1 and of['src_before'] == 1 and of['src_after'] == 0 and of['src_need'] == 1,
      f"uyarı: Magic 1 → 0, gerekli 1 ({of['src_before']}→{of['src_after']}/{of['src_need']})")
r, info = bot.cov_accept(db, of['id'], 20)
check(r == 'need_ack' and cell(20) == 'mgx', 'onaysız kabul → önce uyarı (plan değişmedi)')

print('TEST 5 · uyarıya rağmen kabul → izin ver + kayıt')
r, info = bot.cov_accept(db, of['id'], 20, ack=True)
check(r == 'ok' and cell(20) == 'c5d', '✅ görevlendirme yapıldı')
check(db.execute("SELECT ack FROM cov_offers WHERE id=?", (of['id'],)).fetchone()['ack'] == 1, 'teklifte ack=1')
lg = db.execute("SELECT details FROM logs WHERE action='cov_decision' ORDER BY id DESC LIMIT 1").fetchone()
d = json.loads(lg['details'])
check(d.get('warning_shown') is True and d.get('employee_acknowledged') is True and d.get('source_branch_affected') is True,
      'log: warning_shown / employee_acknowledged / source_branch_affected = true')
check(d.get('source_branch_id') == 2 and d.get('target_branch_id') == 1, 'log: kaynak 2 → hedef 1')
check(not bot.cov_shortfalls(db, 1, *bot.cov_window(D, '07:00', '17:00')), 'C5 tamam')
check(bool(bot.cov_shortfalls(db, 2, *bot.cov_window(D, '07:00', '17:00'))), 'Magic açığı panoda görünür (yeniden hesaplandı)')

print('TEST 6 · kabul edilen vardiyaya gelmezse → mevcut gecikme/ceza sistemi')
st, code = bot.op_scheduled(db, 20, datetime(D.year, D.month, D.day, 7, 40))
check(st is not None and st.strftime('%H:%M') == '07:00' and code == 'c5d', f"mevcut op_scheduled yeni planı görüyor ({st}, {code})")
tbls = {r['name'] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
check(not any('penalt' in t or 'cov_fine' in t for t in tbls), 'yeni ceza tablosu yok')

print('TEST 7 · barista + stajyer aynı gün izin')
clear(); db.execute("UPDATE users SET archived=0 WHERE user_id=14"); db.commit()
put(10, 'c5d'); put(11, 'c5d'); put(14, 'c5d'); put(13, 'c5d'); put(12, 'c5n')
act(10, {"action": "shift_grid_set", "target_uid": 10, "week": 1, "day": DAY, "code": "off"})
check(cell(10) == 'off' and gap_of(10) is None, 'barista izni: kapsama yeterli → hemen')
act(13, {"action": "shift_grid_set", "target_uid": 13, "week": 1, "day": DAY, "code": "off"})
check(cell(13) == 'off' and gap_of(13) is None, 'stajyer izni de: hâlâ 2 kişi ≥ 2 → hemen')
clear(); bot.cov_needs_save(db, 1, [{"s": "07:00", "e": "17:00", "n": 3, "nb": 1}, {"s": "17:00", "e": "03:00", "n": 1, "nb": 1}])
put(10, 'c5d'); put(11, 'c5d'); put(14, 'c5d'); put(13, 'c5d'); put(12, 'c5n')
act(10, {"action": "shift_grid_set", "target_uid": 10, "week": 1, "day": DAY, "code": "off"})
check(cell(10) == 'off', 'ihtiyaç 3: barista izni hâlâ yeterli (3 kişi kalır)')
m = act(13, {"action": "shift_grid_set", "target_uid": 13, "week": 1, "day": DAY, "code": "off"})
g = gap_of(13)
check(cell(13) == 'c5d' and g is not None, 'stajyer izni: açık oluşuyor → hemen yazılmadı')
check(g['role_need'] == 'any', 'stajyer açığını barista/stajyer kapatabilir (role_need=any)')
check('не хватает' in m, 'stajyere açıklama gitti')
bot.cov_needs_save(db, 1, [{"s": "07:00", "e": "17:00", "n": 2, "nb": 1}, {"s": "17:00", "e": "03:00", "n": 1, "nb": 1}])

print('TEST 8 · gönüllü daha yüksek öncelik')
clear(); staff(15, 'Шахзод', 1, work='more'); db.commit()
put(10, 'c5d'); put(11, 'c5d'); put(12, 'c5n')
g = bot.cov_open_gap(db, 'dayoff', WK, DAY, 'c5d', 10)
ok, bad = bot.cov_candidates(db, g)
ids = [c['uid'] for c in ok]
check(ids and ids[0] == 15 and 14 in ids and ids.index(15) < ids.index(14), f"gönüllü 15 önde ({ids})")

print('TEST 9 · ek vardiyaya kapalı → aday değil')
db.execute("UPDATE users SET cov_extra='off' WHERE user_id=14"); db.commit()
ok, bad = bot.cov_candidates(db, g)
check(14 not in [c['uid'] for c in ok] and any(b['uid'] == 14 and 'выключены' in b['why'] for b in bad), 'kapalı kişi listede yok')
db.execute("UPDATE users SET cov_extra=NULL WHERE user_id=14"); db.commit()

print('TEST 10 · hastalık → acil açık, sıra: aynı şube gönüllü → aynı şube → diğer şube gönüllü → diğer → yönetici')
clear()
T1 = today + timedelta(days=1)
db.execute("UPDATE users SET cov_extra='any', cov_work='more' WHERE user_id=21"); db.execute("UPDATE users SET cov_extra='other' WHERE user_id=22"); db.commit()
for u_, c_ in ((10, 'c5d'), (11, 'c5d'), (12, 'c5n')):
    put(u_, c_, T1)
m = act(10, {"action": "cov_sick", "day_offset": 1})
g = gap_of(10)
check(g is not None and g['kind'] == 'sick' and cell(10, T1) == 'sick', 'acil açık + hücre «sick» (haftalık izin hakkı yemez)')
of0 = db.execute("SELECT uid, tier FROM cov_offers WHERE os_id=? ORDER BY id", (g['id'],)).fetchall()
rest, _ = bot.cov_candidates(db, g)
tiers = [(o['uid'], o['tier']) for o in of0] + [(c['uid'], c['tier']) for c in rest]
check([t for _, t in tiers] == sorted(t for _, t in tiers), f"kademe sırası korunuyor {tiers}")
check(tiers[0] == (15, 1) and tiers[1] == (14, 2), 'önce aynı şube gönüllü (15), sonra aynı şube (14)')
check((21, 3) in tiers and (22, 4) in tiers and tiers.index((21, 3)) < tiers.index((22, 4)), 'sonra diğer şube gönüllü (21) → diğer (22)')
of = db.execute("SELECT uid FROM cov_offers WHERE os_id=?", (g['id'],)).fetchall()
check(len(of) == 3, f"acil: ilk dalga 3 kişi ({[o['uid'] for o in of]})")
for _ in range(4):
    for o in db.execute("SELECT * FROM cov_offers WHERE os_id=? AND status='sent'", (g['id'],)).fetchall():
        bot.cov_decline(db, o['id'], o['uid'])
    st, rows = bot.cov_dispatch(db, g['id'])
    if st == 'escalated':
        break
check(db.execute("SELECT cov FROM open_shifts WHERE id=?", (g['id'],)).fetchone()['cov'] == 'escalated', 'herkes reddedince → yönetici')
r2, err = bot.cov_owner_assign(db, g['id'], 1, 1, 'Owner', 'urgent')
lg = json.loads(db.execute("SELECT details FROM logs WHERE action='cov_decision' ORDER BY id DESC LIMIT 1").fetchone()['details'])
check(cell(1, T1) == 'c5d' and lg.get('manager_override') is True and lg.get('reason') == 'urgent', 'yönetici üstlendi · manager_override + sebep loglandı')

print('TEST 11 · personel eklendi → otomatik uyum')
clear(); put(10, 'c5d'); put(11, 'c5d'); put(12, 'c5n')
g = bot.cov_open_gap(db, 'dayoff', WK, DAY, 'c5d', 10)
n0 = len(bot.cov_candidates(db, g)[0])
staff(16, 'Новый', 1); db.commit()
ok, _ = bot.cov_candidates(db, g)
check(len(ok) == n0 + 1 and 16 in [c['uid'] for c in ok], f"yeni kişi aday ({n0} → {len(ok)})")

print('TEST 12 · çalışan çıkarıldı → kalanlarla devam')
db.execute("UPDATE users SET archived=1 WHERE user_id IN (15,16)"); db.commit()
ok, _ = bot.cov_candidates(db, g)
check(15 not in [c['uid'] for c in ok] and 16 not in [c['uid'] for c in ok] and len(ok) >= 1, f"arşivlenen yok, diğerleri var ({[c['uid'] for c in ok]})")

print('TEST 13 · yeni şube (ihtiyaç tanımsız → şablondan türetilir)')
db.execute("INSERT INTO shift_templates (code,branch_id,start_t,end_t,active) VALUES ('nwd',3,'08:00','20:00',1)")
staff(30, 'НВ1', 3); staff(31, 'НВ2', 3); db.commit()
put(30, 'nwd'); put(31, 'nwd')
check(not bot.cov_removal_gaps(db, 30, D, 'nwd'), 'iki kişiden biri çıkınca: türetilmiş ihtiyaç 1 → açık yok')
put(31, 'off')
check(bool(bot.cov_removal_gaps(db, 30, D, 'nwd')), 'tek kişi kalınca çıkarsa: açık var')
ws = bot.cov_week_status(db, 3, 1)
check(ws['st'] in ('green', 'yellow', 'red') and ws['planned'], f"pano yeni şubede çalışıyor ({ws['st']})")

print('EK · pano + çalışan verisi')
dash = bot.cov_dash(db)
check(len(dash['branches']) == 3 and isinstance(dash['gaps'], list) and dash['pool'], 'cov_dash üretiliyor')
me = bot.cov_my(db, 30)
check('cover' in me, 'cov_my: gün bayrakları')
check(me['cover'].get('1-2') == 1, 'NewWay tek kişi: o gün izin için yedek gerekir bayrağı')

print('EK · zaman aşımı + Telegram düğmeleri')
clear(); db.execute("UPDATE users SET archived=0 WHERE user_id IN (15)"); db.commit()
put(10, 'c5d'); put(11, 'c5d'); put(12, 'c5n')
g = bot.cov_open_gap(db, 'dayoff', WK, DAY, 'c5d', 10)
st, rows = bot.cov_dispatch(db, g['id'])
first = [r['uid'] for r in rows]
ev = bot.cov_tick(db, datetime.now(bot.TZ).replace(tzinfo=None) + timedelta(minutes=200))
of2 = db.execute("SELECT uid FROM cov_offers WHERE os_id=? AND status='sent'", (g['id'],)).fetchall()
check(ev and set(o['uid'] for o in of2).isdisjoint(first), f"süre doldu → yeni dalga ({first} → {[o['uid'] for o in of2]})")
class Q:
    def __init__(s, data, uid): s.data = data; s.from_user = type('U', (), {'id': uid, 'first_name': f'U{uid}'})(); s.message = type('M', (), {'chat_id': uid})(); s.edited = None
    async def answer(s, *a, **k): pass
    async def edit_message_text(s, t, **k): s.edited = t
def cb(uid, data):
    sent.clear(); q = Q(data, uid)
    asyncio.run(bot.handle_callback(type('Up', (), {'callback_query': q, 'effective_user': q.from_user})(), type('C', (), {'bot': FB(), 'bot_data': {}})()))
    return q
o = db.execute("SELECT * FROM cov_offers WHERE os_id=? AND status='sent' ORDER BY id LIMIT 1", (g['id'],)).fetchone()
q = cb(o['uid'], f"cov_ok:{o['id']}")
check('ваша' in (q.edited or '') and cell(o['uid']) == 'c5d', f"Telegram «Возьму» → smena atandı ({q.edited})")
check(any(c == 10 for c, t, _ in sent), 'izin isteyene bildirim')
q = cb(1, "cov_own:999999")
check('ℹ️' in (q.edited or ''), 'olmayan açık → bilgi mesajı')

print('EK · «Взять смену» talebi motor açığında')
clear(); put(10, 'c5d'); put(11, 'c5d'); put(12, 'c5n')
act(10, {"action": "shift_grid_set", "target_uid": 10, "week": 1, "day": DAY, "code": "off"})
g = gap_of(10)
act(15, {"action": "open_shift_claim", "id": g['id']})
check(db.execute("SELECT status FROM open_shifts WHERE id=?", (g['id'],)).fetchone()['status'] == 'claimed', 'talep alındı')
act(1, {"action": "open_shift_decide", "id": g['id'], "decision": "ok"})
check(cell(15) == 'c5d' and cell(10) == 'off', 'onay: 15 → c5d, 10 → izin')
check(db.execute("SELECT status FROM dayoff_requests WHERE id=?", (g['dayoff_req_id'],)).fetchone()['status'] == 'ok', 'izin talebi de onaylandı')
print('\nSONUÇ:', 'HEPSİ GEÇTİ' if not FAIL else f'{len(FAIL)} HATA: {FAIL}')
