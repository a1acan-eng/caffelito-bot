# Сейф долгов (2026-10-13): kapanışta kayıt HEMEN saklanır; yetki sunucuda; çift gönderim yok;
# rapor kapanınca bu kayıtlar grup raporuna girer ve tekrar yazılmaz.
# Çalıştırma: rm -f /tmp/gs.db; PYTHONPATH=. DB_PATH=/tmp/gs.db BOT_TOKEN=123:abc python tests/gdebt_safe_test.py
import json, asyncio, urllib.parse as up, bot
from datetime import datetime, timedelta
FAIL = []
def check(c, m):
    print(('  ok  ' if c else '  FAIL ') + m)
    if not c: FAIL.append(m)
SENT = []
class FB:
    async def send_message(self, chat_id=None, text=None, *a, **k): SENT.append((chat_id, text)); return None
    def __getattr__(self, n):
        async def f(*a, **k): return None
        return f
def act(uid, p):
    ctx = bot._ShimContext(FB(), {}, uid=uid)
    asyncio.run(bot.handle_webapp_data(bot._ShimUpdate(FB(), uid, f'U{uid}', None, json.dumps(p)), ctx))
    return ctx.result
db = bot.get_db()
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized) VALUES (1,'Owner','owner',1,1)")
for u, n, b in ((10, 'Бек', 1), (11, 'Али', 2)):
    db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (?,?,'barista',?,1,1)", (u, n, b))
for bid, nm in ((1, 'C5'), (2, 'Magic')):
    db.execute("INSERT OR REPLACE INTO branches (id, name, active) VALUES (?,?,1)", (bid, nm))
g2 = db.execute("INSERT INTO guests (name, branch_id, created_at) VALUES ('Гость Magic',2,'x')").lastrowid
db.execute("INSERT INTO meta (k,val) VALUES ('gdebt_pin', ?)", (bot._gdebt_pin_hash('4321'),))   # PIN var — сейф yine yazar
db.commit()
r = act(10, {'action': 'gdebt_safe_add', 'rid': 'r1', 'k': 'debt', 'n': 'Ислом', 'a': 50000})
check(r and r['ok'] is False and 'смены' in r['error'], 'açık vardiya yokken yazamaz (sunucu reddi)')
st = datetime.now(bot.TZ).replace(tzinfo=None) - timedelta(hours=3)
sid = db.execute("INSERT INTO shifts (user_id,hours,total,date,period,created_at,start_time,branch_id) VALUES (10,0,0,?,?,?,?,1)",
                 (st.strftime('%Y-%m-%d'), st.strftime('%Y-%m'), st.isoformat(), st.isoformat())).lastrowid
db.commit()
for bad, why in (({'k': 'debt', 'n': '', 'a': 5000}, 'ad yok'), ({'k': 'debt', 'n': 'X', 'a': 0}, 'tutar 0'),
                 ({'k': 'debt', 'n': 'X', 'a': -5}, 'eksi tutar'), ({'k': 'debt', 'n': 'X', 'a': 10**10}, 'çok büyük'),
                 ({'k': 'zzz', 'n': 'X', 'a': 5000}, 'yanlış tür')):
    r = act(10, dict({'action': 'gdebt_safe_add', 'rid': 'bad'}, **bad))
    check(r and r['ok'] is False, f'geçersiz reddedildi: {why}')
r = act(10, {'action': 'gdebt_safe_add', 'rid': 'r1', 'k': 'debt', 'n': 'Ислом', 'a': 50000, 'note': 'кофе'})
check(r and r['ok'] and len(r['rows']) == 1 and r['rows'][0]['a'] == 50000, 'kayıt hemen saklandı (PIN gerekmeden, yalnız yazma)')
check(set(r) <= {'ok', 'rows', 'dup'} and all(set(x) == {'id', 'n', 'k', 'a', 'note', 'at'} for x in r['rows']), 'cevapta bakiye/kasa/başkalarının kaydı YOK')
r = act(10, {'action': 'gdebt_safe_add', 'rid': 'r1', 'k': 'debt', 'n': 'Ислом', 'a': 50000})
check(r['ok'] and r.get('dup') and db.execute("SELECT COUNT(*) c FROM guest_tx").fetchone()['c'] == 1, 'aynı rid ikinci kez → çift kayıt YOK')
t = db.execute("SELECT * FROM guest_tx").fetchone()
check(t['shift_id'] == sid and t['by_id'] == 10 and t['branch_id'] == 1 and t['at'], 'kayıt vardiya + çalışan + şube + zaman ile bağlı')
r = act(10, {'action': 'gdebt_safe_add', 'rid': 'r2', 'k': 'pay', 'g': g2, 'n': 'Гость Magic', 'a': 7000})
t2 = db.execute("SELECT guest_id, branch_id FROM guest_tx WHERE rid='r2'").fetchone()
check(t2['guest_id'] != g2 and t2['branch_id'] == 1, 'başka şubenin misafir id\'si kullanılamaz (kendi şubesinde yeni kayıt)')
r = act(11, {'action': 'gdebt_safe_void', 'id': t['id']})
check(r['ok'] is False and db.execute("SELECT voided FROM guest_tx WHERE id=?", (t['id'],)).fetchone()['voided'] == 0, 'başkası silemez')
r = act(10, {'action': 'gdebt_safe_void', 'id': db.execute("SELECT id FROM guest_tx WHERE rid='r2'").fetchone()['id']})
check(r['ok'] and len(r['rows']) == 1, 'kendi kaydını aynı vardiyada geri alabilir')
s = bot.build_hash_payload(db, 10, 'x')
d = dict(p.split('=', 1) for p in s.lstrip('#').split('&') if '=' in p)
check([x['n'] for x in json.loads(up.unquote(d['gd_shift']))] == ['Ислом'], 'payload: yalnız kendi vardiya kayıtları')
check(json.loads(up.unquote(d['gdebt']))['locked'] == 1, 'liste PIN\'le kapalı kalıyor (сейф listeyi açmaz)')
# Kapanış: rapor normal akışla; сейф kayıtları gruba girer, tekrar yazılmaz
db.execute("UPDATE shifts SET end_time=? WHERE id=?", (datetime.now(bot.TZ).replace(tzinfo=None).isoformat(), sid)); db.commit()
SENT.clear()
act(10, {'action': 'cash_report', 'branch_id': 1, 'cups': [], 'expenses': [{'n': 'В долг · Ислом', 'a': 50000}], 'gdebts': [], 'note': ''})
check(db.execute("SELECT COUNT(*) c FROM guest_tx WHERE COALESCE(voided,0)=0").fetchone()['c'] == 1, 'kapanış kaydı TEKRAR yazmadı')
check(db.execute("SELECT report_id FROM guest_tx WHERE rid='r1'").fetchone()['report_id'], 'kayıt kapanış raporuna bağlandı')
print('FAIL' if FAIL else 'ALL OK', FAIL)
