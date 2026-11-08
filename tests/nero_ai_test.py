# Nero AI (owner denemesi, 2026-11-09): yalnız owner, salt-okunur araçlar, geçmiş, kullanım sayacı.
# Çalıştırma: rm -f /tmp/na.db; PYTHONPATH=. DB_PATH=/tmp/na.db BOT_TOKEN=123:abc python tests/nero_ai_test.py
import os, json, asyncio, types
os.environ["ANTHROPIC_API_KEY"] = "test-dummy"
import bot
from datetime import datetime, timedelta
FAIL = []
def check(c, m):
    print(('  ok  ' if c else '  FAIL ') + m)
    if not c: FAIL.append(m)
NS = types.SimpleNamespace
class FakeMessages:
    def __init__(self, script): self.script, self.calls = script, []
    async def create(self, **kw):
        self.calls.append(dict(kw, messages=list(kw["messages"])))
        step = self.script.pop(0)
        return step(kw)
def tool_call(name, inp):
    return lambda kw: NS(stop_reason="tool_use", usage=NS(input_tokens=1000, output_tokens=50, cache_read_input_tokens=0, cache_creation_input_tokens=0),
                         content=[NS(type="tool_use", id="t1", name=name, input=inp)])
def final(text):
    return lambda kw: NS(stop_reason="end_turn", usage=NS(input_tokens=1200, output_tokens=80, cache_read_input_tokens=0, cache_creation_input_tokens=0),
                         content=[NS(type="text", text=text)])
bot.get_db()
db = bot.get_db()
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,approved,authorized,branch_id,password) VALUES (1,'Owner','owner',1,1,1,'secret')")
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (10,'Damir','barista',1,1,1)")
db.execute("INSERT OR REPLACE INTO branches (id,name,active,max_staff) VALUES (1,'C5',1,4)")
db.execute("INSERT OR REPLACE INTO shift_templates (code, branch_id, start_t, end_t, active) VALUES ('c5d',1,'07:00','17:30',1)")
now = datetime.now(bot.TZ).replace(tzinfo=None)
db.execute("INSERT INTO shifts (user_id, branch_id, start_time, period) VALUES (10,1,?,?)", ((now - timedelta(hours=2)).isoformat(), now.strftime('%Y-%m')))
wk = (now.date() - timedelta(days=now.weekday())).isoformat()
db.execute("INSERT OR REPLACE INTO shift_grid (week_key, day, user_id, code) VALUES (?,?,10,'c5d')", (wk, now.weekday()))
db.commit()

# araçlar
t = bot.nero_ai_run_tool(db, "now_on_shift", {})
check('Damir' in t and 'C5' in t and '07:00' in t, 'now_on_shift: açık vardiya + bugünkü plan')
t = bot.nero_ai_run_tool(db, "schedule", {"date_from": now.date().isoformat(), "date_to": now.date().isoformat(), "branch": "C5"})
check('Damir' in t and '07:00' in t, 'schedule: şube filtresiyle')
check('не найден' in bot.nero_ai_run_tool(db, "schedule", {"date_from": "2026-01-01", "date_to": "2026-01-02", "branch": "Yok"}), 'schedule: bilinmeyen şube')
t = bot.nero_ai_sql("SELECT COALESCE(display_name,name) AS n FROM users WHERE role='barista'")
check('Damir' in t, 'sql: okuma çalışır')
check('secret' not in bot.nero_ai_sql("SELECT password FROM users"), 'sql: şifre okunmaz')
_t = bot.nero_ai_sql("SELECT * FROM users")
check('secret' not in _t and 'Damir' in _t, 'sql: SELECT * çalışır, şifre boş')
check('Ошибка' in bot.nero_ai_sql("SELECT * FROM devices"), 'sql: cihaz tablosu yasak')
check('Ошибка' in bot.nero_ai_sql("DELETE FROM users"), 'sql: DELETE yok')
check('Ошибка' in bot.nero_ai_sql("SELECT 1; DELETE FROM users"), 'sql: çoklu sorgu yok')
check('Ошибка' in bot.nero_ai_sql("WITH x AS (SELECT 1) UPDATE users SET name='x'"), 'sql: WITH ile yazma yok')
check(db.execute("SELECT COUNT(*) FROM users").fetchone()[0] == 2, 'veri değişmedi')

# akış: araç çağrısı → sonuç → cevap
fm = FakeMessages([tool_call("now_on_shift", {}), final("Сейчас на смене Damir (C5).")])
cl = NS(messages=fm)
a = asyncio.run(bot.nero_ai_ask(db, 1, "Kim vardiyada?", client=cl))
check(a == "Сейчас на смене Damir (C5).", f'cevap: {a}')
k0 = fm.calls[0]
check(k0["model"] == "claude-haiku-5-5" and k0["output_config"] == {"effort": "low"} and "temperature" not in k0, 'model ve ayarlar')
tr = fm.calls[1]["messages"][-1]["content"][0]
check(tr["type"] == "tool_result" and 'Damir' in tr["content"], 'araç sonucu modele döndü')
check(fm.calls[1]["messages"][-2]["role"] == "assistant", 'asistan bloğu değiştirilmeden geri gönderildi')
# geçmiş: ikinci soru önceki turu içerir (yalnız metin)
fm2 = FakeMessages([final("Да.")])
asyncio.run(bot.nero_ai_ask(db, 1, "A Magic?", client=NS(messages=fm2)))
m = fm2.calls[0]["messages"]
check([x["role"] for x in m] == ["user", "assistant", "user"] and m[1]["content"] == "Сейчас на смене Damir (C5).", 'geçmiş korunur')
check(len(bot.nero_ai_history(1)) == 4, 'geçmiş ekrana döner')
bot.nero_ai_reset(1)
check(bot.nero_ai_history(1) == [], 'yeni sohbet')
u = bot.nero_ai_usage(db)
check(u["n"] == 2 and u["usd"] > 0, f'kullanım sayacı {u}')
# yetki: barista kullanamaz
check('недоступен' in asyncio.run(bot.nero_ai_ask(db, 10, "maaşlar?", client=NS(messages=FakeMessages([])))), 'barista kullanamaz')
# API hatası → kibar mesaj
class Boom:
    async def create(self, **kw): raise RuntimeError("down")
check('недоступен' in asyncio.run(bot.nero_ai_ask(db, 1, "x", client=NS(messages=Boom()))), 'API hatasında kibar mesaj')
# günlük sınır
k, _ = bot._nero_ai_day_count(db, 1)
db.execute("INSERT OR REPLACE INTO meta (k,val) VALUES (?,?)", (k, str(bot.NERO_AI_DAY_LIMIT))); db.commit()
check('лимит' in asyncio.run(bot.nero_ai_ask(db, 1, "x", client=NS(messages=FakeMessages([])))), 'günlük sınır')
# anahtar yoksa kapalı
os.environ.pop("ANTHROPIC_API_KEY")
check(not bot.nero_ai_allowed(db, 1), 'anahtar yoksa kapalı')
class FB:
    async def send_message(self, *a, **k): return None
    def __getattr__(self, n):
        async def f(*a, **k): return None
        return f
def act(uid, p):
    ctx = bot._ShimContext(FB(), {}, uid=uid)
    asyncio.run(bot.handle_webapp_data(bot._ShimUpdate(FB(), uid, f'U{uid}', None, json.dumps(p)), ctx))
    return getattr(ctx, 'result', None)
r = act(1, {'action': 'nero_hist'})
check(r and r['ok'] and r.get('nokey') == 1, 'anahtarsız: owner ekranı açar (önizleme)')
r = act(1, {'action': 'nero_ask', 'q': 'x'})
check(r and r['ok'] is False and 'ANTHROPIC_API_KEY' in r['error'], 'anahtarsız: soru sorunca açıklama')
r = act(10, {'action': 'nero_hist'})
check(r and r['ok'] is False, 'barista ekranı açamaz')
print('ALL OK' if not FAIL else 'FAILED: %s' % FAIL)
raise SystemExit(1 if FAIL else 0)
