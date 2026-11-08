# Engellenen çalışan Telegram gruplarından çıkarılır / susturulur (owner 2026-11-09).
# Çalıştırma: rm -f /tmp/sg.db; PYTHONPATH=. DB_PATH=/tmp/sg.db BOT_TOKEN=123:abc python tests/susp_group_test.py
import json, asyncio, types, bot
from datetime import datetime, timedelta
FAIL = []
def check(c, m):
    print(('  ok  ' if c else '  FAIL ') + m)
    if not c: FAIL.append(m)
calls, sent = [], []
MEMBER = {-100: 'member', -200: 'left', -300: 'administrator'}
class FB:
    async def send_message(self, chat_id=None, text=None, *a, **k): sent.append((chat_id, text))
    async def get_chat_member(self, chat, uid):
        return types.SimpleNamespace(status=MEMBER.get(chat, 'left'))
    async def ban_chat_member(self, chat, uid, *a, **k): calls.append(('ban', chat, uid))
    async def unban_chat_member(self, chat, uid, only_if_banned=False): calls.append(('unban', chat, uid))
    async def restrict_chat_member(self, chat, uid, perms, *a, **k):
        calls.append(('restrict', chat, uid, bool(perms.can_send_messages)))
    async def create_chat_invite_link(self, chat, **k):
        calls.append(('link', chat, k.get('member_limit'))); return types.SimpleNamespace(invite_link=f'https://t.me/+L{-chat}')
    async def get_chat(self, chat):
        return types.SimpleNamespace(permissions=bot.ChatPermissions(can_send_messages=True))
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
db.execute("INSERT OR REPLACE INTO users (user_id,name,role,branch_id,approved,authorized) VALUES (10,'Damir','barista',1,1,1)")
for i, n, g in ((1, 'C5', -100), (2, 'Magic', -200), (3, 'NewWay', -300)):
    db.execute("INSERT OR REPLACE INTO branches (id,name,active,max_staff,group_chat_id) VALUES (?,?,1,4,?)", (i, n, str(g)))
db.commit()
today = datetime.now(bot.TZ).replace(tzinfo=None).date()

# 1) Bugünden ban: üye olduğu + olmadığı gruptan ban, admin olduğu yerde owner'a uyarı
r = act(1, {'action': 'user_suspend', 'target': 10, 'on': 1, 'grp': 'ban'})
check(r and r['ok'] and r['grp'] == 'ban' and r['gdone'] == 2 and r['gfail'] == 1, f'ban sonucu {r}')
check(('ban', -100, 10) in calls and ('ban', -200, 10) in calls and ('ban', -300, 10) not in calls, 'iki gruptan ban')
check(any(c == 1 and 'NewWay' in (t or '') and 'администратор' in t for c, t in sent), 'owner\'a admin uyarısı')
# 2) Kaldır: yasak kalkar, yalnız üye olduğu gruba tek kullanımlık link
calls.clear(); sent.clear()
r = act(1, {'action': 'user_suspend', 'target': 10, 'on': 0})
check(r and r['ok'] and r['links'] == 1, f'kaldırma sonucu {r}')
check(('unban', -100, 10) in calls and ('unban', -200, 10) in calls, 'yasak iki gruptan kalktı')
check(('link', -100, 1) in calls and not any(c[0] == 'link' and c[1] == -200 for c in calls), 'link yalnız eski grubuna, tek kullanımlık')
check(any(c == 10 and 'https://t.me/+L100' in (t or '') for c, t in sent), 'link kişiye özelden gitti')
check(bot.susp_group_state(db, 10) is None, 'durum temizlendi')
# 3) Mute: yalnız üye olduğu grupta yazma kapanır; kaldırınca grubun varsayılan izni döner
calls.clear(); sent.clear()
r = act(1, {'action': 'user_suspend', 'target': 10, 'on': 1, 'grp': 'mute'})
check(r and r['grp'] == 'mute' and r['gdone'] == 1, f'mute sonucu {r}')
check(('restrict', -100, 10, False) in calls and not any(c[0] == 'ban' for c in calls), 'yalnız C5 grubunda yazamaz')
calls.clear()
r = act(1, {'action': 'user_suspend', 'target': 10, 'on': 0})
check(('restrict', -100, 10, True) in calls and r['links'] == 0, 'yazma izni geri geldi, link yok')
# 4) Tarihli blok: bugün dokunulmaz, o gün gelince tick uygular
calls.clear()
fr = today + timedelta(days=2)
r = act(1, {'action': 'user_suspend', 'target': 10, 'on': 1, 'from': fr.isoformat(), 'grp': 'ban'})
check(r['gdone'] == 0 and not calls, 'tarihli blok: bugün gruba dokunulmadı')
asyncio.run(bot.susp_group_tick(FB(), db))
check(not calls, 'tick: gün gelmeden bir şey yapmaz')
db.execute("UPDATE users SET suspended_from=? WHERE user_id=10", (today.isoformat(),)); db.commit()
asyncio.run(bot.susp_group_tick(FB(), db))
check(('ban', -100, 10) in calls and bot.susp_group_state(db, 10).get('applied') == 1, 'tick: gün gelince çıkarıldı')
calls.clear()
asyncio.run(bot.susp_group_tick(FB(), db))
check(not calls, 'tick: ikinci kez uygulamaz')
act(1, {'action': 'user_suspend', 'target': 10, 'on': 0})
# 5) «Не трогать»
calls.clear()
r = act(1, {'action': 'user_suspend', 'target': 10, 'on': 1, 'grp': 'none'})
check(r['ok'] and r['grp'] == '' and not calls and bot.susp_group_state(db, 10) is None, 'grubu değiştirmedi')
act(1, {'action': 'user_suspend', 'target': 10, 'on': 0})
check(not calls, 'kaldırınca da gruba dokunmaz')
print('ALL OK' if not FAIL else 'FAILED: %s' % FAIL)
raise SystemExit(1 if FAIL else 0)
