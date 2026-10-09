# Sipariş kataloğu: «Осень-Зима 26» yeni ürünleri (owner 2026-10-09).
# Çalıştırma: rm -f /tmp/cs.db; PYTHONPATH=. DB_PATH=/tmp/cs.db BOT_TOKEN=123:abc python tests/catalog_season_test.py
import json, bot
FAIL = []
def check(c, m):
    print(('  ok  ' if c else '  FAIL ') + m)
    if not c: FAIL.append(m)
db = bot.get_db()
bot.order_catalog_season_mig(db)
check(db.execute("SELECT val FROM meta WHERE k='order_catalog'").fetchone() is None, 'snapshot yoksa dokunmaz (varsayılan liste kullanılır)')
db.execute("DELETE FROM meta WHERE k='cat_mig_ow26'")
snap = [{"id": "🍯 Сиропы, соусы, пюре", "n": "Сиропы, соусы, пюре", "ic": "x", "items": [{"id": "it95", "n": "Пюре ягодное (1 кг)", "note": ""}]},
        {"id": "🫙 Бакалея и заготовки", "n": "Бакалея и заготовки", "ic": "x", "items": [{"id": "it28", "n": "Джем помело красное (упак, 1,3 кг)", "note": ""},
                                                                                         {"id": "it203", "n": "Порошок со вкусом сыра (для сырной пенки)", "note": "мой"}]}]
db.execute("INSERT OR REPLACE INTO meta (k,val) VALUES ('order_catalog', ?)", (json.dumps(snap, ensure_ascii=False),)); db.commit()
bot.order_catalog_season_mig(db)
c = json.loads(db.execute("SELECT val FROM meta WHERE k='order_catalog'").fetchone()["val"])
syr = [i["n"] for i in c[0]["items"]]
check(sum('Пюре Lucky' in n for n in syr) == 3, 'üç Пюре Lucky «Сиропы» kategorisine eklendi')
bak = c[1]["items"]
check(sum('сырной пенки' in i["n"] for i in bak) == 1 and [i for i in bak if i["id"] == "it203"][0]["note"] == "мой", 'zaten olan ürün iki kez eklenmez, notu korunur')
check([i for i in bak if i["id"] == "it28"][0]["note"] == "прошлый сезон (Лето 26)", 'yaz ürünü silinmez, «прошлый сезон» notu alır')
c2 = json.dumps(c, ensure_ascii=False); bot.order_catalog_season_mig(db)
check(db.execute("SELECT val FROM meta WHERE k='order_catalog'").fetchone()["val"] == c2, 'bir kez çalışır')
check(sum(n.endswith('(1 л)') for n in syr if 'Пюре Lucky' in n) == 3, 'Пюре Lucky adlarında 1 л')
# Önceki göç (adsız) çoktan çalışmış kayıtlı katalog: yalnız 1 л göçü çalışır.
db.execute("DELETE FROM meta WHERE k='cat_mig_lucky1l'")
old = [{"id": "s", "n": "Сиропы", "items": [{"id": "it200", "n": "Пюре Lucky клубника-йогурт", "note": "x"},
                                            {"id": "it95", "n": "Пюре ягодное (1 кг)", "note": ""}]}]
db.execute("INSERT OR REPLACE INTO meta (k,val) VALUES ('order_catalog', ?)", (json.dumps(old, ensure_ascii=False),)); db.commit()
bot.order_catalog_season_mig(db); bot.order_catalog_season_mig(db)
it = json.loads(db.execute("SELECT val FROM meta WHERE k='order_catalog'").fetchone()["val"])[0]["items"]
check([i["n"] for i in it] == ["Пюре Lucky клубника-йогурт (1 л)", "Пюре ягодное (1 кг)"] and it[0]["note"] == "x", 'eski kayıtta adlara 1 л eklendi (bir kez, not korunur)')
print('FAIL' if FAIL else 'ALL OK', FAIL)
