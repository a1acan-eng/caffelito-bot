# Avans komisyonu parça sayısına göre (owner 2026-10-05): 1 parça yok, 2 parça en fazla %20, 3 parça en fazla %30.
# Çalıştırma: PYTHONPATH=. DB_PATH=/tmp/advc.db BOT_TOKEN=123:abc python tests/adv_com_test.py
import bot
FAIL = []
def check(c, m):
    print(('  ok  ' if c else '  FAIL ') + m)
    if not c: FAIL.append(m)
L = 1782000
q1, q2, q3 = (bot.adv_quote(L, L, True, n, 1) for n in (1, 2, 3))
check(q1['commission'] == 0 and q1['inst'] == [L], 'tek parça komisyonsuz')
check(q2['pct'] == 20.0 and q2['commission'] == int(L * 0.2 + 0.5), 'tam limit, 2 parça → %20')
check(q3['pct'] == 30.0 and q3['commission'] == int(L * 0.3 + 0.5), 'tam limit, 3 parça → %30')
h2, h3 = bot.adv_quote(L // 2, L, True, 2, 1), bot.adv_quote(L // 2, L, True, 3, 1)
check(h2['pct'] == 10.0 and h3['pct'] == 15.0, 'yarım limit: 2 parça %10, 3 parça %15')
check(sum(h2['inst']) == h2['total'] and len(h2['inst']) == 2, '2 parça toplamı = geri ödeme')
check(bot.adv_quote(L, L, False, 3, 1)['commission'] == 0, 'komisyon kapalıysa yok')
print('FAIL' if FAIL else 'ALL OK', FAIL)
