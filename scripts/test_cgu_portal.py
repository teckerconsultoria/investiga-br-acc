import httpx

sources = ['ceaf', 'ceis', 'cnep', 'transparencia', 'cpgf', 'pep_cgu', 'sanctions', 'leniency', 'renuncias', 'viagens', 'ceis', 'siop']
date = '20260405'

print('=== CGU Portal Download Test ===')
print(f'Date: {date}')
print()

ok = 0
blocked = 0

for src in sources:
    url = f'https://dadosabertos-download.cgu.gov.br/PortalDaTransparencia/saida/{src}/{date}_{src.upper()}.zip'
    try:
        r = httpx.head(url, follow_redirects=True, timeout=15)
        status = '✅ OK' if r.status_code == 200 else f'❌ {r.status_code}'
        if r.status_code == 200:
            ok += 1
        else:
            blocked += 1
        print(f'{src:20s} {status}')
    except Exception as e:
        blocked += 1
        print(f'{src:20s} ❌ ERROR - {str(e)[:50]}')

print(f'\n=== Summary: {ok} OK, {blocked} blocked ===')
