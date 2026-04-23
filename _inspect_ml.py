import json, re

for nb in ['ML/ml_regresion.ipynb', 'ML/ml_clustering.ipynb']:
    with open(nb, encoding='utf-8') as f:
        cells = json.load(f)['cells']
    code = '\n'.join(''.join(c['source']) for c in cells if c['cell_type']=='code')
    # columnas referenciadas entre comillas
    cols = re.findall(r"['\"]([a-zA-Z][a-zA-Z0-9_]+)['\"]", code)
    seen = set()
    unique = [x for x in cols if x not in seen and not seen.add(x)]
    print(f"=== {nb} ===")
    print(', '.join(unique[:80]))
    print()
