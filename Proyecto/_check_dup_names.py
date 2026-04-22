import json

with open('/mnt/c/Users/ferna/Documents/GitHub/Proyecto_Big_Data_I/Proyecto_Big_Data_I/municipios_es.geojson') as f:
    data = json.load(f)

names = {}
for feat in data['features']:
    p = feat['properties']
    name = p.get('NAMEUNIT', '')
    if name not in names:
        names[name] = []
    names[name].append(p.get('NATCODE', ''))

dups = {k: v for k, v in names.items() if len(v) > 1}
print('Municipios con nombre duplicado:', len(dups))
for k, v in list(dups.items())[:15]:
    print(f"  '{k}': {v}")
