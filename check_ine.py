import requests, json
# Tabla alternativa con código de municipio: padrón municipal serie histórica
r = requests.get("https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/2879", params={"nult": 1}, timeout=30)
data = r.json(strict=False)
print(json.dumps(data[0], ensure_ascii=False, indent=2))
