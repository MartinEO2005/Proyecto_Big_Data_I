import pandas as pd
import json
import unicodedata
import os

DICCIONARIO_REBELDES = {
    "manjabalago y ortigosa de rioalmar": "manjabalago y ortigosa de rioa",
    "san martin de la vega del alberche": "san martin de la vega del albe",
    "partido de la sierra en tobalina": "partido de la sierra en tobalin",
    "quintanilla del agua y tordueles": "quintanilla del agua y torduele",
    "villarcayo de merindad de castilla la vieja": "villarcayo de merindad de casti",
    "san sebastian de los ballesteros": "san sebastian de los ballester",
    "cerdedo": "cerdido", "cesuras": "oza cesuras", "oza dos rios": "oza cesuras",
    "as pontes de garcia rodriguez": "a pontes de garcia rodriguez",
    "cruilles monells i sant sadurni de l heura": "cruilles monells i sant sadur",
    "gargantilla del lozoya y pinilla de buitrago": "gargantilla del lozoya y pinill",
    "bustillo del paramo de carrion": "bustillo del paramo de carrio",
    "santa maria de guia de gran canaria": "santa maria de guia de gran c",
    "cotobade": "cerdedo cotobade",
    "montejo de la vega de la serrezuela": "montejo de la vega de la serrez",
    "vandellos i l hospitalet de l infant": "vandellos i l hospitalet de l",
    "villanueva del rebollar de la sierra": "villanueva del rebollar de la s",
    "abanto y ciervana abanto zierbena": "abanto y ciervana abanto zierb",
    "san martin de la virgen de moncayo": "san martin de la virgen de mon",
    "velez blanco": "velez blanco", "benitagla": "benitagla", "carrascalejo el": "el carrascalejo",
    "villar del pozo": "villar del pozo", "cumbres de enmedio": "cumbres de enmedio",
    "pesquera": "pesquera", "tresviso": "tresviso", "puebla de san miguel": "puebla de san miguel",
    "sempere": "sempere", "el poble nou de benitatxell": "benitachell",
    "poble nou de benitatxell el": "benitachell", "el camp de mirra": "campo de mirra",
    "camp de mirra el": "campo de mirra", "el fondo de les neus": "hondon de las nieves",
    "fondo de les neus el": "hondon de las nieves", "l orxa": "lorcha", "orxa l": "lorcha",
    "el pinos": "pinoso", "pinos el": "pinoso", "la torre de les macanes": "torremanzanas",
    "torre de les macanes la": "torremanzanas", "la vila joiosa": "villajoyosa",
    "vila joiosa la": "villajoyosa", "les useres": "useras", "useres les": "useras",
    "les alqueries": "alquerias del nino perdido", "alqueries les": "alquerias del nino perdido",
    "a pontes de garcia rodriguez": "as pontes de garcia rodriguez", "molar el": "el molar",
    "campillo el": "el campillo", "zarza la": "la zarza", "torrent": "torrent",
    "cieza": "cieza", "moya": "moya"
}

def limpiar_texto(texto):
    if not isinstance(texto, str): return "desconocido"
    texto = texto.split('/')[0].strip()
    if ',' in texto:
        partes = texto.split(',')
        if len(partes) == 2:
            articulo = partes[1].strip()
            nombre = partes[0].strip()
            if len(articulo) <= 4 or articulo.lower() in ["l'", "l´", "les"]:
                texto = f"{articulo} {nombre}"
    texto = texto.lower()
    texto = "".join(c for c in unicodedata.normalize('NFKD', texto) if not unicodedata.combining(c))
    texto = texto.replace('-', ' ')
    for char in [',', '.', '(', ')', '*', '\"', '\'', '´', '  ']:
        texto = texto.replace(char, ' ')
    resultado = " ".join(texto.split())
    return DICCIONARIO_REBELDES.get(resultado, resultado)

def get_maestro_municipios(ruta_geojson):
    """Lee el GeoJSON oficial y devuelve un DataFrame maestro con claves de cruce."""
    with open(ruta_geojson, 'r', encoding='utf-8') as f:
        data = json.load(f)
        
    filas = []
    for feature in data['features']:
        props = feature['properties']
        lau_id = props.get('LAU_ID')
        if lau_id:
            lau_id = str(lau_id).zfill(5)
            prov_key = lau_id[:2] # Los dos primeros dígitos del LAU_ID son la provincia
            nombre = props.get('LAU_NAME', '')
            muni_clean = limpiar_texto(nombre)
            
            filas.append({
                'muni_key': lau_id,
                'prov_key': prov_key,
                'muni_display': nombre,
                'union_key': f"{prov_key}_{muni_clean}"
            })
            
    return pd.DataFrame(filas)