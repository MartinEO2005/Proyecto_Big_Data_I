import os
import requests
import pandas as pd
import io
import re


def fetch_viviendas_uso_ine(base_outdir="data"):
    url_csv = "https://www.ine.es/jaxi/files/tpx/es/csv_bdsc/59531.csv"
    out_dir = os.path.join(base_outdir, "energia")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "consumo_electrico.csv")

    provincias_dict = {
        "01": "Araba/Álava", "02": "Albacete", "03": "Alicante/Alacant", "04": "Almería",
        "05": "Ávila", "06": "Badajoz", "07": "Balears, Illes", "08": "Barcelona",
        "09": "Burgos", "10": "Cáceres", "11": "Cádiz", "12": "Castellón/Castelló",
        "13": "Ciudad Real", "14": "Córdoba", "15": "Coruña, A", "16": "Cuenca",
        "17": "Girona", "18": "Granada", "19": "Guadalajara", "20": "Gipuzkoa",
        "21": "Huelva", "22": "Huesca", "23": "Jaén", "24": "León", "25": "Lleida",
        "26": "Rioja, La", "27": "Lugo", "28": "Madrid", "29": "Málaga", "30": "Murcia",
        "31": "Navarra", "32": "Ourense", "33": "Asturias", "34": "Palencia",
        "35": "Palmas, Las", "36": "Pontevedra", "37": "Salamanca", "38": "Santa Cruz de Tenerife",
        "39": "Cantabria", "40": "Segovia", "41": "Sevilla", "42": "Soria", "43": "Tarragona",
        "44": "Teruel", "45": "Toledo", "46": "Valencia/València", "47": "Valladolid",
        "48": "Bizkaia", "49": "Zamora", "50": "Zaragoza", "51": "Ceuta", "52": "Melilla"
    }

    print("-> Iniciando extracción de consumo eléctrico...")
    print("-> Iniciando filtrado estricto. Objetivo: 3.237 ubicaciones (Prov + Mun)...")

    try:
        r = requests.get(url_csv, headers={"User-Agent": "Mozilla/5.0"}, timeout=60)
        r.raise_for_status()

        contenido = r.content.decode("utf-8-sig", errors="ignore")
        df = pd.read_csv(io.StringIO(contenido), sep=";")
        df.columns = [c.strip() for c in df.columns]

        print("Columnas detectadas:", df.columns.tolist())

        rows_list = []

        for _, row in df.iterrows():
            muni_val = str(row.get("Municipios", "")).strip()
            prov_val = str(row.get("Provincias", "")).strip()

            target = ""

            if muni_val and muni_val != "nan" and re.match(r"^\d{5}", muni_val):
                target = muni_val
            elif prov_val and prov_val != "nan" and re.match(r"^\d{2}\s", prov_val):
                target = prov_val

            if not target:
                continue

            match = re.search(r"^(\d+)\s+(.*)$", target)
            if match:
                cod, nom = match.group(1), match.group(2).strip()

                es_valido = False
                if len(cod) == 5:
                    es_valido = True
                elif len(cod) == 2:
                    if provincias_dict.get(cod) == nom:
                        es_valido = True

                if es_valido:
                    total_raw = str(row.get("Total", "0"))
                    total_clean = total_raw.replace(".", "").replace(",", ".")

                    rows_list.append({
                        "Codigo": cod,
                        "Nombre": nom,
                        "Consumo eléctrico": row.get("Consumo eléctrico", ""),
                        "Total": total_clean
                    })

        if not rows_list:
            raise ValueError("No se pudieron extraer datos. Revisa el formato del CSV del INE.")

        df_final = pd.DataFrame(rows_list)
        df_final = df_final.drop_duplicates(subset=["Codigo", "Consumo eléctrico"])

        df_final["Total"] = pd.to_numeric(df_final["Total"], errors="coerce")

        df_final.to_csv(out_path, index=False, encoding="utf-8-sig", sep=";")

        c_prov = len(df_final[df_final["Codigo"].str.len() == 2]["Codigo"].unique())
        c_muni = len(df_final[df_final["Codigo"].str.len() == 5]["Codigo"].unique())

        print("✅ Extracción completada")
        print(f"📊 GeoLúmica Data: {c_prov} Provincias y {c_muni} Municipios guardados.")
        print(f"📄 Filas totales exportadas: {len(df_final)}")
        print(f"📁 CSV guardado en: {out_path}")

        return out_path

    except Exception as e:
        print(f"❌ Error: {e}")
        return None


if __name__ == "__main__":
    fetch_viviendas_uso_ine()