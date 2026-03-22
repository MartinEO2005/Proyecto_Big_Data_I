import os
import requests
import pandas as pd
import io

def fetch_viviendas_uso_ine(base_outdir="data"):
    out_dir = os.path.join(base_outdir, "energia")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "consumo_electrico.csv")

    url_csv = "https://www.ine.es/jaxi/files/tpx/es/csv_bdsc/59531.csv"

    print("-> Iniciando extracción de consumo eléctrico...")

    try:
        r = requests.get(url_csv, headers={"User-Agent": "Mozilla/5.0"}, timeout=60)
        r.raise_for_status()

        contenido = r.content.decode("utf-8-sig", errors="ignore")
        df = pd.read_csv(io.StringIO(contenido), sep=";")
        df.columns = [c.strip() for c in df.columns]

        print("Columnas detectadas:", df.columns.tolist())

        columnas_necesarias = ["Provincias", "Municipios", "Consumo eléctrico", "Total"]
        for c in columnas_necesarias:
            if c not in df.columns:
                raise KeyError(f"No existe la columna requerida: {c}")

        df_mun = df[df["Municipios"].notna()].copy()

        split_mun = df_mun["Municipios"].astype(str).str.extract(r"^(\d{5})\s+(.*)$")
        df_mun["Codigo"] = split_mun[0]
        df_mun["Nombre"] = split_mun[1]

        df_mun["Nombre_provincia"] = (
            df_mun["Provincias"]
            .astype(str)
            .str.replace(r"^\d{2}\s+", "", regex=True)
            .str.strip()
        )

        df_mun["Total"] = (
            df_mun["Total"]
            .astype(str)
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False)
        )
        df_mun["Total"] = pd.to_numeric(df_mun["Total"], errors="coerce")

        df_export = df_mun[["Codigo", "Nombre", "Nombre_provincia", "Consumo eléctrico", "Total"]].copy()
        df_export = df_export.dropna(subset=["Codigo", "Nombre"])

        df_export.to_csv(out_path, index=False, encoding="utf-8-sig", sep=";")

        ubicaciones_unicas = df_export[["Codigo", "Nombre"]].drop_duplicates()

        print("✅ Extracción completada")
        print(f"📊 Municipios únicos: {len(ubicaciones_unicas)}")
        print(f"📄 Filas totales exportadas: {len(df_export)}")
        print(f"📁 CSV guardado en: {out_path}")

        return out_path

    except Exception as e:
        print(f"❌ Error: {e}")
        return None

if __name__ == "__main__":
    fetch_viviendas_uso_ine()