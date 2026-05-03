import pandas as pd
import os
import glob

base = "/mnt/c/Users/ferna/Documents/GitHub/Proyecto_Big_Data_I/Proyecto_Big_Data_I"

print("=== RAW CSVs ===")
for root, dirs, files in os.walk(base + "/data/raw"):
    for f in sorted(files):
        if f.endswith(".csv"):
            path = os.path.join(root, f)
            try:
                df = pd.read_csv(path, sep=None, engine="python", encoding="utf-8-sig", on_bad_lines="skip")
                tema = root.replace(base + "/data/raw/", "")
                print(f"  {tema}/{f}: {len(df)} filas x {len(df.columns)} cols")
            except Exception as e:
                print(f"  {f}: ERROR {e}")

print()
print("=== CLEAN CSVs ===")
for root, dirs, files in os.walk(base + "/data/clean"):
    for f in sorted(files):
        if f.endswith(".csv"):
            path = os.path.join(root, f)
            try:
                df = pd.read_csv(path, encoding="utf-8-sig", on_bad_lines="skip")
                print(f"  {f}: {len(df)} filas x {len(df.columns)} cols")
            except Exception as e:
                print(f"  {f}: ERROR {e}")

print()
print("=== GOLD (parquet) ===")
gold_dir = base + "/data/gold"
if os.path.exists(gold_dir):
    parts = glob.glob(gold_dir + "/**/*.parquet", recursive=True)
    if parts:
        try:
            dfs = [pd.read_parquet(p) for p in parts]
            gold = pd.concat(dfs, ignore_index=True) if len(dfs) > 1 else dfs[0]
            print(f"  Gold total: {len(gold)} filas x {len(gold.columns)} cols")
            print(f"  Columnas: {list(gold.columns)}")
        except Exception as e:
            print(f"  ERROR gold: {e}")
    else:
        print("  No hay parquet en gold/")
else:
    print("  Carpeta gold/ no existe")
