import os
import pandas as pd

def analizar_local(path_base):
    print(f"🔍 Analizando estructura local en: {os.path.abspath(path_base)}\n")
    
    if not os.path.exists(path_base):
        print(f"❌ La carpeta '{path_base}' no existe en esta ruta.")
        return

    for root, dirs, files in os.walk(path_base):
        # Calculamos la profundidad para que se vea como un árbol
        level = root.replace(path_base, '').count(os.sep)
        indent = ' ' * 4 * level
        print(f"{indent}📁 {os.path.basename(root)}/")
        
        sub_indent = ' ' * 4 * (level + 1)
        for f in files:
            if f.endswith('.csv'):
                file_path = os.path.join(root, f)
                print(f"{sub_indent}📄 {f}")
                try:
                    # Leemos solo las columnas para no cargar todo el archivo
                    df_temp = pd.read_csv(file_path, nrows=0)
                    print(f"{sub_indent}   📊 Columnas: {list(df_temp.columns)}")
                except Exception as e:
                    print(f"{sub_indent}   ⚠️ Error leyendo columnas: {e}")

# Ejecutamos sobre tu carpeta de datos
analizar_local("data")