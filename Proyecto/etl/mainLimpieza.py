import subprocess
import sys
import os
import time

# Lista de archivos a ejecutar.
# Puedes cambiar el orden si alguno depende de otro.
scripts_a_ejecutar = [
    "limpiezaDemografiaCiudades.py",
    "limpiezaDemografiaProvincias.py",
    "limpiezaosm.py",
    "limpiezaViirs.py"
    "unionDemografias.py"
]

def ejecutar_todo():
    # Obtiene la ruta de la carpeta donde está este script (etl/)
    # Esto evita errores de "File not found" si ejecutas desde otra carpeta.
    directorio_base = os.path.dirname(os.path.abspath(__file__))
    
    print("==========================================")
    print("🚀 INICIANDO PROCESO DE LIMPIEZA GENERAL")
    print("==========================================\n")

    start_global = time.time()
    errores = []

    for script in scripts_a_ejecutar:
        ruta_script = os.path.join(directorio_base, script)
        print(f"▶ Ejecutando: {script}...")
        
        start_script = time.time()
        
        try:
            # sys.executable asegura que se use el mismo entorno de Python (mismas librerías pandas, etc.)
            result = subprocess.run(
                [sys.executable, ruta_script],
                check=True,          # Lanza error si el script falla
                capture_output=False # Deja que los prints de cada script se vean en consola
            )
            duracion = time.time() - start_script
            print(f"✔ {script} finalizado correctamente ({duracion:.2f}s).\n")
            print("-" * 40)

        except subprocess.CalledProcessError:
            print(f"❌ ERROR CRÍTICO en {script}.")
            errores.append(script)
            # Si quieres que se detenga todo al primer error, descomenta la linea de abajo:
            # sys.exit(1) 
    
    print("\n==========================================")
    duracion_total = time.time() - start_global
    
    if errores:
        print(f"⚠️ El proceso terminó con errores en: {', '.join(errores)}")
    else:
        print(f"✅ TODOS LOS SCRIPTS FINALIZARON CON ÉXITO")
    
    print(f"⏱ Tiempo total: {duracion_total:.2f}s")
    print("==========================================")

if __name__ == "__main__":
    ejecutar_todo()