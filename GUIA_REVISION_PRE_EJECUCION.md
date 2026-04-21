# 🔍 GUÍA: QUÉ REVISAR EN LOS SCRIPTS ANTES DE EJECUTAR

## ⚠️ PROBLEMAS CONOCIDOS + CÓMO REVISARLOS

Este documento señala las 3 cosas CRÍTICAS que el usuario advirtió y te ayuda a validarlas manualmente.

---

## 1️⃣ dim_municipio desde GeoJSON

### Problema potencial:
El GeoJSON puede no tener exactamente lo que espera `dimensions.py`.

### Qué revisar en `Proyecto/silver/dimensions.py`:

**Línea ~60:**
```python
muni_id = props.get('LAU_ID')  # ¿Existe? ¿Es string de 5 dígitos?
prov_id = props.get('NUTS_ID', '')[-2:]  # ¿NUTS_ID existe? ¿Tiene al menos 2 dígitos?
```

**❌ Posibles problemas:**
- `LAU_ID` = None para algunos features
- `LAU_ID` = integer en lugar de string
- `LAU_ID` = "ES28001" en lugar de "28001"
- `NUTS_ID` no existe o está mal formado

**✅ Cómo testear:**
1. Ejecuta: `python validation_pre.py`
2. Lee el output de "Muestra de LAU_IDs"
3. Si ves formatos raros, ajusta `dimensions.py` línea ~50-70

**Ajuste sugerido si LAU_ID es "ES28001":**
```python
muni_id = props.get('LAU_ID', '')[-5:]  # Toma últimos 5 caracteres
```

---

### 2️⃣ Claves de JOIN reales

### Problema potencial:
Las facts pueden tener formatos inconsistentes en muni_id, prov_id, year.

### Qué revisar en `Proyecto/silver/facts.py`:

**Para CADA función create_fact_*():**

❌ **fact_demografia (línea ~70-100):**
```python
# PROBLEMA: Si demo_muni tiene "Municipio" y demo_prov tiene "Provincia",
# pero el join es por nombre, puede fallar si hay tildes o mayúsculas inconsistentes

demo_muni = demo_muni.withColumn("LAU_NAME", normalize_udf(F.col("Municipio")))
```

**Riesgo:** `normalize_udf` puede no limpiar tildes bien.

**Revisión:** Después de ejecutar, ve a `data/silver/fact/fact_demografia.parquet` y verifica:
```python
df = pd.read_parquet("data/silver/fact/fact_demografia.parquet")
df[df['muni_id'].isna()].head()  # ¿Hay muni_id = NULL?
```

Si hay NULLs, el join con demo_muni falló → ajusta la lógica de normalización.

---

❌ **fact_energia, fact_renta, etc. (línea ~150-200):**
```python
# Normalizar IDs
energia = energia.withColumn(
    "muni_id",
    F.lpad(F.col("muni_id"), 5, "0")  # ¿Existe columna "muni_id"?
)
```

**Riesgo:** La columna puede llamarse "Municipio", "LAU_ID", "muni", etc.

**Revisión:** Lee la cabecera del CSV raw:
```bash
head -1 data/raw/energia/consumo_electrico.csv
```

Si no aparece "muni_id", cambia el nombre en `facts.py`.

---

### 3️⃣ VIIRS y Gold: Duplicados al unir

### Problema potencial:
La agregación de VIIRS → anual en `main_gold.py` puede generar duplicados.

### Qué revisar en `Proyecto/main_gold.py`:

**Línea ~150 (aggregate_viirs_annual):**
```python
df_viirs_annual = df_viirs.groupBy("muni_id", "prov_id", "year").agg(
    F.avg("radiancia_media").alias("radiancia_media_anual"),
    ...
)
```

**Riesgo:** Si fact_viirs ya tiene duplicados en (muni_id, prov_id, year), la agregación los multiplica.

**Revisión POST-EJECUCIÓN (más importante):**
```bash
python validation_post.py
```

Busca esta línea en el output:
```
🔴 VALIDACIÓN CRÍTICA: Duplicados en (muni_id, year)
```

Si dice **"❌ PROBLEMA"**, entonces:
1. El CROSS JOIN generó más filas de las esperadas
2. O los LEFT JOINs generaron duplicados
3. Falla el pipeline

---

## 📋 PROCEDIMIENTO DE REVISIÓN MANUAL ANTES DE EJECUTAR

### PASO 1: Validación PRE-EJECUCIÓN
```bash
cd Proyecto
python validation_pre.py
```

**Qué debe pasar:**
- ✅ GeoJSON: LAU_ID, LAU_NAME, NUTS_ID existen
- ✅ Raw: Todos los 8 temas con archivos
- ✅ Claves: Se espera muni_id (5 dígitos), prov_id (2 dígitos), year (int)

**Si falla:**
- Lee el output
- Ajusta paths o estructura de datos en Raw/GeoJSON
- Repite `python validation_pre.py`

---

### PASO 2: Revisar nombre de columnas en CSVs raw
```bash
# Comprueba que existan las columnas esperadas
head -1 data/raw/demografia/demografia_poblacion_municipios.csv
head -1 data/raw/energia/consumo_electrico.csv
head -1 data/raw/renta/renta_municipios.csv
# ... etc
```

**Si los nombres NO coinciden con lo que espera facts.py, ajusta facts.py**

**Columnas esperadas por facts.py:**
- `fact_demografia`: "Municipio", "Provincia", años (2000, 2001, ...)
- `fact_energia`: "muni_id", "year", "consumo_kwh_total"
- `fact_renta`: "muni_id", "year", "renta_neta_media_euros"
- ... revisar cada función en facts.py

---

### PASO 3: Ejecutar main_silver.py (genera Silver)
```bash
python main_silver.py
```

**Qué debe generar:**
- `data/silver/dim/dim_municipio.parquet` (~8k registros)
- `data/silver/dim/dim_provincia.parquet` (~52 registros)
- `data/silver/dim/dim_fecha_anual.parquet` (30 registros)
- `data/silver/fact/fact_*.parquet` (8 archivos)

**Si falla:**
- Lee el log: `logs/main_silver.log`
- Identifica cuál fact falló
- Ajusta `facts.py` según el error

---

### PASO 4: Ejecutar main_gold.py (genera Gold)
```bash
python main_gold.py
```

**Qué debe generar:**
- `data/gold/df_maestro.parquet` (~244k registros si hay datos 1995-2025)

**Si falla:**
- Lee el log: `logs/main_gold.log`
- Probable problema: JOIN, VIIRS, o derivadas

---

### PASO 5: Validación POST-EJECUCIÓN (más importante)
```bash
python validation_post.py
```

**BUSCA ESTA LÍNEA:**
```
🔴 VALIDACIÓN CRÍTICA: Duplicados en (muni_id, year)
```

**Si dice "✅ NO HAY DUPLICADOS":**
- ✅ ¡Pipeline OK!
- Puedes usar Gold para ML

**Si dice "❌ PROBLEMA":**
- ❌ Pipeline falló
- Duplicados = error en CROSS JOIN o LEFT JOINs
- Necesita fixing en `main_gold.py`

---

## 🚨 PROBLEMAS QUE CASI SEGURO VAS A ENCONTRAR

### Problema 1: LAU_ID no existe o mal formado
**Síntoma:** `dim_municipio` tiene < 1000 registros

**Fix:** Revisa en `dimensions.py` línea ~60
```python
muni_id = props.get('LAU_ID')
# Agrega debug:
if muni_id is None:
    logger.warning(f"LAU_ID None en feature {i}, intentando NUTS_ID")
    muni_id = props.get('NUTS_ID', '')[-5:]
```

---

### Problema 2: Columnas de Raw tienen nombres raros
**Síntoma:** `fact_energia` o `fact_renta` tienen 0 registros

**Fix:** Imprime columnas reales:
```python
# Agrega en facts.py en cada create_fact_*:
logger.info(f"Columnas disponibles: {df.columns}")
```

Luego ajusta el nombre exacto de la columna.

---

### Problema 3: muni_id es integer en lugar de string
**Síntoma:** JOINs fallan con mensaje "incompatible types"

**Fix:** Asegura que sea string:
```python
df = df.withColumn("muni_id", F.col("muni_id").cast(StringType()))
```

---

### Problema 4: Duplicados en (muni_id, year) en Gold
**Síntoma:** `validation_post.py` dice "hay duplicados"

**Fix:** El problema está en VIIRS. Revisión en `main_gold.py`:
```python
# Verifica que VIIRS GROUP BY esté correcto
df_viirs_annual = df_viirs.groupBy("muni_id", "prov_id", "year").agg(...)

# Si aún hay dupes, es porque LEFT JOIN con un fact multiplica filas
# Aumenta logging en main_gold.py para ver cuántas filas hay ANTES y DESPUÉS de cada JOIN
```

---

## 📝 CHECKLIST FINAL

- [ ] Ejecuté `python validation_pre.py` y pasó
- [ ] Revisé nombres de columnas en CSVs raw
- [ ] Ejecuté `python main_silver.py` sin errores
- [ ] Ejecuté `python main_gold.py` sin errores
- [ ] Ejecuté `python validation_post.py`
  - [ ] No hay duplicados en (muni_id, year)
  - [ ] Gold tiene ~244k registros
  - [ ] Cobertura de datos > 50% en columnas clave
- [ ] ✅ Listo para usar Gold en ML

---

## 🆘 SI ALGO SALE MAL

1. **Revisa logs:**
   ```
   logs/main_silver.log
   logs/main_gold.log
   logs/validation_post.log
   ```

2. **Aumenta logging en scripts:**
   - Agrega `logger.info()` antes/después de JOINs
   - Imprime esquemas: `df.printSchema()`
   - Imprime muestras: `df.limit(5).show(truncate=False)`

3. **Testea con subset:**
   - Filtra a 10 municipios solo
   - Filtra a 1 año solo
   - Verifica que Silver se genere bien

4. **Busca en logs el error exacto:**
   - SparkSQL errors usually have a clear message
   - KeyError o AttributeError en Python → columna no existe
   - "incompatible types" → mani_id es int vs string

---

## ✅ CUANDO ESTÉ TODO OK

1. Gold está en `data/gold/df_maestro.parquet`
2. Puedes leerlo desde ML: 
   ```python
   import pandas as pd
   df = pd.read_parquet("data/gold/df_maestro.parquet")
   df.shape  # (~244k, 40+)
   ```
3. Sin duplicados
4. Listo para entrenar modelos

---
