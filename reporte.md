# Reporte del Pipeline y Seleccion de 15 Columnas Gold

Fecha: 2026-04-24

## 1. Resumen ejecutivo

El pipeline GeoLumica se ejecuto correctamente en el entorno real de trabajo (WSL2 Ubuntu + Spark + HDFS) y el dataset Gold final quedo en:

- Ruta HDFS: `hdfs://localhost:9000/geolumica/gold/df_maestro.parquet`
- Export local: `data/gold/df_maestro.csv`
- Resultado final: `23.958 filas x 15 columnas`

La seleccion de columnas se hizo con un criterio de calidad: mantener solo variables con cobertura robusta en el Gold final y alineadas con el foco del proyecto (transporte, densidad poblacional, dinamica territorial y luminosidad nocturna).

Adicionalmente, se aplico una politica de redondeo para mejorar legibilidad en parquet/CSV sin perder interpretabilidad analitica.

## 2. Pipeline actual (Raw -> Silver -> Gold)

1. Raw
- Ingesta de fuentes abiertas (INE, OSM, VIIRS, energia, renta, migracion, conectividad).
- Datos iniciales en `data/raw/`.

2. Silver
- Normalizacion y estandarizacion en tablas dimensionales y de hechos en Parquet.
- Persistencia en HDFS bajo `/geolumica/silver/dim` y `/geolumica/silver/fact`.

3. Gold
- Construccion de una base `municipio x anio` por interseccion de claves en facts criticas.
- Joins controlados para evitar relleno artificial y duplicaciones.
- Calculo de variables derivadas (densidades, crecimiento, tasas).
- Seleccion final de columnas para consumo analitico y ML.

## 3. Criterio para elegir 15 columnas

Se aplicaron estos criterios:

1. Cobertura de datos: priorizar columnas con valores presentes de forma consistente.
2. Relevancia analitica: mantener variables explicativas para movilidad, dinamica demografica y actividad economica proxy.
3. Estabilidad de pipeline: evitar variables con alta presencia de `desconocido` o dependencias inestables en esta version.
4. Coherencia con README: foco en transporte, densidad poblacional y luminosidad nocturna.

## 4. Columnas finales Gold (15) y motivo especifico

1. `muni_id_join`
- Clave unica del municipio para trazabilidad y para poder hacer joins con otras tablas (GIS, predicciones, catalogos externos) sin ambiguedad de nombres.

2. `year`
- Dimension temporal minima del problema. Permite analisis evolutivo, calculo de tendencias y entrenamiento de modelos con componente temporal.

3. `provincia`
- Nivel administrativo intermedio para agregaciones robustas y validaciones de consistencia territorial (control de outliers por provincia).

4. `comunidad_autonoma`
- Contexto regional macro. Captura diferencias estructurales entre CCAA (politicas, geografia, estructura economica) que afectan movilidad y luminosidad.

5. `area_km2`
- Variable de escala fisica del municipio. Necesaria para interpretar magnitudes absolutas y construir indicadores de densidad.

6. `pob_absoluta_actual`
- Variable base de tamano poblacional. Es esencial para explicar demanda potencial de transporte y para normalizar otras metricas.

7. `densidad_poblacion_km2`
- Indicador estructural de concentracion humana. Mejora comparabilidad entre municipios grandes y pequenos frente al uso de poblacion absoluta.

8. `crecimiento_pob_yoy_pct`
- Señal dinamica demografica de corto plazo. Ayuda a detectar municipios en expansion o contraccion, relevante para riesgo territorial.

9. `saldo_migratorio_neto`
- Flujo neto de personas (entrada/salida). Complementa crecimiento poblacional al separar dinamica migratoria de la inercia natural.

10. `tasa_migratoria_1000hab`
- Version normalizada del saldo migratorio por tamano poblacional. Evita sesgo por escala y mejora comparaciones intermunicipales.

11. `num_vehiculos`
- Proxy directo de intensidad de movilidad terrestre y parque movil local, vinculado a conectividad y actividad economica.

12. `Indice_Conectividad`
- Indicador sintetico de accesibilidad territorial. Resume calidad relativa de conexion y aporta señal explicativa para desarrollo local.

13. `empresas_transporte_actual`
- Mide tejido empresarial logistico/transporte. Refleja capacidad productiva del sector y su relacion con movilidad y actividad.

14. `densidad_empresas_1000hab`
- Normaliza empresas de transporte por poblacion. Permite distinguir especializacion local real frente a volumen absoluto.

15. `luz_absoluta_actual`
- Indicador satelital de actividad nocturna (VIIRS). Funciona como proxy transversal de actividad economica y ocupacion del territorio.

## 5. Por que no se dejaron mas columnas en esta version

Aunque el Gold intermedio calcula mas variables, no todas cumplen el mismo nivel de calidad en el snapshot actual. En particular, algunas variables de renta y consumo presentan cobertura incompleta o valores `desconocido` en una parte relevante de filas. Por eso, en esta iteracion se priorizo un Gold mas compacto y confiable.

## 6. Estado de documentacion README sobre "28 columnas"

Se revisaron los README del repositorio y no se encontro ninguna referencia a `28 columnas`, por lo que no fue necesario eliminar nada sobre ese punto.

## 7. Politica de redondeo aplicada en Gold

Con el objetivo de evitar decimales excesivos en la salida final, se aplico redondeo por columna en `select_final_columns`.

Reglas vigentes:

1. `area_km2`: 2 decimales
2. `densidad_poblacion_km2`: 2 decimales
3. `crecimiento_pob_yoy_pct`: 2 decimales
4. `tasa_migratoria_1000hab`: 2 decimales
5. `densidad_empresas_1000hab`: 3 decimales
6. `luz_absoluta_actual`: 3 decimales

Observacion:
- `empresas_transporte_actual` se mantiene como entero (sin redondeo) por ser un conteo.

Estado verificado:
- El CSV final (`data/gold/df_maestro.csv`) ya refleja esta politica.
