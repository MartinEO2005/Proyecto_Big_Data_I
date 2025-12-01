| column_name                     | type   | description                                                       | values / notes                          |
| ------------------------------- | ------ | ----------------------------------------------------------------- | --------------------------------------- |
| SOG_ID                          | string | Identificador administrativo del municipio                        | clave para joins, no nulo               |
| LAU_ID                          | string | Código oficial LAU (Local Administrative Unit)                   | puede coincidir con SOG_ID              |
| LAU_NAME                        | string | Nombre del municipio                                              | normalizar acentos/espacios             |
| AREA_KM2                        | float  | Área del municipio en km²                                       | derivado de geometría                  |
| POP_2023                        | int    | Población municipal (año 2023)                                  | usar para normalizaciones               |
| stations_count                  | int    | Estaciones ferroviarias dentro del polígono municipal            | 0 = sin estaciones internas             |
| stations_unique                 | int    | Estaciones únicas tras deduplicación                            | evita dobles conteos                    |
| stations_density_km2            | float  | Densidad de estaciones por km²                                   | 0 si no hay estaciones                  |
| stations_with_operator_share    | float  | Proporción de estaciones con operador definido                   | 0..1, proxy de jerarquía               |
| operator_count                  | int    | Número de estaciones con operador                                | complemento absoluto                    |
| stations_per_10k_pop            | float  | Estaciones por cada 10 000 habitantes                             | requiere POP_2023 > 0                   |
| stations_within_1km_count       | int    | Estaciones dentro de 1 km del centroid municipal                  | accesibilidad inmediata                 |
| stations_within_5km_count       | int    | Estaciones dentro de 5 km del centroid municipal                  | accesibilidad extendida                 |
| stations_in_muni_plus_1km_count | int    | Estaciones dentro del polígono ampliado por 1 km                 | reduce falsos ceros                     |
| stations_in_muni_plus_5km_count | int    | Estaciones dentro del polígono ampliado por 5 km                 | área de influencia amplia              |
| min_distance_km_to_station      | float  | Distancia mínima desde el centroid a estación más cercana (km) | 0 si hay estación sobre centroid       |
| mean_distance_km_to_station     | float  | Distancia media desde centroid a estaciones dentro de 20 km       | None si no hay estaciones cerca         |
| accessible_count                | int    | Estaciones con wheelchair=yes                                     | depende de cobertura OSM                |
| accessible_share                | float  | accessible_count / stations_count                                 | 0 si stations_count = 0                 |
| category_connectivity           | string | Clasificación de conectividad del municipio                      | directo, funcional, aislado, periferico |
