-- create_neolumina.sql
DROP DATABASE IF EXISTS `Neolumina`;
CREATE DATABASE `Neolumina` DEFAULT CHARACTER SET utf8mb4 DEFAULT COLLATE utf8mb4_unicode_ci;
USE `Neolumina`;

DROP TABLE IF EXISTS `viirs_municipios_final`;
CREATE TABLE `viirs_municipios_final` (
  `FID`        BIGINT        NULL,                     -- id del feature fuente
  `NAME`       VARCHAR(255)  NULL,                     -- nombre original fuente
  `date`       DATE          NOT NULL,                 -- mes (guardado como primer día)
  `mean_pov`   DECIMAL(12,3) NULL,                     -- indicador promedio (p. ej. pobreza)
  `AREA_HA`    DECIMAL(12,3) NULL,                     -- superficie en hectáreas
  `CTR_MOE_2006_2008_D` VARCHAR(100) NULL,             -- campo de metadato / margen de error
  `UA_ID`      VARCHAR(50)   NOT NULL,                 -- id canónico unidad administrativa (LAU/UA)
  `UA_NAME`    VARCHAR(255)  NULL,                     -- nombre normalizado del municipio
  `max`        DECIMAL(12,3) NULL,                     -- valor máximo VIIRS u otra métrica
  `mean`       DECIMAL(12,3) NULL,                     -- valor medio VIIRS u otra métrica
  `min`        DECIMAL(12,3) NULL,                     -- valor mínimo VIIRS u otra métrica
  `stdDev`     DECIMAL(12,3) NULL,                     -- desviación típica
  `created_at` TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,-- marca temporal inserción
  PRIMARY KEY (`UA_ID`, `date`)                         -- unicidad por municipio + mes
) ENGINE=InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;

CREATE INDEX `idx_viirs_date` ON `viirs_municipios_final` (`date`);
CREATE INDEX `idx_viirs_ua_name` ON `viirs_municipios_final` (`UA_NAME`);
CREATE INDEX `idx_viirs_name` ON `viirs_municipios_final` (`NAME`);

-- Ejemplo de carga CSV (comentar/descomentar según uso):
-- LOAD DATA LOCAL INFILE '/ruta/viirs_municipios_final.csv'
-- INTO TABLE viirs_municipios_final
-- FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 LINES
-- (@fid,@name,@date_str,@mean_pov,@area_ha,@ctr_moe,@ua_id,@ua_name,@max,@mean,@min,@stdDev)
-- SET
--   FID = NULLIF(@fid,''),
--   NAME = NULLIF(@name,''),
--   date = STR_TO_DATE(CASE WHEN LENGTH(@date_str)=7 THEN CONCAT(LEFT(@date_str,7),'-01') ELSE @date_str END, '%Y-%m-%d'),
--   mean_pov = NULLIF(@mean_pov,''),
--   AREA_HA = NULLIF(@area_ha,''),
--   CTR_MOE_2006_2008_D = NULLIF(@ctr_moe,''),
--   UA_ID = NULLIF(@ua_id,''),
--   UA_NAME = NULLIF(@ua_name,''),
--   `max` = NULLIF(@max,''),
--   `mean` = NULLIF(@mean,''),
--   `min` = NULLIF(@min,''),
--   stdDev = NULLIF(@stdDev,'');

-- Tabla de métricas ferroviarias municipales (OSM)
DROP TABLE IF EXISTS `osm_municipios_metrics`;
CREATE TABLE `osm_municipios_metrics` (
  `PROV_NAME`                          VARCHAR(100)   NULL,    						-- nombre oficial de la provincia
  `LAU_ID`                              VARCHAR(50)    NOT NULL,                 -- id canónico unidad administrativa (LAU/UA)
  `LAU_NAME`                            VARCHAR(255)   NULL,                     -- nombre normalizado del municipio
  `AREA_KM2`                           DECIMAL(12,3)  NULL,                     -- superficie municipal en km²
  `POP_2023`                           INT            NULL,                     -- población estimada 2023
  `stations_count`                     INT            NULL,                     -- estaciones ferroviarias dentro del municipio
  `stations_unique`                   INT            NULL,                     -- estaciones únicas tras deduplicación
  `stations_density_km2`              DECIMAL(8,3)   NULL,                     -- densidad de estaciones por km²
  `stations_with_operator_share`      DECIMAL(5,3)   NULL,                     -- proporción con operador definido
  `operator_count`                     INT            NULL,                     -- estaciones con operador
  `stations_per_10k_pop`              DECIMAL(8,3)   NULL,                     -- estaciones por cada 10 000 habitantes
  `stations_within_1km_count`         INT            NULL,                     -- estaciones dentro de 1 km del centroid
  `stations_within_5km_count`         INT            NULL,                     -- estaciones dentro de 5 km del centroid
  `stations_in_muni_plus_1km_count`   INT            NULL,                     -- estaciones en polígono ampliado 1 km
  `stations_in_muni_plus_5km_count`   INT            NULL,                     -- estaciones en polígono ampliado 5 km
  `min_distance_km_to_station`        DECIMAL(8,3)   NULL,                     -- distancia mínima al centroid (km)
  `mean_distance_km_to_station`       DECIMAL(8,3)   NULL,                     -- distancia media a estaciones cercanas (km)
  `accessible_count`                  INT            NULL,                     -- estaciones con wheelchair=yes
  `accessible_share`                  DECIMAL(5,3)   NULL,                     -- proporción accesible sobre total
  `category_connectivity`             VARCHAR(20)    NULL,                     -- clasificación de conectividad
  `created_at`                        TIMESTAMP      DEFAULT CURRENT_TIMESTAMP,-- marca temporal de inserción
  PRIMARY KEY (`UA_ID`)                                             -- clave única por municipio
) ENGINE=InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;

CREATE INDEX `idx_osm_prov_name` ON `osm_municipios_metrics` (`PROV_NAME`);
CREATE INDEX `idx_osm_ua_name` ON `osm_municipios_metrics` (`UA_NAME`);
CREATE INDEX `idx_osm_category` ON `osm_municipios_metrics` (`category_connectivity`);






